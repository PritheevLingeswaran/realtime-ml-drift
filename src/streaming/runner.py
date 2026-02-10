from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import structlog
import numpy as np

from src.drift.adaptation import AdaptationConfig, ThresholdController
from src.drift.monitor import DriftConfig, DriftMonitor
from src.feature_engineering.featurizer import Featurizer
from src.feature_engineering.window_store import EntityWindowStore, WindowConfig
from src.models.scorer import ModelScorer
from src.monitoring.alerts import AlertStore
from src.monitoring.logger import configure_logging
from src.monitoring import metrics as m
from src.schemas.alert_schema import Alert
from src.schemas.event_schema import Event
from src.utils.config import load_config
from src.utils.ids import stable_id


@dataclass
class ServiceState:
    """Holds mutable runtime state for API and stream processor."""

    config: Dict[str, Any]
    featurizer: Featurizer
    scorer: ModelScorer
    drift: DriftMonitor
    threshold: ThresholdController
    alerts: AlertStore
    logger: structlog.BoundLogger


def build_state(config_path: Optional[str]) -> ServiceState:
    cfg = load_config(config_path).raw

    configure_logging(level=str(cfg["monitoring"]["log_level"]))
    logger = structlog.get_logger()

    # Featurizer
    fcfg = cfg["feature_engineering"]
    store = EntityWindowStore(WindowConfig(
        window_seconds=int(fcfg["window_seconds"]),
        max_events_per_entity=int(fcfg["max_events_per_entity"]),
    ))
    featurizer = Featurizer(store=store, enabled_features=list(fcfg["features"]))

    # Model scorer
    mcfg = cfg["model"]
    scorer = ModelScorer(
        model_type=str(mcfg["type"]),
        enabled_features=list(fcfg["features"]),
        warmup_events=int(mcfg["warmup_events"]),
        iforest_params=mcfg.get("isolation_forest", {}),
    )

    # Drift
    dcfg = cfg["drift"]
    drift = DriftMonitor(
        cfg=DriftConfig(
            reference_window_events=int(dcfg["reference_window_events"]),
            current_window_events=int(dcfg["current_window_events"]),
            min_samples=int(dcfg["min_samples"]),
            feature_ks_p=float(dcfg["feature_checks"]["ks_pvalue_threshold"]),
            feature_psi=float(dcfg["feature_checks"]["psi_threshold"]),
            pred_ks_p=float(dcfg["prediction_checks"]["ks_pvalue_threshold"]),
            pred_psi=float(dcfg["prediction_checks"]["psi_threshold"]),
            adwin_enabled=bool(dcfg["prediction_checks"]["adwin"]["enabled"]),
            adwin_delta=float(dcfg["prediction_checks"]["adwin"]["delta"]),
        ),
        feature_names=list(fcfg["features"]),
    )

    # Threshold controller
    acfg = cfg["adaptation"]
    threshold = ThresholdController(AdaptationConfig(
        enabled=bool(acfg["enabled"]),
        target_anomaly_rate=float(acfg["target_anomaly_rate"]),
        initial_threshold=float(acfg["initial_threshold"]),
        min_threshold=float(acfg["min_threshold"]),
        max_threshold=float(acfg["max_threshold"]),
        max_step=float(acfg["max_step"]),
        cooldown_seconds=int(acfg["cooldown_seconds"]),
    ))

    # Alerts
    sink = os.path.join("data", "processed", "snapshots", "alerts.jsonl")
    alerts = AlertStore(max_size=int(cfg["monitoring"]["alert_buffer_size"]), sink_path=sink)

    # Initialize metrics state
    m.CURRENT_THRESHOLD.set(float(threshold.threshold))

    return ServiceState(
        config=cfg,
        featurizer=featurizer,
        scorer=scorer,
        drift=drift,
        threshold=threshold,
        alerts=alerts,
        logger=logger,
    )


def _severity(score: float, threshold: float) -> str:
    # Simple severity mapping; production often uses multi-threshold tiers.
    if score >= min(0.99, threshold + 0.10):
        return "high"
    if score >= min(0.95, threshold + 0.05):
        return "medium"
    return "low"


async def process_event(state: ServiceState, e: Event, source: str = "stream") -> Dict[str, Any]:
    """Single-event pipeline. Used by both stream runner and API /score."""
    m.EVENTS_INGESTED.labels(source=source).inc()
    with m.FEATURE_LATENCY.time():
        feats = state.featurizer.ingest_and_featurize(e)

    if not state.scorer.ready:
        state.scorer.warmup_update(feats)
        return {"status": "warming_up", "event_id": e.event_id}

    with m.SCORE_LATENCY.time():
        score, raw = state.scorer.score(feats)
    m.EVENTS_SCORED.inc()

    with m.DRIFT_LATENCY.time():
        drift_state = state.drift.update(feats=feats, score=score, ts=e.ts)

    # Adapt threshold (guarded)
    new_thr = state.threshold.update(score=score, ts=e.ts, drift_active=drift_state.drift_active)
    m.CURRENT_THRESHOLD.set(float(new_thr))
    m.ANOMALY_RATE.set(float(state.threshold.anomaly_rate()))
    m.DRIFT_ACTIVE.set(1.0 if drift_state.drift_active else 0.0)

    is_anom = score >= new_thr
    if is_anom:
        sev = _severity(score, new_thr)
        alert = Alert(
            alert_id=stable_id("alert", e.event_id, e.ts, score),
            ts=e.ts,
            entity_id=e.entity_id,
            event_id=e.event_id,
            score=float(score),
            threshold=float(new_thr),
            severity=sev,  # type: ignore[arg-type]
            reason="score>=threshold",
            drift_state=drift_state.to_dict(),
            metadata={"merchant_category": e.merchant_category, "country": e.country, "channel": e.channel},
        )
        state.alerts.add(alert)
        m.ALERTS_EMITTED.labels(severity=sev).inc()

    return {
        "status": "scored",
        "event_id": e.event_id,
        "score": float(score),
        "threshold": float(new_thr),
        "is_anomaly": bool(is_anom),
        "drift_active": bool(drift_state.drift_active),
    }


async def run_stream_processor(config_path: Optional[str] = None) -> None:
    from src.streaming.sources import DriftScenario, JSONLReplaySource, SyntheticTransactionSource

    state = build_state(config_path=config_path)
    cfg = state.config
    scfg = cfg["streaming"]
    drift_cfg = scfg.get("drift", {})
    drift = DriftScenario(
        enabled=bool(drift_cfg.get("enabled", True)),
        drift_start_event=int(drift_cfg.get("drift_start_event", 0)),
        drift_type=str(drift_cfg.get("drift_type", "mean_shift_amount")),
    )

    if scfg["source"] == "synthetic":
        src = SyntheticTransactionSource(
            seed=int(cfg["app"]["seed"]),
            event_rate_per_sec=float(scfg["event_rate_per_sec"]),
            entity_cardinality=int(scfg["entity_cardinality"]),
            max_events=int(scfg["max_events"]),
            drift=drift,
        )
    elif scfg["source"] == "replay":
        src = JSONLReplaySource(path=str(scfg["replay_path"]), speedup=20.0 if cfg["app"]["env"] == "dev" else 1.0)
    else:
        raise ValueError(f"Unknown streaming source: {scfg['source']}")

    state.logger.info("stream_started", source=scfg["source"], env=cfg["app"]["env"])

    async for e in src.stream():
        try:
            await process_event(state, e, source="stream")
        except Exception as ex:  # noqa: BLE001
            m.ERRORS_TOTAL.labels(component="stream_processor").inc()
            state.logger.error("process_error", error=str(ex), event_id=e.event_id)
