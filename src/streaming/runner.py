from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import structlog

from src.drift.adaptation import AdaptationConfig, ThresholdController
from src.drift.monitor import DriftConfig, DriftMonitor
from src.feature_engineering.featurizer import Featurizer
from src.feature_engineering.window_store import EntityWindowStore, WindowConfig
from src.models.scorer import ModelScorer
from src.monitoring import metrics as m
from src.monitoring.alerts import AlertStore
from src.monitoring.logger import configure_logging
from src.schemas.alert_schema import Alert
from src.schemas.event_schema import Event
from src.utils.config import load_config
from src.utils.ids import stable_id


@dataclass
class ServiceState:
    """Holds mutable runtime state for API and stream processor."""

    config: dict[str, Any]
    featurizer: Featurizer
    scorer: ModelScorer
    drift: DriftMonitor
    threshold: ThresholdController
    alerts: AlertStore
    logger: structlog.BoundLogger


@dataclass
class MicroBatchConfig:
    enabled: bool
    batch_size: int
    flush_ms: int


def build_state(config_path: str | None) -> ServiceState:
    cfg = load_config(config_path).raw

    configure_logging(level=str(cfg["monitoring"]["log_level"]))
    logger = structlog.get_logger()

    # Featurizer
    fcfg = cfg["feature_engineering"]
    store = EntityWindowStore(
        WindowConfig(
            window_seconds=int(fcfg["window_seconds"]),
            max_events_per_entity=int(fcfg["max_events_per_entity"]),
        )
    )
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
            window_size=int(dcfg.get("window_size", dcfg["current_window_events"])),
            evaluation_interval=int(dcfg.get("evaluation_interval", 100)),
            check_interval_events=int(dcfg.get("check_interval_events", 200)),
            periodic_expensive_checks_enabled=bool(
                dcfg.get("periodic_expensive_checks_enabled", False)
            ),
            min_samples=int(dcfg["min_samples"]),
            feature_ks_p=float(dcfg["feature_checks"]["ks_pvalue_threshold"]),
            feature_psi=float(dcfg["feature_checks"]["psi_threshold"]),
            pred_ks_p=float(dcfg["prediction_checks"]["ks_pvalue_threshold"]),
            pred_psi=float(dcfg["prediction_checks"]["psi_threshold"]),
            threshold_method=str(dcfg.get("threshold_method", "adaptive")),
            threshold_k=float(dcfg.get("threshold_k", 2.0)),
            fixed_score_threshold=float(dcfg.get("fixed_score_threshold", 1.0)),
            feature_vote_fraction=float(dcfg.get("feature_vote_fraction", 0.30)),
            smoothing_consecutive=int(dcfg.get("smoothing_consecutive", 3)),
            alert_cooldown_events=int(dcfg.get("alert_cooldown_events", 300)),
            score_weight_psi=float(dcfg.get("score_weights", {}).get("psi", 0.5)),
            score_weight_ks=float(dcfg.get("score_weights", {}).get("ks", 0.3)),
            score_weight_pred=float(dcfg.get("score_weights", {}).get("prediction", 0.2)),
            baseline_min_evals=int(dcfg.get("baseline_min_evals", 20)),
            mean_shift_z_threshold=float(dcfg.get("mean_shift_z_threshold", 2.5)),
            feature_threshold_k=float(dcfg.get("feature_threshold_k", 1.5)),
            adaptive_score_quantile=float(dcfg.get("adaptive_score_quantile", 0.90)),
            feature_alert_score_threshold=float(dcfg.get("feature_alert_score_threshold", 0.85)),
            norm_cap=float(dcfg.get("norm_cap", 3.0)),
            warning_enter_mult=float(dcfg.get("warning_enter_mult", 1.0)),
            warning_exit_mult=float(dcfg.get("warning_exit_mult", 0.8)),
            critical_enter_mult=float(dcfg.get("critical_enter_mult", 1.2)),
            critical_exit_mult=float(dcfg.get("critical_exit_mult", 1.0)),
            warning_vote_fraction=float(dcfg.get("warning_vote_fraction", 0.30)),
            critical_vote_fraction=float(dcfg.get("critical_vote_fraction", 0.50)),
            warning_consecutive=int(dcfg.get("warning_consecutive", 1)),
            critical_consecutive=int(dcfg.get("critical_consecutive", 2)),
            adwin_enabled=bool(dcfg["prediction_checks"]["adwin"]["enabled"]),
            adwin_delta=float(dcfg["prediction_checks"]["adwin"]["delta"]),
        ),
        feature_names=list(fcfg["features"]),
    )

    # Threshold controller
    acfg = cfg["adaptation"]
    threshold = ThresholdController(
        AdaptationConfig(
            enabled=bool(acfg["enabled"]),
            target_anomaly_rate=float(acfg["target_anomaly_rate"]),
            initial_threshold=float(acfg["initial_threshold"]),
            min_threshold=float(acfg["min_threshold"]),
            max_threshold=float(acfg["max_threshold"]),
            max_step=float(acfg["max_step"]),
            cooldown_seconds=int(acfg["cooldown_seconds"]),
            min_history=int(acfg.get("min_history", 200)),
        )
    )

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


def _finalize_scored_event(
    state: ServiceState,
    e: Event,
    feats: dict[str, float],
    score: float,
    raw: float,
) -> dict[str, Any]:
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
            metadata={
                "merchant_category": e.merchant_category,
                "country": e.country,
                "channel": e.channel,
            },
        )
        state.alerts.add(alert)
        m.ALERTS_EMITTED.labels(severity=sev).inc()

    if drift_state.drift_evaluated:
        m.DRIFT_ALERT_TOTAL.inc()
        if drift_state.drift_active:
            m.DRIFT_ALERT_POSITIVE_TOTAL.inc()
        if drift_state.drift_fixed_active:
            m.DRIFT_ALERT_FIXED_POSITIVE_TOTAL.inc()

        gt_drift = int(bool(e.drift_tag))
        if gt_drift:
            m.DRIFT_TRUE_LABEL_TOTAL.inc()
            if drift_state.drift_active:
                m.DRIFT_TRUE_POSITIVE_TOTAL.inc()
        elif drift_state.drift_active:
            m.DRIFT_FALSE_POSITIVE_TOTAL.inc()

        tp = float(m.DRIFT_TRUE_POSITIVE_TOTAL._value.get())
        fp = float(m.DRIFT_FALSE_POSITIVE_TOTAL._value.get())
        true_labels = float(m.DRIFT_TRUE_LABEL_TOTAL._value.get())
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / true_labels if true_labels else 0.0
        m.DRIFT_PRECISION_GAUGE.set(precision)
        m.DRIFT_RECALL_GAUGE.set(recall)
    m.update_resource_usage_metrics()

    return {
        "status": "scored",
        "event_id": e.event_id,
        "score": float(score),
        "threshold": float(new_thr),
        "is_anomaly": bool(is_anom),
        "drift_active": bool(drift_state.drift_active),
        "drift_fixed_active": bool(drift_state.drift_fixed_active),
        "drift_warning_active": bool(drift_state.drift_warning_active),
        "drift_evaluated": bool(drift_state.drift_evaluated),
        "drift_score": float(drift_state.drift_score),
        "drift_threshold": float(drift_state.drift_threshold),
        "drift_vote_ratio": float(drift_state.vote_ratio),
        "drift_psi_component": float(drift_state.psi_component),
        "drift_ks_component": float(drift_state.ks_component),
        "drift_prediction_component": float(drift_state.prediction_component),
        "drift_feature_scores": {
            k: {s.kind: float(s.value) for s in stats}
            for k, stats in drift_state.feature_stats.items()
        },
        "raw_score": float(raw),
    }


async def process_event(state: ServiceState, e: Event, source: str = "stream") -> dict[str, Any]:
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
    return _finalize_scored_event(state=state, e=e, feats=feats, score=score, raw=raw)


async def process_events_batch(
    state: ServiceState,
    events: list[Event],
    source: str = "stream",
) -> list[dict[str, Any]]:
    """Batch pipeline path with vectorized model scoring.

    Why:
    - Featurization stays per-event and stateful.
    - Model scoring can be batched to reduce Python overhead.
    - Drift/adaptation/alert remain per-event to preserve semantics.
    """
    if not events:
        return []

    feats_batch: list[dict[str, float]] = []
    outputs: list[dict[str, Any] | None] = [None for _ in events]

    # Step 1: ingest + featurize per event (stateful per entity).
    for i, e in enumerate(events):
        m.EVENTS_INGESTED.labels(source=source).inc()
        with m.FEATURE_LATENCY.time():
            feats = state.featurizer.ingest_and_featurize(e)
        feats_batch.append(feats)

    # Step 2: warmup if needed, preserving event order and behavior.
    start_scoring_idx = 0
    for i, feats in enumerate(feats_batch):
        if state.scorer.ready:
            start_scoring_idx = i
            break
        state.scorer.warmup_update(feats)
        outputs[i] = {"status": "warming_up", "event_id": events[i].event_id}
        start_scoring_idx = i + 1

    if start_scoring_idx >= len(events):
        return [o if o is not None else {"status": "warming_up"} for o in outputs]

    # Step 3: vectorized model scoring for the ready slice.
    with m.SCORE_LATENCY.time():
        scored = state.scorer.score_batch(feats_batch[start_scoring_idx:])

    for rel_i, (score, raw) in enumerate(scored):
        i = start_scoring_idx + rel_i
        m.EVENTS_SCORED.inc()
        outputs[i] = _finalize_scored_event(
            state=state,
            e=events[i],
            feats=feats_batch[i],
            score=score,
            raw=raw,
        )

    return [o if o is not None else {"status": "warming_up"} for o in outputs]


async def run_stream_processor(config_path: str | None = None) -> None:
    from src.streaming.sources import DriftScenario, JSONLReplaySource, SyntheticTransactionSource
    import time

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
        src = JSONLReplaySource(
            path=str(scfg["replay_path"]), speedup=20.0 if cfg["app"]["env"] == "dev" else 1.0
        )
    else:
        raise ValueError(f"Unknown streaming source: {scfg['source']}")

    state.logger.info("stream_started", source=scfg["source"], env=cfg["app"]["env"])
    mb_cfg = scfg.get("micro_batch", {})
    micro = MicroBatchConfig(
        enabled=bool(mb_cfg.get("enabled", False)),
        batch_size=int(mb_cfg.get("batch_size", 64)),
        flush_ms=int(mb_cfg.get("flush_ms", 50)),
    )

    if not micro.enabled:
        async for e in src.stream():
            try:
                await process_event(state, e, source="stream")
            except Exception as ex:  # noqa: BLE001
                m.ERRORS_TOTAL.labels(component="stream_processor").inc()
                state.logger.error("process_error", error=str(ex), event_id=e.event_id)
        return

    batch: list[Event] = []
    flush_deadline = 0.0
    async for e in src.stream():
        now = time.perf_counter()
        if not batch:
            flush_deadline = now + (micro.flush_ms / 1000.0)
        batch.append(e)

        should_flush = len(batch) >= max(1, micro.batch_size) or now >= flush_deadline
        if not should_flush:
            continue

        try:
            await process_events_batch(state, batch, source="stream")
        except Exception as ex:  # noqa: BLE001
            m.ERRORS_TOTAL.labels(component="stream_processor").inc()
            state.logger.error("process_batch_error", error=str(ex), batch_size=len(batch))
        finally:
            batch.clear()

    if batch:
        try:
            await process_events_batch(state, batch, source="stream")
        except Exception as ex:  # noqa: BLE001
            m.ERRORS_TOTAL.labels(component="stream_processor").inc()
            state.logger.error("process_batch_error", error=str(ex), batch_size=len(batch))
