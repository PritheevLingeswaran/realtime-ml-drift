from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import numpy as np
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
from src.streaming.state_store import (
    SnapshotStore,
    build_snapshot_payload,
    restore_snapshot_payload,
)
from src.utils.config import load_config
from src.utils.ids import stable_id


@dataclass
class RuntimeState:
    # Why: idempotency is mandatory for at-least-once ingestion and replay safety.
    seen_event_ids: deque[str]
    seen_event_set: set[str]
    restored: bool = False
    restored_at_unix: float | None = None
    last_snapshot_unix: float | None = None
    lag_circuit_open: bool = False
    lag_samples_seconds: deque[float] = field(default_factory=lambda: deque(maxlen=5000))
    queue_depth_samples: deque[float] = field(default_factory=lambda: deque(maxlen=5000))
    max_processing_lag_seconds: float = 0.0
    dropped_events_total: int = 0
    offered_events_total: int = 0
    overload_alert_emitted: bool = False
    duplicate_events_total: int = 0
    processed_events_total: int = 0


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
    runtime: RuntimeState
    snapshot_store: SnapshotStore | None = None


@dataclass
class MicroBatchConfig:
    enabled: bool
    batch_size: int
    flush_ms: int


@dataclass
class QueueItem:
    event: Event
    ingest_wall_ts: float
    on_success: Callable[[], Awaitable[None]] | None = None


async def _noop_async() -> None:
    return


def _build_source(config: dict[str, Any]) -> tuple[Any, bool]:
    from src.streaming.sources import DriftScenario, JSONLReplaySource, SyntheticTransactionSource

    scfg = config["streaming"]
    drift_cfg = scfg.get("drift", {})
    drift = DriftScenario(
        enabled=bool(drift_cfg.get("enabled", True)),
        drift_start_event=int(drift_cfg.get("drift_start_event", 0)),
        drift_type=str(drift_cfg.get("drift_type", "mean_shift_amount")),
    )

    if scfg["source"] == "synthetic":
        src = SyntheticTransactionSource(
            seed=int(config["app"]["seed"]),
            event_rate_per_sec=float(scfg["event_rate_per_sec"]),
            entity_cardinality=int(scfg["entity_cardinality"]),
            max_events=int(scfg["max_events"]),
            drift=drift,
        )
        return src, False

    if scfg["source"] == "replay":
        src = JSONLReplaySource(
            path=str(scfg["replay_path"]), speedup=20.0 if config["app"]["env"] == "dev" else 1.0
        )
        return src, False

    if scfg["source"] == "kafka":
        from src.streaming.kafka_source import KafkaConfig, KafkaEventSource

        kcfg = scfg.get("kafka", {})
        src = KafkaEventSource(
            KafkaConfig(
                enabled=True,
                bootstrap_servers=str(kcfg["bootstrap_servers"]),
                topic=str(kcfg["topic"]),
                group_id=str(kcfg["group_id"]),
                dlq_topic=str(kcfg.get("dlq_topic")) if kcfg.get("dlq_topic") else None,
                poll_timeout_ms=int(kcfg.get("poll_timeout_ms", 500)),
            )
        )
        return src, True

    raise ValueError(f"Unknown streaming source: {scfg['source']}")


def _build_runtime(cfg: dict[str, Any]) -> RuntimeState:
    dedupe_size = int(cfg.get("streaming", {}).get("idempotency", {}).get("dedupe_cache_size", 200000))
    return RuntimeState(seen_event_ids=deque(maxlen=max(1, dedupe_size)), seen_event_set=set())


def _mark_seen(state: ServiceState, event_id: str) -> None:
    if len(state.runtime.seen_event_ids) == state.runtime.seen_event_ids.maxlen and state.runtime.seen_event_ids:
        old = state.runtime.seen_event_ids.popleft()
        state.runtime.seen_event_set.discard(old)
    state.runtime.seen_event_ids.append(event_id)
    state.runtime.seen_event_set.add(event_id)


def _is_duplicate(state: ServiceState, event_id: str, source: str) -> bool:
    if event_id in state.runtime.seen_event_set:
        state.runtime.duplicate_events_total += 1
        m.DUPLICATE_EVENTS_TOTAL.labels(source=source).inc()
        if state.runtime.duplicate_events_total == 1 or (state.runtime.duplicate_events_total % 1000) == 0:
            state.logger.info(
                "duplicate_event_skipped",
                audit_type="idempotency",
                event_id=event_id,
                source=source,
                duplicate_events_total=int(state.runtime.duplicate_events_total),
            )
        return True
    return False


def _emit_lag_circuit_alert(state: ServiceState, lag_s: float) -> None:
    alert = Alert(
        alert_id=stable_id("lag_circuit", int(time.time())),
        ts=float(time.time()),
        entity_id="system",
        event_id="lag-circuit-breaker",
        score=1.0,
        threshold=1.0,
        severity="critical",
        reason="processing_lag_threshold_exceeded",
        drift_state={"processing_lag_seconds": float(lag_s)},
        metadata={"component": "stream_processor"},
    )
    state.alerts.add(alert)
    m.ALERTS_EMITTED.labels(severity="critical").inc()


def _emit_drop_alert(state: ServiceState, queue_depth: int) -> None:
    if state.runtime.overload_alert_emitted:
        return
    state.runtime.overload_alert_emitted = True
    drop_rate = state.runtime.dropped_events_total / max(1, state.runtime.offered_events_total)
    alert = Alert(
        alert_id=stable_id("overload_drop", int(time.time())),
        ts=float(time.time()),
        entity_id="system",
        event_id="queue-overload-drop",
        score=1.0,
        threshold=1.0,
        severity="critical",
        reason="queue_overload_drop_policy_triggered",
        drift_state={},
        metadata={
            "component": "stream_processor",
            "queue_depth": int(queue_depth),
            "dropped_events_total": int(state.runtime.dropped_events_total),
            "offered_events_total": int(state.runtime.offered_events_total),
            "drop_rate": float(drop_rate),
            "overload_policy": "drop",
        },
    )
    state.alerts.add(alert)
    m.ALERTS_EMITTED.labels(severity="critical").inc()


def _record_queue_depth(state: ServiceState, depth: int) -> None:
    depth_f = float(max(0, depth))
    state.runtime.queue_depth_samples.append(depth_f)
    m.QUEUE_DEPTH.set(depth_f)


def _record_drop(state: ServiceState, queue_depth: int) -> None:
    state.runtime.dropped_events_total += 1
    m.DROPPED_EVENTS_TOTAL.inc()
    m.QUEUE_OVERFLOW_TOTAL.labels(policy="drop").inc()
    drop_rate = state.runtime.dropped_events_total / max(1, state.runtime.offered_events_total)
    m.DROP_RATE.set(float(drop_rate))
    _emit_drop_alert(state, queue_depth)
    if state.runtime.dropped_events_total == 1 or (state.runtime.dropped_events_total % 100) == 0:
        # Why: explicit overload drops are operationally significant and must leave an audit trail.
        state.logger.error(
            "overload_drop",
            audit_type="overload",
            reason="queue_full",
            overload_policy="drop",
            queue_depth=int(queue_depth),
            dropped_events_total=int(state.runtime.dropped_events_total),
            offered_events_total=int(state.runtime.offered_events_total),
            drop_rate=float(drop_rate),
        )


def _apply_processing_lag(state: ServiceState, event_ts: float | None, ingest_wall_ts: float | None) -> None:
    now = time.time()
    queue_lag_s = max(0.0, now - float(ingest_wall_ts)) if ingest_wall_ts is not None else None
    event_age_s = max(0.0, now - float(event_ts)) if event_ts is not None else None
    lag_s = queue_lag_s if queue_lag_s is not None else event_age_s
    if lag_s is None:
        return
    state.runtime.lag_samples_seconds.append(lag_s)
    state.runtime.max_processing_lag_seconds = max(state.runtime.max_processing_lag_seconds, lag_s)
    m.PROCESSING_LAG_SECONDS.set(lag_s)
    m.PROCESSING_LAG_HISTOGRAM.observe(event_age_s if event_age_s is not None else lag_s)
    m.MAX_PROCESSING_LAG_SECONDS.set(state.runtime.max_processing_lag_seconds)

    lag_limit = float(
        state.config.get("streaming", {}).get("ingestion", {}).get("lag_circuit_breaker_seconds", 5.0)
    )
    if lag_s > lag_limit and not state.runtime.lag_circuit_open:
        # Why: if processor lag explodes, freeze automation first to avoid unsafe autonomous threshold changes.
        state.threshold.freeze("lag_circuit_breaker")
        state.runtime.lag_circuit_open = True
        state.logger.warning("lag_circuit_opened", lag_seconds=lag_s, lag_limit_seconds=lag_limit)
        _emit_lag_circuit_alert(state, lag_s)


def _try_snapshot(state: ServiceState) -> None:
    store = state.snapshot_store
    if store is None:
        return
    snap_cfg = state.config.get("state", {}).get("snapshot", {})
    interval_s = float(snap_cfg.get("interval_seconds", 60))
    check_interval_events = int(snap_cfg.get("check_interval_events", 100))
    now = time.time()
    if (state.runtime.processed_events_total % max(1, check_interval_events)) != 0:
        return
    if state.runtime.last_snapshot_unix is not None and (now - state.runtime.last_snapshot_unix) < interval_s:
        return
    payload = build_snapshot_payload(state)
    store.save(payload)
    state.runtime.last_snapshot_unix = now


def _should_sample_threshold_log(state: ServiceState, event_id: str) -> bool:
    sample_rate = float(state.config.get("monitoring", {}).get("logging", {}).get("sample_rate", 0.01))
    if sample_rate >= 1.0:
        return True
    if sample_rate <= 0.0:
        return False
    # Why: deterministic hash-based sampling keeps replay behavior stable while avoiding per-event log I/O.
    bucket = stable_id("threshold-log-sample", event_id)
    return (int(bucket[:8], 16) / 0xFFFFFFFF) < sample_rate


def build_state(config_path: str | None, *, allow_restore: bool | None = None) -> ServiceState:
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
                dcfg.get("periodic_expensive_checks_enabled", True)
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
            history_window_size=int(acfg.get("history_window_size", 2000)),
            update_interval_events=int(acfg.get("recompute_interval_events", acfg.get("update_interval_events", 1))),
            audit_log_min_delta=float(acfg.get("audit_log_min_delta", 0.0)),
            audit_log_cooldown_seconds=float(acfg.get("audit_log_cooldown_seconds", 0.0)),
            adapt_during_drift=bool(acfg.get("adapt_during_drift", False)),
            rate_feedback_enabled=bool(acfg.get("rate_feedback_enabled", False)),
            target_tolerance_abs=float(acfg.get("target_tolerance_abs", 0.0)),
        )
    )

    # Alerts
    sink = os.path.join("data", "processed", "snapshots", "alerts.jsonl")
    alerts = AlertStore(
        max_size=int(cfg["monitoring"]["alert_buffer_size"]),
        sink_path=sink,
        dedupe_size=int(cfg.get("streaming", {}).get("idempotency", {}).get("dedupe_cache_size", 200000)),
    )

    snapshot_store: SnapshotStore | None = None
    if bool(cfg.get("state", {}).get("snapshot", {}).get("enabled", True)):
        path = str(cfg.get("state", {}).get("snapshot", {}).get("path", "data/processed/state/snapshot.json"))
        snapshot_store = SnapshotStore(path=path)

    runtime = _build_runtime(cfg)

    # Initialize metrics state
    m.CURRENT_THRESHOLD.set(float(threshold.threshold))
    m.DROP_RATE.set(0.0)
    state = ServiceState(
        config=cfg,
        featurizer=featurizer,
        scorer=scorer,
        drift=drift,
        threshold=threshold,
        alerts=alerts,
        logger=logger,
        runtime=runtime,
        snapshot_store=snapshot_store,
    )
    _record_queue_depth(state, 0)
    m.PROCESSING_LAG_SECONDS.set(0.0)
    m.MAX_PROCESSING_LAG_SECONDS.set(0.0)

    # Best-effort restore.
    should_restore = bool(cfg.get("state", {}).get("restore_on_startup", True))
    if allow_restore is not None:
        should_restore = bool(allow_restore)
    if snapshot_store is not None and should_restore:
        try:
            payload = snapshot_store.load()
            if payload:
                restore_snapshot_payload(state, payload)
                state.runtime.restored = True
                state.runtime.restored_at_unix = time.time()
                state.runtime.last_snapshot_unix = float(payload.get("saved_at_unix", time.time()))
                state.logger.info("state_restored", snapshot_path=snapshot_store.path)
        except Exception as ex:  # noqa: BLE001
            state.logger.error("state_restore_failed", error=str(ex))

    return state


def _severity(score: float, threshold: float) -> str:
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
    *,
    ingest_wall_ts: float | None = None,
) -> dict[str, Any]:
    timings_ms: dict[str, float] = {
        "feature_engineering": 0.0,
        "scoring": 0.0,
        "drift_adwin": 0.0,
        "drift_ks_psi": 0.0,
        "adaptation": 0.0,
        "snapshotting": 0.0,
        "logging_metrics": 0.0,
    }
    drift_t0 = time.perf_counter()
    with m.DRIFT_LATENCY.time():
        drift_state = state.drift.update(feats=feats, score=score, ts=e.ts)
    _ = (time.perf_counter() - drift_t0) * 1000.0
    timings_ms["drift_adwin"] = float(drift_state.adwin_time_ms)
    timings_ms["drift_ks_psi"] = float(drift_state.expensive_checks_time_ms)

    old_thr = float(state.threshold.threshold)
    adapt_t0 = time.perf_counter()
    new_thr = state.threshold.update(score=score, ts=e.ts, drift_active=drift_state.drift_active)
    timings_ms["adaptation"] = (time.perf_counter() - adapt_t0) * 1000.0
    if old_thr != float(new_thr) and state.threshold.should_audit_log_change(
        old_thr, float(new_thr), e.ts
    ) and _should_sample_threshold_log(state, e.event_id):
        log_t0 = time.perf_counter()
        # Why: threshold movements are operationally sensitive and must be auditable.
        state.logger.info(
            "threshold_changed",
            audit_type="threshold_change",
            actor="controller",
            reason="target_anomaly_rate_control",
            old_threshold=old_thr,
            new_threshold=float(new_thr),
            event_id=e.event_id,
        )
        timings_ms["logging_metrics"] += (time.perf_counter() - log_t0) * 1000.0

    metrics_t0 = time.perf_counter()
    m.CURRENT_THRESHOLD.set(float(new_thr))
    m.ANOMALY_RATE.set(float(state.threshold.anomaly_rate()))
    m.DRIFT_ACTIVE.set(1.0 if drift_state.drift_active else 0.0)

    _apply_processing_lag(state, e.ts, ingest_wall_ts)

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
    timings_ms["logging_metrics"] += (time.perf_counter() - metrics_t0) * 1000.0

    state.runtime.processed_events_total += 1
    snapshot_t0 = time.perf_counter()
    _try_snapshot(state)
    timings_ms["snapshotting"] = (time.perf_counter() - snapshot_t0) * 1000.0

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
        "processing_lag_seconds": float(
            max(0.0, time.time() - ingest_wall_ts) if ingest_wall_ts is not None else max(0.0, time.time() - e.ts)
        ),
        "queue_depth": float(m.QUEUE_DEPTH._value.get()),
        "timings_ms": timings_ms,
    }


async def process_event(
    state: ServiceState,
    e: Event,
    source: str = "stream",
    *,
    ingest_wall_ts: float | None = None,
) -> dict[str, Any]:
    """Single-event pipeline. Used by both stream runner and API /score."""
    if source in {"stream", "api"} and _is_duplicate(state, e.event_id, source=source):
        return {"status": "duplicate", "event_id": e.event_id}

    m.EVENTS_INGESTED.labels(source=source).inc()
    with m.FEATURE_LATENCY.time():
        feature_t0 = time.perf_counter()
        feats = state.featurizer.ingest_and_featurize(e)
        feature_ms = (time.perf_counter() - feature_t0) * 1000.0

    if source in {"stream", "api"}:
        _mark_seen(state, e.event_id)

    if not state.scorer.ready:
        state.scorer.warmup_update(feats)
        state.runtime.processed_events_total += 1
        _try_snapshot(state)
        return {"status": "warming_up", "event_id": e.event_id, "timings_ms": {"feature_engineering": feature_ms, "scoring": 0.0, "drift_adwin": 0.0, "drift_ks_psi": 0.0, "adaptation": 0.0, "snapshotting": 0.0, "logging_metrics": 0.0}}

    with m.SCORE_LATENCY.time():
        score_t0 = time.perf_counter()
        score, raw = state.scorer.score(feats)
        score_ms = (time.perf_counter() - score_t0) * 1000.0
    m.EVENTS_SCORED.inc()
    out = _finalize_scored_event(
        state=state,
        e=e,
        feats=feats,
        score=score,
        raw=raw,
        ingest_wall_ts=ingest_wall_ts,
    )
    out["timings_ms"]["feature_engineering"] = feature_ms
    out["timings_ms"]["scoring"] = score_ms
    return out


async def process_events_batch(
    state: ServiceState,
    events: list[Event],
    source: str = "stream",
    *,
    ingest_wall_ts: list[float] | None = None,
) -> list[dict[str, Any]]:
    """Batch pipeline path with vectorized model scoring."""
    if not events:
        return []

    feats_batch: list[dict[str, float] | None] = [None for _ in events]
    outputs: list[dict[str, Any] | None] = [None for _ in events]

    for i, e in enumerate(events):
        if source in {"stream", "api"} and _is_duplicate(state, e.event_id, source=source):
            outputs[i] = {"status": "duplicate", "event_id": e.event_id}
            continue
        m.EVENTS_INGESTED.labels(source=source).inc()
        with m.FEATURE_LATENCY.time():
            feature_t0 = time.perf_counter()
            feats_batch[i] = state.featurizer.ingest_and_featurize(e)
            feature_ms = (time.perf_counter() - feature_t0) * 1000.0
        if outputs[i] is None:
            outputs[i] = {"timings_ms": {"feature_engineering": feature_ms, "scoring": 0.0, "drift_adwin": 0.0, "drift_ks_psi": 0.0, "adaptation": 0.0, "snapshotting": 0.0, "logging_metrics": 0.0}}
        if source in {"stream", "api"}:
            _mark_seen(state, e.event_id)

    score_indices: list[int] = []
    score_feats: list[dict[str, float]] = []
    for i, feats in enumerate(feats_batch):
        if feats is None:
            continue
        if not state.scorer.ready:
            state.scorer.warmup_update(feats)
            state.runtime.processed_events_total += 1
            outputs[i] = {"status": "warming_up", "event_id": events[i].event_id, "timings_ms": outputs[i]["timings_ms"] if outputs[i] else {"feature_engineering": 0.0, "scoring": 0.0, "drift_adwin": 0.0, "drift_ks_psi": 0.0, "adaptation": 0.0, "snapshotting": 0.0, "logging_metrics": 0.0}}
            continue
        score_indices.append(i)
        score_feats.append(feats)

    if score_feats:
        with m.SCORE_LATENCY.time():
            score_t0 = time.perf_counter()
            scored = state.scorer.score_batch(score_feats)
            batch_score_ms = (time.perf_counter() - score_t0) * 1000.0
        for rel_i, (score, raw) in enumerate(scored):
            i = score_indices[rel_i]
            m.EVENTS_SCORED.inc()
            outputs[i] = _finalize_scored_event(
                state=state,
                e=events[i],
                feats=score_feats[rel_i],
                score=score,
                raw=raw,
                ingest_wall_ts=(ingest_wall_ts[i] if ingest_wall_ts and i < len(ingest_wall_ts) else None),
            )
            outputs[i]["timings_ms"]["feature_engineering"] = outputs[i].get("timings_ms", {}).get("feature_engineering", 0.0)
            outputs[i]["timings_ms"]["scoring"] = batch_score_ms / max(1, len(score_feats))

    return [o if o is not None else {"status": "warming_up"} for o in outputs]


async def process_source_with_backpressure(state: ServiceState, source: Any, *, is_kafka: bool = False) -> None:
    cfg = state.config
    scfg = cfg["streaming"]
    mb_cfg = scfg.get("micro_batch", {})
    micro = MicroBatchConfig(
        enabled=bool(mb_cfg.get("enabled", False)),
        batch_size=int(mb_cfg.get("batch_size", 128)),
        flush_ms=int(mb_cfg.get("flush_ms", 50)),
    )

    ingest_cfg = scfg.get("ingestion", {})
    queue_max = int(ingest_cfg.get("queue_max_size", 5000))
    overload_policy = str(ingest_cfg.get("overload_policy", "")).strip().lower()
    if not overload_policy:
        overload_policy = "drop" if bool(ingest_cfg.get("drop_on_overflow", False)) else "backpressure"
    if overload_policy not in {"backpressure", "drop"}:
        raise ValueError(f"Unsupported overload_policy: {overload_policy}")

    queue: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=max(1, queue_max))
    stop = asyncio.Event()

    async def producer() -> None:
        try:
            if is_kafka:
                async for e, rec in source.stream():
                    state.runtime.offered_events_total += 1
                    item = QueueItem(
                        event=e,
                        ingest_wall_ts=time.time(),
                        on_success=(lambda rec=rec: source.commit(rec)),
                    )
                    if queue.full() and overload_policy == "drop":
                        _record_drop(state, queue.qsize())
                        continue
                    if queue.full():
                        m.QUEUE_OVERFLOW_TOTAL.labels(policy="backpressure").inc()
                    _record_queue_depth(state, queue.qsize())
                    await queue.put(item)
                    _record_queue_depth(state, queue.qsize())
            else:
                async for e in source.stream():
                    state.runtime.offered_events_total += 1
                    item = QueueItem(event=e, ingest_wall_ts=time.time(), on_success=_noop_async)
                    if queue.full() and overload_policy == "drop":
                        _record_drop(state, queue.qsize())
                        continue
                    if queue.full():
                        m.QUEUE_OVERFLOW_TOTAL.labels(policy="backpressure").inc()
                    _record_queue_depth(state, queue.qsize())
                    await queue.put(item)
                    _record_queue_depth(state, queue.qsize())
        finally:
            stop.set()

    async def consumer() -> None:
        batch_events: list[Event] = []
        batch_items: list[QueueItem] = []
        flush_deadline = 0.0

        while not (stop.is_set() and queue.empty()):
            timeout = 0.05
            if micro.enabled and batch_events:
                timeout = max(0.0, flush_deadline - time.perf_counter())

            try:
                item = await asyncio.wait_for(queue.get(), timeout=timeout)
                _record_queue_depth(state, queue.qsize())
                if not batch_events:
                    flush_deadline = time.perf_counter() + (micro.flush_ms / 1000.0)
                batch_events.append(item.event)
                batch_items.append(item)
            except TimeoutError:
                pass

            should_flush = False
            if not micro.enabled:
                should_flush = len(batch_events) >= 1
            else:
                should_flush = bool(batch_events) and (
                    len(batch_events) >= max(1, micro.batch_size) or time.perf_counter() >= flush_deadline
                )

            if not should_flush:
                continue

            try:
                if len(batch_events) == 1 and not micro.enabled:
                    out = await process_event(
                        state,
                        batch_events[0],
                        source="stream",
                        ingest_wall_ts=batch_items[0].ingest_wall_ts,
                    )
                    if out.get("status") in ("scored", "warming_up", "duplicate") and batch_items[0].on_success:
                        await batch_items[0].on_success()
                else:
                    outs = await process_events_batch(
                        state,
                        batch_events,
                        source="stream",
                        ingest_wall_ts=[x.ingest_wall_ts for x in batch_items],
                    )
                    for out, item in zip(outs, batch_items, strict=False):
                        if out.get("status") in ("scored", "warming_up", "duplicate") and item.on_success:
                            await item.on_success()
            except Exception as ex:  # noqa: BLE001
                m.ERRORS_TOTAL.labels(component="stream").inc()
                state.logger.error("stream_process_error", error=str(ex))
            finally:
                batch_events.clear()
                batch_items.clear()

    state.logger.info("stream_started", source=scfg["source"], env=cfg["app"]["env"])
    producer_task = asyncio.create_task(producer())
    consumer_task = asyncio.create_task(consumer())
    await asyncio.gather(producer_task, consumer_task)


async def run_stream_processor(config_path: str | None = None) -> None:
    state = build_state(config_path=config_path)
    src, is_kafka = _build_source(state.config)
    await process_source_with_backpressure(state, src, is_kafka=is_kafka)


def state_view(state: ServiceState) -> dict[str, Any]:
    lag_samples = list(state.runtime.lag_samples_seconds)
    return {
        "model_ready": bool(state.scorer.ready),
        "threshold": float(state.threshold.threshold),
        "adaptation_frozen": bool(state.threshold.is_frozen),
        "adaptation_freeze_reason": state.threshold.stats().get("freeze_reason", ""),
        "drift_active": bool(state.drift.state.drift_active),
        "drift_warning_active": bool(state.drift.state.drift_warning_active),
        "drift_score": float(state.drift.state.drift_score),
        "anomaly_rate": float(state.threshold.anomaly_rate()),
        "anomaly_rate_recent": float(state.threshold.anomaly_rate()),
        "queue_depth": float(m.QUEUE_DEPTH._value.get()),
        "queue_depth_p95": float(
            np.percentile(np.asarray(list(state.runtime.queue_depth_samples), dtype=float), 95)
            if state.runtime.queue_depth_samples
            else 0.0
        ),
        "processing_lag_seconds": float(m.PROCESSING_LAG_SECONDS._value.get()),
        "processing_lag_p50_seconds": float(
            np.percentile(np.asarray(lag_samples, dtype=float), 50) if lag_samples else 0.0
        ),
        "processing_lag_p95_seconds": float(
            np.percentile(np.asarray(lag_samples, dtype=float), 95) if lag_samples else 0.0
        ),
        "max_processing_lag_seconds": float(m.MAX_PROCESSING_LAG_SECONDS._value.get()),
        "dropped_events_total": int(state.runtime.dropped_events_total),
        "drop_rate": float(state.runtime.dropped_events_total / max(1, state.runtime.offered_events_total)),
        "duplicate_events_total": int(state.runtime.duplicate_events_total),
        "restored": bool(state.runtime.restored),
        "restored_state": bool(state.runtime.restored),
        "restored_at_unix": state.runtime.restored_at_unix,
        "last_snapshot_unix": state.runtime.last_snapshot_unix,
        "last_snapshot_time": state.runtime.last_snapshot_unix,
    }
