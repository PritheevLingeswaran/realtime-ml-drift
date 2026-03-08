"""
Prometheus metrics registry for the realtime-ml-drift service.

Operational principle:
- Missing time-series is worse than zero. Dashboards/alerts break.
- So we pre-initialize labeled Counters using .labels(...).inc(0).
"""

from __future__ import annotations

import os
import time

from prometheus_client import Counter, Gauge, Histogram

# ----------------------------
# Throughput & errors
# ----------------------------

EVENTS_INGESTED = Counter("events_ingested_total", "Total events ingested", ["source"])
EVENTS_SCORED = Counter("events_scored_total", "Total events scored")
ALERTS_EMITTED = Counter("alerts_emitted_total", "Total alerts emitted", ["severity"])
ERRORS_TOTAL = Counter("errors_total", "Total errors", ["component"])
DUPLICATE_EVENTS_TOTAL = Counter("duplicate_events_total", "Total duplicate events skipped", ["source"])
DROPPED_EVENTS_TOTAL = Counter("dropped_events_total", "Total events dropped due to explicit overload policy")
DROP_RATE = Gauge("drop_rate", "Dropped events divided by offered events under explicit overload policy")
QUEUE_OVERFLOW_TOTAL = Counter("queue_overflow_total", "Total queue overflow events", ["policy"])
ADMIN_ACTIONS_TOTAL = Counter("admin_actions_total", "Administrative actions", ["action", "result"])

# Pre-initialize labeled counters so they always export series (even when zero).
EVENTS_INGESTED.labels(source="stream").inc(0)
EVENTS_INGESTED.labels(source="api").inc(0)

ALERTS_EMITTED.labels(severity="low").inc(0)
ALERTS_EMITTED.labels(severity="medium").inc(0)
ALERTS_EMITTED.labels(severity="high").inc(0)
ALERTS_EMITTED.labels(severity="critical").inc(0)

ERRORS_TOTAL.labels(component="stream").inc(0)
ERRORS_TOTAL.labels(component="feature").inc(0)
ERRORS_TOTAL.labels(component="model").inc(0)
ERRORS_TOTAL.labels(component="drift").inc(0)
ERRORS_TOTAL.labels(component="api").inc(0)
DUPLICATE_EVENTS_TOTAL.labels(source="stream").inc(0)
DUPLICATE_EVENTS_TOTAL.labels(source="api").inc(0)
QUEUE_OVERFLOW_TOTAL.labels(policy="drop").inc(0)
QUEUE_OVERFLOW_TOTAL.labels(policy="backpressure").inc(0)
ADMIN_ACTIONS_TOTAL.labels(action="freeze_adaptation", result="success").inc(0)
ADMIN_ACTIONS_TOTAL.labels(action="unfreeze_adaptation", result="success").inc(0)
ADMIN_ACTIONS_TOTAL.labels(action="refresh_reference", result="success").inc(0)

# ----------------------------
# Latency
# ----------------------------

SCORE_LATENCY = Histogram(
    "score_latency_seconds",
    "End-to-end scoring latency seconds",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
FEATURE_LATENCY = Histogram(
    "feature_latency_seconds",
    "Feature computation latency seconds",
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
DRIFT_LATENCY = Histogram(
    "drift_latency_seconds",
    "Drift computation latency seconds",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# ----------------------------
# Drift/adaptation state
# ----------------------------

CURRENT_THRESHOLD = Gauge("current_threshold", "Current anomaly threshold")
DRIFT_ACTIVE = Gauge("drift_active", "1 if drift suspected, else 0")
ANOMALY_RATE = Gauge("anomaly_rate_recent", "Recent anomaly rate (windowed)")

# Drift quality counters/gauges (optional ground truth via event.drift_tag).
DRIFT_ALERT_TOTAL = Counter("drift_alert_total", "Total scored events evaluated for drift")
DRIFT_ALERT_POSITIVE_TOTAL = Counter("drift_alert_positive_total", "Total adaptive drift alerts")
DRIFT_ALERT_FIXED_POSITIVE_TOTAL = Counter(
    "drift_alert_fixed_positive_total", "Total fixed-baseline drift alerts"
)
DRIFT_TRUE_LABEL_TOTAL = Counter("drift_true_label_total", "Total events with ground-truth drift label")
DRIFT_TRUE_POSITIVE_TOTAL = Counter("drift_true_positive_total", "Adaptive drift true positives")
DRIFT_FALSE_POSITIVE_TOTAL = Counter("drift_false_positive_total", "Adaptive drift false positives")
DRIFT_PRECISION_GAUGE = Gauge("drift_precision", "Adaptive drift precision")
DRIFT_RECALL_GAUGE = Gauge("drift_recall", "Adaptive drift recall")

CPU_USAGE_PERCENT = Gauge("cpu_usage", "Process CPU usage percent (best-effort)")
MEMORY_USAGE_BYTES = Gauge("memory_usage", "Process memory usage in bytes (best-effort)")
QUEUE_DEPTH = Gauge("queue_depth", "Current ingestion queue depth")
PROCESSING_LAG_SECONDS = Gauge("processing_lag_seconds", "Current processing lag in seconds")
PROCESSING_LAG_HISTOGRAM = Histogram(
    "processing_lag_seconds_histogram",
    "Observed processing lag in seconds from event timestamp to processing time",
    buckets=(0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0),
)
MAX_PROCESSING_LAG_SECONDS = Gauge("max_processing_lag_seconds", "Max processing lag seen in current process")

# Kafka/Redpanda operational metrics (optional path).
CONSUMER_LAG = Gauge("consumer_lag", "Approximate consumer lag", ["topic", "partition"])
COMMIT_LATENCY_SECONDS = Histogram(
    "commit_latency_seconds",
    "Broker commit latency seconds",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
DLQ_COUNT = Counter("dlq_count_total", "Malformed events sent to DLQ")
DESERIALIZE_ERRORS_TOTAL = Counter(
    "deserialize_errors_total",
    "Kafka/replay deserialize errors",
    ["source"],
)

DESERIALIZE_ERRORS_TOTAL.labels(source="kafka").inc(0)
DESERIALIZE_ERRORS_TOTAL.labels(source="replay").inc(0)

_PROCESS = None
_LAST_RESOURCE_TS = 0.0

try:
    import psutil  # type: ignore

    _PROCESS = psutil.Process(os.getpid())
except Exception:  # noqa: BLE001
    _PROCESS = None


def update_resource_usage_metrics(sample_interval_s: float = 1.0) -> None:
    global _LAST_RESOURCE_TS
    now = time.time()
    if (now - _LAST_RESOURCE_TS) < float(sample_interval_s):
        return
    _LAST_RESOURCE_TS = now

    if _PROCESS is None:
        return

    try:
        CPU_USAGE_PERCENT.set(float(_PROCESS.cpu_percent(interval=None)))
        MEMORY_USAGE_BYTES.set(float(_PROCESS.memory_info().rss))
    except Exception:  # noqa: BLE001
        return
