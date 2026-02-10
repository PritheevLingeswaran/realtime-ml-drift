from __future__ import annotations

"""
Prometheus metrics registry for the realtime-ml-drift service.

Operational principle:
- Missing time-series is worse than zero. Dashboards/alerts break.
- So we pre-initialize labeled Counters using .labels(...).inc(0).
"""

from prometheus_client import Counter, Gauge, Histogram

# ----------------------------
# Throughput & errors
# ----------------------------

EVENTS_INGESTED = Counter("events_ingested_total", "Total events ingested", ["source"])
EVENTS_SCORED = Counter("events_scored_total", "Total events scored")
ALERTS_EMITTED = Counter("alerts_emitted_total", "Total alerts emitted", ["severity"])
ERRORS_TOTAL = Counter("errors_total", "Total errors", ["component"])

# Pre-initialize labeled counters so they always export series (even when zero).
EVENTS_INGESTED.labels(source="stream").inc(0)
EVENTS_INGESTED.labels(source="api").inc(0)

ALERTS_EMITTED.labels(severity="low").inc(0)
ALERTS_EMITTED.labels(severity="medium").inc(0)
ALERTS_EMITTED.labels(severity="high").inc(0)

ERRORS_TOTAL.labels(component="stream").inc(0)
ERRORS_TOTAL.labels(component="feature").inc(0)
ERRORS_TOTAL.labels(component="model").inc(0)
ERRORS_TOTAL.labels(component="drift").inc(0)
ERRORS_TOTAL.labels(component="api").inc(0)

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
