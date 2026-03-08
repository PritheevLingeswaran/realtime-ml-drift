# Production Gap Audit

## P0

- Gap: idempotency missing on live processing paths.
  Failure mode: duplicate broker deliveries or client retries double-update entity windows and emit duplicate alerts.
  Files/modules: `src/streaming/runner.py`, `src/monitoring/alerts.py`, `tests/test_production_runtime.py`
  Acceptance tests: duplicate event processed twice returns `duplicate`, window length unchanged, alert count unchanged.

- Gap: no explicit backpressure and lag circuit breaker.
  Failure mode: unbounded burst causes memory pressure or hidden event loss; adaptation keeps changing under overload.
  Files/modules: `src/streaming/runner.py`, `src/monitoring/metrics.py`, `scripts/benchmark.py`, `scripts/soak_test.py`, `tests/test_production_runtime.py`
  Acceptance tests: default `dropped_events_total == 0`, queue depth updates, lag grows, adaptation freezes when lag threshold is exceeded.

- Gap: restart recovery absent for online state.
  Failure mode: restart cold-starts thresholds, drift windows, feature state, and alert dedupe, causing false spikes after recovery.
  Files/modules: `src/streaming/state_store.py`, `src/streaming/runner.py`, `src/drift/adaptation.py`, `src/drift/monitor.py`, `src/feature_engineering/window_store.py`, `src/models/scorer.py`, `tests/test_production_runtime.py`
  Acceptance tests: snapshot save/restore preserves threshold state, restore is exposed by `/health` and `/state`, behavior continues after restart.

## P1

- Gap: no production ingestion option with consumer-group semantics.
  Failure mode: single-process source limits scaling and gives no partition ownership or commit safety.
  Files/modules: `src/streaming/kafka_source.py`, `src/streaming/runner.py`, `src/monitoring/metrics.py`, `tests/test_kafka_source.py`
  Acceptance tests: mocked consumer path yields events, commits offsets after processing, increments DLQ/deserialize metrics for malformed payloads.

- Gap: missing operational control plane.
  Failure mode: no safe way to freeze adaptation, inspect runtime state, or refresh reference during incidents.
  Files/modules: `src/api/app.py`, `src/streaming/runner.py`, `src/monitoring/metrics.py`
  Acceptance tests: protected admin endpoints require API key, `/state` exposes runtime controls and recovery state.

- Gap: lag/CPU reporting lacked normalized values and runtime lag summaries.
  Failure mode: operators cannot compare saturation across hosts or correlate degraded latency with queue pressure.
  Files/modules: `src/monitoring/benchmarking.py`, `scripts/benchmark.py`, `scripts/soak_test.py`
  Acceptance tests: benchmark and soak reports include raw CPU, normalized CPU, queue/lag metrics, and stay reproducible on replay.

## P2

- Gap: security hygiene was partial.
  Failure mode: oversized requests or unauthenticated admin access can destabilize service or bypass controls.
  Files/modules: `src/api/app.py`, `configs/base.yaml`, `docs/security.md`
  Acceptance tests: request size guard rejects oversized bodies, admin endpoints require API key, docs specify stronger production auth requirements.

- Gap: audit visibility for control changes was weak.
  Failure mode: threshold changes and admin actions are hard to trace during incident review.
  Files/modules: `src/streaming/runner.py`, `src/api/app.py`
  Acceptance tests: structured audit logs emitted for admin actions and threshold changes with timestamp and action metadata.
