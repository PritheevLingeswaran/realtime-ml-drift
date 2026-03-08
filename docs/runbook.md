# Runbook

## Overload / Data Loss

- Default policy is `streaming.ingestion.overload_policy=backpressure`.
- If `dropped_events_total > 0` in backpressure mode, treat it as a correctness incident.
- If `overload_policy=drop`, any non-zero `drop_rate` is a critical condition by design.
- Primary signals:
  - `queue_depth`
  - `processing_lag_seconds`
  - `max_processing_lag_seconds`
  - `dropped_events_total`
  - `drop_rate`

## Required operator actions

- If lag rises and adaptation is frozen by circuit breaker:
  - reduce input rate or add worker capacity
  - keep adaptation frozen until lag normalizes
  - review whether replay timestamps are stale before using lag metrics operationally
- If drops occur in `drop` mode:
  - page on-call
  - capture the audit log entry `overload_drop`
  - switch back to `backpressure` unless loss is an explicit business decision
- If soak test fails on drops in default mode:
  - do not treat the build as production-safe
  - investigate queue sizing, scorer latency, and downstream sink latency
