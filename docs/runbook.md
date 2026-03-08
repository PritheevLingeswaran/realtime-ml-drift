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

## Restart Recovery

- Snapshot schema is versioned. If restore fails because of a schema mismatch, the service boots clean and logs `state_restore_failed`.
- Verify `/health` and `/state` after restart:
  - `restored_state`
  - `last_snapshot_time`
  - `threshold`
  - `drift_active`
  - `dropped_events_total`
- Snapshot includes recent dedupe state. Replayed `event_id` values after restart should return `duplicate`, not mutate state twice.

## Admin Controls

- Endpoints:
  - `POST /admin/freeze_adaptation`
  - `POST /admin/unfreeze_adaptation`
  - `POST /admin/refresh_reference`
- Protection:
  - `x-api-key` must match `RTML_ADMIN_API_KEY` or configured `security.admin_api_key`
  - send `x-admin-actor`
  - send `reason` query param
- Audit expectations:
  - every admin action logs `audit_type=admin`
  - every threshold movement logs `audit_type=threshold_change`

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
- If duplicate traffic increases:
  - inspect `duplicate_events_total`
  - verify upstream delivery semantics before relaxing dedupe cache sizing
- If restore is unexpectedly false after a restart:
  - inspect snapshot path permissions and schema version
  - do not assume continuity of threshold or dedupe state until restore is confirmed
