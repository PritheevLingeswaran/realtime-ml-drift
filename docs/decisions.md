# Decisions

## Why Isolation Forest?
- Common unsupervised baseline for anomaly scoring in production.
- Stable scoring if you avoid frequent retraining.
- Works with tabular windowed features.

## Why windowed per-entity features?
- Many real incidents are *behavioral* deviations per entity (account/device).
- Windowed stats are incremental and streaming-friendly.

## Why KS + PSI + ADWIN?
- KS and PSI are standard for distribution shift; complementary failure modes.
- ADWIN provides streaming early-warning on mean shifts.
- Ensemble of signals reduces dependence on a single detector.

## Why guarded adaptation instead of auto-retrain?
- Auto-retrain is risky without labels/ground truth.
- Threshold tuning is easier to audit and roll back.
- Guardrails prevent runaway alert suppression.

## Why explicit backpressure over silent drops?
- Dropped events hide incidents and break replay/debug parity.
- Bounded queue with blocking ingestion keeps failure mode visible (lag) and measurable.
- Overload drops are opt-in via `overload_policy=drop`, counted (`dropped_events_total`), alerted, and audit-logged.

## Why event_id idempotency at runtime?
- Broker retries/rebalances create duplicate deliveries in at-least-once mode.
- Duplicate suppression prevents double feature updates and duplicate alerts.
- Keeps threshold/drift state stable under replays and restart storms.

## Why snapshot+restore for online state?
- Restarting without state causes cold-start behavior and alert instability.
- Persisting threshold/drift/reference/window summaries preserves operational continuity.
- Snapshot schema versioning keeps future migrations explicit.

## Why keep a benchmark-only overlay?
- Production defaults stay conservative because false suppression is safer than runaway tuning.
- Benchmark runs need measurable alert volume so FP/false-alert metrics have enough sample size to mean anything.
- `configs/benchmark.yaml` relaxes only convergence guardrails needed for a timed benchmark; the 5% hard anomaly safety gate still applies.
