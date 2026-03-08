# Architecture

This is a streaming-first real-time ML system. There is no batch pipeline assumption.

## Component diagram

```mermaid
flowchart LR
  A[Streaming Source\n(synthetic | replay | Kafka)] --> Q[Bounded Ingestion Queue\n(backpressure by default)]
  Q --> B[Validation + Schema\n(Pydantic)]
  B --> C[Online Feature Engineering\n(per-entity bounded window + incremental aggregates)]
  C --> D[Anomaly Model Scoring\n(IsolationForest/ZScore)]
  D --> E[Score Normalization\n(sigmoid)]
  E --> F[Drift Monitor\n(KS + PSI + ADWIN)]
  F --> G[Threshold Controller\n(guarded adaptation)]
  G --> H[Alert Store\n(idempotent + JSONL sink)]
  G --> S[State Snapshot Store\n(periodic save + restore)]
  G --> I[Metrics + Logs\n(Prometheus + JSON)]
  H --> J[FastAPI\n/alerts,/state]
  D --> J[FastAPI\n/score]
  I --> J[FastAPI\n/metrics]
```

## Key design choices (production-like)

- **Bounded state:** per-entity deque + capped window avoids unbounded memory.
- **Backpressure-first ingestion:** bounded queue blocks producers by default to avoid silent drops.
- **Explicit overload policy:** `streaming.ingestion.overload_policy=backpressure` is the default; `drop` is opt-in, metered, alerted, and audited.
- **Idempotent processing:** duplicate `event_id` is skipped so windows/alerts are not double-mutated.
- **Restart-safe idempotency:** snapshot persists recent `event_id` dedupe state so a replay immediately after restart still suppresses duplicates.
- **Deterministic replay:** JSONL stream with stable event IDs makes debugging reproducible.
- **Durable restart:** threshold/drift/feature/scorer/alerts/runtime snapshot is restored on startup with schema-version validation.
- **No blind retraining:** retraining is an explicit decision, not automated.
- **Guarded adaptation:** threshold tuning pauses during drift and respects cooldown/step limits.
- **Circuit breaker:** high processing lag freezes adaptation and emits critical operational alert.
- **Observability-first:** JSON logs and Prometheus metrics are first-class output.
- **Loss accounting:** `dropped_events_total`, `drop_rate`, queue depth, and lag histograms make overload visible instead of silent.
- **Operational control plane:** `/state` exposes runtime health; admin actions are API-key protected and leave audit logs with actor/reason.
