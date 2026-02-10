# Architecture

This is a streaming-first real-time ML system. There is no batch pipeline assumption.

## Component diagram

```mermaid
flowchart LR
  A[Streaming Source\n(synthetic | JSONL replay | optional Kafka)] --> B[Validation + Schema\n(Pydantic)]
  B --> C[Online Feature Engineering\n(per-entity bounded window)]
  C --> D[Anomaly Model Scoring\n(IsolationForest/ZScore)]
  D --> E[Score Normalization\n(sigmoid)]
  E --> F[Drift Monitor\n(KS + PSI + ADWIN)]
  F --> G[Threshold Controller\n(guarded adaptation)]
  G --> H[Alert Store\n(in-memory + JSONL sink)]
  G --> I[Metrics + Logs\n(Prometheus + JSON)]
  H --> J[FastAPI\n/alerts]
  D --> J[FastAPI\n/score]
  I --> J[FastAPI\n/metrics]
```

## Key design choices (production-like)

- **Bounded state:** per-entity deque + capped window avoids unbounded memory.
- **Deterministic replay:** JSONL stream with stable event IDs makes debugging reproducible.
- **No blind retraining:** retraining is an explicit decision, not automated.
- **Guarded adaptation:** threshold tuning pauses during drift and respects cooldown/step limits.
- **Observability-first:** JSON logs and Prometheus metrics are first-class output.
