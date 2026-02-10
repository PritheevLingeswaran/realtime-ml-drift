# Changelog

## 0.1.0
- Initial production-style streaming ML skeleton:
  - Streaming sources (synthetic + JSONL replay)
  - Windowed features (per-entity)
  - Unsupervised anomaly scoring (Isolation Forest + normalization)
  - Drift detection (KS, PSI, ADWIN)
  - Guarded threshold adaptation
  - FastAPI service + Prometheus metrics + structured JSON logs
  - Deterministic replay and evaluation harness
