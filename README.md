# realtime-ml-drift

Production-grade **streaming-first** real-time ML system for online inference, drift detection, and safe adaptation.

This project is intentionally opinionated:
- **Streaming-first** (no batch assumptions)
- **Incremental/windowed computation** for features and drift checks
- **Config-driven** behavior (no hard-coded thresholds)
- **Deterministic replay** from JSONL for debugging and evaluation
- **Safety-first automation** (threshold adaptation guarded; no blind retraining)
- **Observability-first** (structured logs + Prometheus metrics)

## What this system does

**Input:** live events (synthetic generator, JSONL replay, or optional Kafka/Redpanda)  
**Processing:** windowed features → anomaly scoring → drift detection → guarded threshold tuning → alerts/metrics  
**Output:** FastAPI endpoints + Prometheus metrics + JSON logs + snapshots for replay

## Quickstart (local)

### 1) Install
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Generate a deterministic stream (JSONL)
```bash
python scripts/generate_stream.py --config configs/dev.yaml --out data/raw/streams/dev_stream.jsonl
```

### 3) Run API (starts background stream processor by default in dev)
```bash
python scripts/run_api.py --config configs/dev.yaml
```

Open:
- Health: `GET /health`
- Metrics: `GET /metrics`
- Alerts: `GET /alerts`
- Score ad-hoc event: `POST /score`

### 4) Run stream-only (no API)
```bash
python scripts/run_stream.py --config configs/dev.yaml
```

### 5) Run evaluation (replay + compute metrics)
```bash
python scripts/run_eval.py --config configs/dev.yaml
```

## Reality check (read this)

Concept drift cannot be “solved,” only **detected and managed**. Drift detectors have false positives/negatives. Automatic adaptation can hide real incidents if misused. **Human review is required** before any retraining decisions.

Details:
- `docs/drift_strategy.md`
- `docs/tradeoffs.md`
- `docs/failure_cases.md`

## Repo map
See `docs/architecture.md` for component diagram and data flow.
