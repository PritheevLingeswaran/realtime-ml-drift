# realtime-ml-drift

Production-grade **streaming-first** real-time ML system for online inference, drift detection, and safe threshold adaptation.

This repo is intentionally opinionated:
- **Streaming-first** (no batch assumptions)
- **Incremental/windowed computation** (bounded state)
- **Config-driven** behavior (no hardcoded thresholds)
- **Deterministic replay** from JSONL streams for debugging/evaluation
- **Safety-first automation** (guardrails; no blind retraining)
- **Observability-first** (structured logs + Prometheus metrics)

---

## What this system does

**Input:** live events (synthetic generator or JSONL replay)  
**Processing:** windowed features → anomaly scoring → drift detection → guarded threshold tuning → alerts/metrics  
**Output:** FastAPI endpoints + Prometheus metrics + JSON logs + snapshots for replay

---

## Requirements

- Python **3.11 recommended** (3.10+ should work)
- Git (optional, for contributing)

> Note: `river` requires `numpy < 2.0`. If you hit dependency conflicts, pin `numpy==1.26.4` in `requirements.txt`.

---

## Quickstart

### 1) Install

#### macOS / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

#### Windows (CMD)
```bat
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

#### Windows (PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

### 2) Generate a deterministic stream (JSONL)
```bash
python scripts/generate_stream.py --config configs/dev.yaml --out data/raw/streams/dev_stream.jsonl
```

---

### 3) Run API (starts background stream processor in dev)

**Windows (recommended to avoid `src` import issues):**
```bat
set PYTHONPATH=.
python scripts/run_api.py --config configs/dev.yaml
```

**macOS/Linux:**
```bash
export PYTHONPATH=.
python scripts/run_api.py --config configs/dev.yaml
```

Open:
- Health: `GET /health`
- Metrics: `GET /metrics`
- Alerts: `GET /alerts`
- Score: `POST /score`

Quick check:
```bash
curl http://127.0.0.1:8000/health
```

---

### 4) Run stream-only (no API)
```bash
export PYTHONPATH=.
python scripts/run_stream.py --config configs/dev.yaml
```

---

### 5) Run evaluation (replay + compute metrics)
```bash
export PYTHONPATH=.
python scripts/run_eval.py --config configs/dev.yaml
```

This writes a summary to:
- `docs/evaluation_results.md`

---

## API Endpoints

- `GET /health` → service status + model readiness
- `GET /metrics` → Prometheus metrics
- `GET /alerts?limit=200` → recent alerts
- `POST /score` → score a single event

Example score request:
```bash
curl -X POST http://127.0.0.1:8000/score \
  -H "Content-Type: application/json" \
  -d '{"event_id":"manual_1","ts":1730000000.0,"entity_id":"acct_000001","amount":120.0,"merchant_id":"m_0001","merchant_category":"electronics","country":"US","channel":"web","device_type":"desktop"}'
```

---

## Observability

Prometheus metrics include:
- throughput: `events_ingested_total`, `events_scored_total`
- alerts: `alerts_emitted_total`
- errors: `errors_total`
- drift/adaptation: `drift_active`, `current_threshold`, `anomaly_rate_recent`
- latency histograms: `score_latency_seconds`, `feature_latency_seconds`, `drift_latency_seconds`

---

## Reality check (mandatory)

Concept drift cannot be “solved,” only **detected and managed**.
- Drift detectors have false positives and false negatives.
- Automatic adaptation can hide real incidents if misused.
- Human review is required before retraining decisions.

See:
- `docs/drift_strategy.md`
- `docs/tradeoffs.md`
- `docs/failure_cases.md`
- `docs/architecture.md`

---

## Docker (optional)
```bash
docker-compose up --build
```

API will be at:
- `http://127.0.0.1:8000/health`

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'src'`
You’re running scripts without project root on PYTHONPATH.

Fix:
- Windows CMD: `set PYTHONPATH=.`
- macOS/Linux: `export PYTHONPATH=.`

### Dependency conflict: `river` vs NumPy 2.x
Pin:
- `numpy==1.26.4`

Then reinstall:
```bash
pip install -r requirements.txt
```

### `Input X contains infinity ...`
This usually comes from `inf` features (e.g., time-since-last on first event). Ensure model input sanitization is in place in `src/models/scorer.py`.

---

## Repo map
See `docs/architecture.md` for component diagram and data flow.
