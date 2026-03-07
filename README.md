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
- Predict alias: `POST /predict`
- Drift-focused scoring: `POST /detect_drift`

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
- `POST /predict` → alias for scoring endpoint
- `POST /detect_drift` → drift-focused result (`drift_active`, `score`, `threshold`)

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
- drift quality/system gauges: `drift_precision`, `drift_recall`, `cpu_usage`, `memory_usage`

---

## Drift Pipeline (Live)

Current production-style drift path is windowed and config-driven:
- Cached reference statistics are built once after the reference window is full.
- Current window is maintained with bounded deques.
- Drift evaluation runs every `drift.evaluation_interval` events (not every event) to control CPU.
- Drift score combines multiple signals:
  - feature PSI
  - feature KS
  - prediction shift
- Alerting uses quality guards:
  - feature voting (`drift.feature_vote_fraction`)
  - smoothing (`drift.smoothing_consecutive`)
  - cooldown (`drift.alert_cooldown_events`)
  - adaptive threshold (`drift.threshold_method`, `drift.threshold_k`)
- Two alert levels are produced:
  - warning: `drift_warning_active` (early signal)
  - critical: `drift_active` (smoothed + cooldown guarded)

Key config knobs in `configs/base.yaml`:
```yaml
drift:
  window_size: 1000
  evaluation_interval: 100
  threshold_method: adaptive
  threshold_k: 1.0
  feature_vote_fraction: 0.40
  smoothing_consecutive: 2
  alert_cooldown_events: 300
```

---

## Live Benchmarking (Production-style)

Important separation:
- `scripts/evaluate_realtime_drift.py` is **offline algorithm validation** only
- `scripts/benchmark_api.py` + `scripts/evaluate_live_run.py` are **live system benchmarking**

### 1) Start service
```bash
export PYTHONPATH=.
python scripts/run_api.py --config configs/dev.yaml
```

### 2) Optional: generate labeled phased stream artifact
```bash
python scripts/generate_stream_phases.py \
  --config configs/benchmark_phases.yaml \
  --events_per_second 50 \
  --out_jsonl artifacts/live_run/generated_stream.jsonl \
  --out_csv artifacts/live_run/generated_stream.csv
```

### 3) Run sustained live benchmark (5-15 min)
If you launched the API in the background, pass its PID to capture CPU/memory fallback samples:
```bash
API_PID=<your_api_pid>
```

```bash
python scripts/benchmark_api.py \
  --config configs/benchmark_phases.yaml \
  --duration_minutes 10 \
  --concurrency 20 \
  --service_pid $API_PID \
  --out_dir artifacts/live_run
```

Artifacts (per run directory):
- `requests.csv` (per-request raw log)
- `system_metrics.csv` (resource samples from `/metrics`)
- `benchmark_summary.json`
- `benchmark_config_input.yaml`
- `benchmark_config_resolved.json`
- `prometheus_final.txt`

### 4) Evaluate live run
```bash
python scripts/evaluate_live_run.py \
  --request_csv artifacts/live_run/run_<timestamp>/requests.csv \
  --resource_csv artifacts/live_run/run_<timestamp>/system_metrics.csv \
  --out_json artifacts/live_run_summary.json
```

### 5) KPIs produced
- sustained RPS
- latency p50/p95/p99
- error rate
- CPU avg/p95
- memory avg/p95
- alert precision/recall/F1
- avg/p95 detection latency from drift start to first alert
- false alert rate
- false alert reduction vs fixed-threshold baseline

Example summary shape:
```json
{
  "sustained_rps": 120.3,
  "latency_p95_ms": 48.2,
  "error_rate_percent": 0.12,
  "cpu_avg_percent": 62.5,
  "memory_avg_mb": 410.7,
  "alert_precision": 0.79,
  "alert_recall": 0.71,
  "alert_f1": 0.75,
  "avg_detection_latency_seconds": 9.4,
  "false_alert_reduction_percent": 18.6
}
```

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
