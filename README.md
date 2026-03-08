# Realtime ML Drift Detection System

Streaming-first machine learning system for real-time anomaly scoring, drift detection, guarded threshold adaptation, and production-style observability.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688)
![scikit-learn](https://img.shields.io/badge/scikit--learn-ML-F7931E)
![River](https://img.shields.io/badge/River-Online%20Drift-0A7E8C)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED)

## Problem

Many machine learning systems degrade after deployment because live data stops matching the training or reference distribution. In production, this creates two expensive failure modes:

- real anomalies are missed because the model becomes desensitized to new behavior
- false alerts increase because temporary shifts are mistaken for incidents

This project solves that problem by continuously scoring streaming events, monitoring feature and prediction drift, and adapting thresholds with safety guardrails instead of blindly retraining the model. The design is relevant to fraud detection, payments monitoring, platform reliability, abuse detection, and any high-volume event stream where model quality must be monitored in real time.

## Architecture

The system is built as an online inference pipeline with bounded state, deterministic replay, and operational controls for safe deployment.

- `Data ingestion`: synthetic stream, JSONL replay, or Kafka source
- `Embedding / feature pipeline`: per-entity windowed feature engineering with incremental aggregates
- `Retrieval / model layer`: anomaly scoring using `IsolationForest` with a `ZScore` online fallback, followed by score normalization
- `API layer`: FastAPI endpoints for scoring, alerts, health, state inspection, and metrics
- `Evaluation layer`: offline replay evaluation, live benchmarking, threshold sweeps, soak testing, and drift-quality reporting

Core flow:

1. Events enter through a bounded ingestion path with idempotency and backpressure controls.
2. Pydantic schemas validate input payloads.
3. The feature pipeline updates per-entity windows and computes streaming-safe features.
4. The scoring layer produces anomaly scores.
5. The drift monitor evaluates current windows against a stable reference using PSI, KS, and ADWIN-based signals.
6. A guarded threshold controller updates alert thresholds only when safety conditions allow it.
7. Alerts, Prometheus metrics, JSON logs, and snapshots are emitted for monitoring and recovery.

![Architecture Diagram](docs/architecture.png)

## Model / Pipeline

The ML pipeline is designed for streaming operation rather than batch retraining.

- `Data preprocessing`
  - Validate incoming events with typed schemas.
  - Deduplicate events by `event_id` to support replay safety and at-least-once ingestion.
  - Maintain bounded queues and bounded per-entity state to avoid unbounded memory growth.
- `Feature engineering`
  - Build rolling entity-level features from recent transaction history.
  - Compute incremental aggregates from a window store instead of recomputing full histories.
  - Emit only config-enabled features for predictable runtime behavior.
- `Model training`
  - Train the primary anomaly scorer with `IsolationForest`.
  - Keep a `ZScore` baseline available as a deterministic, online-friendly fallback.
  - Treat the scoring model as stable in production and avoid automatic retraining.
- `Inference pipeline`
  - Ingest event
  - Update entity window
  - Generate features
  - Score anomaly likelihood
  - Normalize scores
  - Evaluate drift on a periodic cadence
  - Apply guarded threshold adaptation
  - Emit alert and monitoring signals
- `Evaluation process`
  - Replay deterministic streams for reproducible experiments
  - Measure alert quality, latency, throughput, CPU, memory, and drift-detection behavior
  - Compare adaptive thresholding against fixed-threshold baselines

## Evaluation Metrics

The repository evaluates both model quality and production performance.

- `Precision`: how many emitted alerts are correct
- `Recall`: how many true drift events are detected
- `MRR`: not currently reported in this project because the system is anomaly/drift detection rather than ranked retrieval
- `Latency`: API and pipeline response time at p50, p95, and p99
- `Throughput`: sustained requests or events processed per second

Latest measured alert-quality results from repository artifacts:

| Metric | Adaptive live run | Fixed baseline |
| --- | ---: | ---: |
| Precision | 0.571 | 0.500 |
| Recall | 0.129 | 0.258 |
| F1 | 0.211 | 0.340 |
| False alert rate | 0.429 | 0.500 |
| False alert reduction | 62.5% | 0.0% |

Notes:

- The adaptive configuration improved precision and reduced false alerts, but it traded off recall.
- `MRR` is intentionally omitted from the results table because it does not apply to this problem formulation.
- Performance metrics are reported separately below because they come from different benchmark artifacts.

## Results

The system demonstrates that guarded threshold adaptation can reduce noisy alerting while preserving production-grade performance characteristics.

- Offline benchmark results in [`docs/benchmark_report.md`](docs/benchmark_report.md) show a `42.28%` false-positive reduction between baseline and tuned configurations.
- The tuned benchmark pass maintained nearly identical throughput, with a performance gate result of `PASS`.
- Live-run evaluation in `artifacts/live_run_summary.json` reported `62.5%` false-alert reduction versus a fixed-threshold baseline.
- A 6-hour soak configuration reported `100%` availability during the recorded run window, `0` crashes, and `0` dropped events under backpressure mode.

## Latency Benchmarks

The repository includes both API benchmarking and stream-processing benchmarks.

- `Response time`
  - Live API run: p50 `6.37 ms`, p95 `8.05 ms`, p99 `8.96 ms`
  - Benchmark API run: p50 `22.00 ms`, p95 `33.79 ms`, p99 `96.39 ms`
- `Throughput`
  - Live API evaluation summary: `779.12 RPS`
  - Benchmark API summary: `746.85 RPS`
  - Soak test stream processing: `851.79 events/sec`
- `Concurrent requests`
  - Benchmark harness supports concurrent load generation and includes a `--concurrency` flag
  - Example production-style benchmark in the repo uses concurrency-driven API testing over sustained windows

## Example Output

Sample request:

```bash
curl -X POST http://127.0.0.1:8000/detect_drift \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "manual_1",
    "ts": 1730000000.0,
    "entity_id": "acct_000001",
    "amount": 120.0,
    "merchant_id": "m_0001",
    "merchant_category": "electronics",
    "country": "US",
    "channel": "web",
    "device_type": "desktop"
  }'
```

Representative response:

```json
{
  "status": "ok",
  "event_id": "manual_1",
  "drift_active": false,
  "is_anomaly": false,
  "score": 0.1842,
  "threshold": 0.8123
}
```

## Demo

- `Demo video`: add link or embed here
- `Screenshots`: add API, metrics dashboard, and alert timeline screenshots here
- `API example`: demonstrate `/score`, `/detect_drift`, and `/alerts`

Suggested assets:

- `docs/demo.mp4`
- `docs/screenshots/api.png`
- `docs/screenshots/metrics.png`
- `docs/screenshots/alerts.png`

## How to Run

### Step 1: Clone repository

```bash
git clone <your-repo-url>
cd realtime-ml-drift
```

### Step 2: Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3: Run application

Generate a deterministic stream:

```bash
python scripts/generate_stream.py --config configs/dev.yaml --out data/raw/streams/dev_stream.jsonl
```

Start the API:

```bash
export PYTHONPATH=.
python scripts/run_api.py --config configs/dev.yaml
```

Optional checks:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/metrics
curl http://127.0.0.1:8000/alerts?limit=20
```

## Tech Stack

- `Python 3.11`
- `FastAPI`
- `Uvicorn`
- `Pydantic`
- `scikit-learn`
- `River`
- `NumPy`
- `SciPy`
- `Prometheus`
- `Structlog`
- `Docker`
- `Kafka` support for streaming ingestion

Note:

- `HuggingFace` and `FAISS / Vector DB` are not used in the current implementation.
- The “embedding / retrieval” layer in this project is a streaming feature and scoring layer rather than vector search.

## Failure Cases

The system is realistic about failure modes and includes guardrails, but it is not immune to production issues.

- `False drift alarms`: temporary traffic shifts, campaigns, or seasonality can trigger alerts even when the underlying system is healthy
- `Silent drift`: slow-moving distribution changes may degrade model quality before thresholds are crossed
- `Adaptation hides incidents`: automatic threshold movement can suppress true anomalies if guardrails are too weak
- `State blow-up`: sudden growth in entity cardinality can pressure memory if window and eviction settings are misconfigured
- `Operational overload`: sustained lag or queue pressure can require freezing adaptation and prioritizing service stability

## Future Work

- Add a real architecture diagram to [`docs/architecture.png`](docs/architecture.png)
- Add dashboard screenshots and a short demo video
- Expand live evaluation with longer soak tests and multi-node deployment scenarios
- Improve recall without losing the precision gains from adaptive thresholding
- Add richer drift labeling and incident review workflows
- Support automated experiment tracking for benchmark and threshold-sweep runs
- Add container-first deployment documentation with Kubernetes examples
