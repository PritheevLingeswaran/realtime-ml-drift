# Evaluation Results

Use the measured runners below (no hand-edited metrics):

```bash
python scripts/benchmark.py --config configs/dev.yaml --replay data/raw/streams/dev_stream.jsonl --duration_sec 600
python scripts/benchmark.py --config configs/dev.yaml --replay data/raw/streams/dev_stream.jsonl --duration_sec 120 --performance_compare --micro_batch_size 128 --performance_target_eps 900
python scripts/soak_test.py --config configs/dev.yaml --duration_hours 6
```

Outputs:
- Benchmark Markdown: `docs/benchmark_report.md`
- Benchmark JSON: `data/processed/snapshots/benchmark_report.json`
- Soak Markdown: `docs/soak_report.md`
- Soak JSON: `data/processed/snapshots/soak_report.json`

Notes:
- Drift detection latency here means **detection delay** (drift onset to first `drift_active=True`).
- Pipeline latency is end-to-end event processing latency, not API response latency.
- Performance comparison mode reports CPU avg/p95, throughput, and p99 pipeline latency before vs after micro-batching on the same replay file.
- Reports include a Reality Check section by design (FP/FN tradeoffs, human review requirement).
