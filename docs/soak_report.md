# Soak Test Report

## Context
- Config: `configs/dev.yaml`
- Replay: `data/raw/streams/dev_stream.jsonl`
- Duration target (hours): `6.0`
- Smoke mode: `True`
- Max events cap: `0`
- Actual runtime: `45.000 sec`
- Host OS: `macOS-15.7.4-arm64-arm-64bit`
- Python: `3.11.15`

## Stability
- Crash count: `0`
- Restart count: `0`
- Downtime seconds: `0.000000`
- Soak availability: `100.000000%`

## Processing
- Events seen: `39831`
- Events processed: `38331`
- Events dropped: `0`
- Events not scored: `1500`
- Overload policy: `backpressure`
- Drop rate: `0.000000`
- Throughput: `851.794378 events/sec`

## Error Health
- Internal exceptions: `0`
- errors_total start/end/delta: `0` / `0` / `0`

## Latency Health
- Event latency p50/p95/p99: `1.1301` / `1.2576` / `1.5857` ms
- Rolling p95 avg/max/latest: `1.2496801906130641` / `1.797258354781661` / `1.2663517612963915` ms

## Resource Efficiency
- CPU avg/p95: `98.1911% / 98.2000%`
- CPU avg/p95 normalized by cores: `12.2739% / 12.2750%`
- Peak RSS: `239.4219 MB`
- Queue depth p95: `0.0000`
- Processing lag p95/max: `0.001242` / `0.008865` sec

## Reality Check
- 100% availability in a finite soak window does not prove long-term reliability.
- Counter-based error totals can miss silent data-quality failures; incident review is still required.
- Automated adaptation and alerting reduce toil but can hide incidents if guardrails are weak.
