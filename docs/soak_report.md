# Soak Test Report

## Context
- Config: `configs/dev.yaml`
- Replay: `data/raw/streams/dev_stream.jsonl`
- Duration target (hours): `6.0`
- Smoke mode: `True`
- Max events cap: `10000`
- Actual runtime: `9.356 sec`
- Host OS: `macOS-15.7.4-arm64-arm-64bit`
- Python: `3.11.15`

## Stability
- Crash count: `0`
- Restart count: `0`
- Downtime seconds: `0.000000`
- Soak availability: `100.000000%`

## Processing
- Events seen: `10000`
- Events processed: `8500`
- Events dropped: `1500`
- Throughput: `908.476958 events/sec`

## Error Health
- Internal exceptions: `0`
- errors_total start/end/delta: `0` / `0` / `0`

## Latency Health
- Event latency p50/p95/p99: `1.0773` / `1.1055` / `1.1206` ms
- Rolling p95 avg/max/latest: `1.098367725374855` / `1.1163502982526552` / `1.096950296050636` ms

## Resource Efficiency
- CPU avg/p95: `99.4967% / 107.5805%`
- Peak RSS: `218.6875 MB`

## Reality Check
- 100% availability in a finite soak window does not prove long-term reliability.
- Counter-based error totals can miss silent data-quality failures; incident review is still required.
- Automated adaptation and alerting reduce toil but can hide incidents if guardrails are weak.
