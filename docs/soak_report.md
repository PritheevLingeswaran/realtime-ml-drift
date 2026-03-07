# Soak Test Report

## Context
- Config: `configs/dev.yaml`
- Replay: `data/raw/streams/dev_stream.jsonl`
- Duration target (hours): `0.02`
- Smoke mode: `False`
- Max events cap: `0`
- Actual runtime: `72.001 sec`
- Host OS: `macOS-15.7.4-arm64-arm-64bit`
- Python: `3.11.15`

## Stability
- Crash count: `0`
- Restart count: `0`
- Downtime seconds: `0.000000`
- Soak availability: `100.000000%`

## Processing
- Events seen: `64299`
- Events processed: `62799`
- Events dropped: `1500`
- Throughput: `872.199512 events/sec`

## Error Health
- Internal exceptions: `0`
- errors_total start/end/delta: `0` / `0` / `0`

## Latency Health
- Event latency p50/p95/p99: `1.1045` / `1.2268` / `1.4166` ms
- Rolling p95 avg/max/latest: `1.193185024172396` / `1.732104153779801` / `1.1682791679049842` ms

## Resource Efficiency
- CPU avg/p95: `97.9494% / 98.2523%`
- Peak RSS: `245.3281 MB`

## Reality Check
- 100% availability in a finite soak window does not prove long-term reliability.
- Counter-based error totals can miss silent data-quality failures; incident review is still required.
- Automated adaptation and alerting reduce toil but can hide incidents if guardrails are weak.
