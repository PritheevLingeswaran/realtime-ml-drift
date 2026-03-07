# Benchmark Report

## Run Context
- Config: `configs/dev.yaml`
- Replay: `data/raw/streams/dev_stream.jsonl`
- Duration per pass: `10 sec`
- Host OS: `macOS-15.7.4-arm64-arm-64bit`
- Python: `3.11.15`
- CPU logical cores: `8`
- RAM total (GB): `8.0`


## Baseline Pass (adaptation disabled)
- Throughput: `904.3735 events/sec` (`54262.41 events/min`)
- Pipeline latency (ms): p50 `1.0788`, p95 `1.0962`, p99 `1.1079`
- Drift detection latency: `0.11057378187740238` sec, `100` events
- Alerts: `1` (false: `0`; false alert rate: `0.0000`)
- Segment alerts (drift/non-drift): `1` / `0`
- Segment alert rates (drift/non-drift): `0.0002` / `0.0000`
- Drift confusion (TP/FP/TN/FN): `1` / `0` / `2499` / `6544`
- Anomaly rate: `0.0001` (denominator: `9044` scored events)
- CPU avg/p95: `99.8500% / 109.0000%`
- Peak RSS: `247.5625 MB`
- Processing lag p50/p95/max: `0.001062` / `0.001078` / `0.003101` sec

## Tuned Pass (adaptation enabled)
- Throughput: `903.7827 events/sec` (`54226.96 events/min`)
- Pipeline latency (ms): p50 `1.0765`, p95 `1.1047`, p99 `1.1433`
- Drift detection latency: `0.11064606799074946` sec, `100` events
- Alerts: `2` (false: `0`; false alert rate: `0.0000`)
- Segment alerts (drift/non-drift): `2` / `0`
- Segment alert rates (drift/non-drift): `0.0003` / `0.0000`
- Drift confusion (TP/FP/TN/FN): `2` / `0` / `2499` / `6537`
- Anomaly rate: `0.0002` (denominator: `9038` scored events)
- CPU avg/p95: `98.6600% / 102.7200%`
- Peak RSS: `248.1094 MB`
- Processing lag p50/p95/max: `0.001060` / `0.001087` / `0.002913` sec

## Baseline vs Tuned
- False positive reduction: `0.0000%`
- Anomaly rate change (tuned-baseline): `0.000111`
- Drift detection delay change seconds (tuned-baseline): `7.228611334707258e-05`
- Drift detection delay change events (tuned-baseline): `0`

## Performance Comparison (Before vs After Micro-Batching)
- Before CPU avg/p95: `98.1700% / 101.1800%`
- After CPU avg/p95: `29.0800% / 68.2250%`
- Before p99 pipeline latency: `1.1435 ms`
- After p99 pipeline latency: `0.9306 ms`
- CPU avg delta (after-before): `-69.0900%`
- Throughput before/after: `899.8778` / `899.4849` events/sec
- Throughput delta (after-before): `-0.3929` events/sec


## Safety Gate
- Status: `PASS`
- Detail: `PASS: anomaly_rate=0.0002 within safety limit 0.0500. | PASS (guardrail waiver): tuned anomaly_rate=0.0002 is outside target 0.0100 +/-0.0050, but adaptation was blocked by guardrails.`

## Reality Check
- False-alert definition used here: alert fired when `drift_tag is None` (non-drift segment).
- Drift detection is probabilistic. False positives and false negatives are unavoidable.
- Automation can suppress incidents when thresholds are misconfigured; human review remains required.
- Adaptation is rate-control, not root-cause remediation; retraining and data quality interventions are still manual decisions.
