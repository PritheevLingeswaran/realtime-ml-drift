# Benchmark Report

## Run Context
- Config: `configs/benchmark.yaml`
- Replay: `data/raw/streams/dev_stream.jsonl`
- Duration per pass: `120 sec`
- Host OS: `macOS-15.7.4-arm64-arm-64bit`
- Python: `3.11.15`
- CPU logical cores: `8`
- RAM total (GB): `8.0`


## Baseline Pass (adaptation disabled)
- Statistical validity: `PASS: alerts=3146 meets minimum required 50.`
- Throughput: `584.8892 events/sec` (`35093.35 events/min`)
- Pipeline latency (ms): p50 `1.7189`, p95 `2.0478`, p99 `2.1728`
- Drift detection latency: `0.17097254661404676` sec, `100` events
- Scored events / alerts / anomaly denominator: `70187` / `3146` / `70187`
- Alerts: `3146` (false: `395`; false alert rate: `0.1256`)
- Segment alerts (drift/non-drift): `2751` / `395`
- Segment alert rates (drift/non-drift): `0.0461` / `0.0376`
- Drift confusion (TP/FP/TN/FN): `2751` / `395` / `10102` / `56939`
- Anomaly rate: `0.0448` (denominator: `70187` scored events)
- CPU avg/p95: `98.1542% / 99.0000%`
- CPU avg/p95 normalized by cores: `12.2693% / 12.3750%`
- Peak RSS: `280.9531 MB`
- Queue depth p95: `0.0000`
- Dropped events / drop rate: `0` / `0.000000`
- Processing lag p50/p95/max: `0.001712` / `0.002029` / `0.035282` sec

## Tuned Pass (adaptation enabled)
- Statistical validity: `PASS: alerts=597 meets minimum required 50.`
- Throughput: `384.5708 events/sec` (`23074.25 events/min`)
- Pipeline latency (ms): p50 `2.4967`, p95 `3.8887`, p99 `4.0441`
- Drift detection latency: `0.2600301224489732` sec, `100` events
- Scored events / alerts / anomaly denominator: `46149` / `597` / `46149`
- Alerts: `597` (false: `123`; false alert rate: `0.2060`)
- Segment alerts (drift/non-drift): `474` / `123`
- Segment alert rates (drift/non-drift): `0.0124` / `0.0155`
- Drift confusion (TP/FP/TN/FN): `474` / `123` / `7788` / `37764`
- Anomaly rate: `0.0129` (denominator: `46149` scored events)
- CPU avg/p95: `98.4567% / 99.4000%`
- CPU avg/p95 normalized by cores: `12.3071% / 12.4250%`
- Peak RSS: `256.5156 MB`
- Queue depth p95: `0.0000`
- Dropped events / drop rate: `0` / `0.000000`
- Processing lag p50/p95/max: `0.002528` / `0.003873` / `0.108506` sec

## Baseline vs Tuned
- False positive reduction: `68.8608%`
- FP reduction validity: `PASS`
- Anomaly rate change (tuned-baseline): `-0.031887`
- Drift detection delay change seconds (tuned-baseline): `0.08905757583492646`
- Drift detection delay change events (tuned-baseline): `0`


## Safety Gate
- Status: `PASS`
- Detail: `PASS: anomaly_rate=0.0129 within safety limit 0.0500. | PASS: tuned anomaly_rate=0.0129 within +/-0.0050 of target 0.0100.`

## Reality Check
- False-alert definition used here: alert fired when `drift_tag is None` (non-drift segment).
- Drift detection is probabilistic. False positives and false negatives are unavoidable.
- Automation can suppress incidents when thresholds are misconfigured; human review remains required.
- Adaptation is rate-control, not root-cause remediation; retraining and data quality interventions are still manual decisions.
