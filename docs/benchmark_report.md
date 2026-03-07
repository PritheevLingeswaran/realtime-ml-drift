# Benchmark Report

## Run Context
- Config: `configs/dev.yaml`
- Replay: `data/raw/streams/dev_stream.jsonl`
- Duration per pass: `3 sec`
- Host OS: `macOS-15.7.4-arm64-arm-64bit`
- Python: `3.11.15`
- CPU logical cores: `8`
- RAM total (GB): `None`


## Baseline Pass (adaptation disabled)
- Throughput: `887.7867 events/sec` (`53267.20 events/min`)
- Pipeline latency (ms): p50 `1.0202`, p95 `1.1132`, p99 `1.1599`
- Drift detection latency: `0.11263967248509857` sec, `100` events
- Alerts: `0` (false: `0`; false alert rate: `0.0000`)
- Segment alerts (drift/non-drift): `0` / `0`
- Segment alert rates (drift/non-drift): `0.0000` / `0.0000`
- Drift confusion (TP/FP/TN/FN): `0` / `0` / `2499` / `165`
- Anomaly rate: `0.0000` (denominator: `2664` scored events)
- CPU avg/p95: `100.0122% / 105.2189%`
- Peak RSS: `222.4219 MB`

## Tuned Pass (adaptation enabled)
- Throughput: `882.0722 events/sec` (`52924.33 events/min`)
- Pipeline latency (ms): p50 `1.0225`, p95 `1.1114`, p99 `1.1695`
- Drift detection latency: `0.11336941035110543` sec, `100` events
- Alerts: `0` (false: `0`; false alert rate: `0.0000`)
- Segment alerts (drift/non-drift): `0` / `0`
- Segment alert rates (drift/non-drift): `0.0000` / `0.0000`
- Drift confusion (TP/FP/TN/FN): `0` / `0` / `2499` / `148`
- Anomaly rate: `0.0000` (denominator: `2647` scored events)
- CPU avg/p95: `97.3096% / 98.2278%`
- Peak RSS: `223.7969 MB`

## Baseline vs Tuned
- False positive reduction: `0.0000%`
- Anomaly rate change (tuned-baseline): `0.000000`
- Drift detection delay change seconds (tuned-baseline): `0.0007297378660068576`
- Drift detection delay change events (tuned-baseline): `0`


## Safety Gate
- Status: `PASS`
- Detail: `PASS: anomaly_rate=0.0000 within safety limit 0.0500. | PASS (guardrail waiver): tuned anomaly_rate=0.0000 is outside target 0.0100 +/-0.0050, but adaptation was blocked by guardrails.`

## Reality Check
- False-alert definition used here: alert fired when `drift_tag is None` (non-drift segment).
- Drift detection is probabilistic. False positives and false negatives are unavoidable.
- Automation can suppress incidents when thresholds are misconfigured; human review remains required.
- Adaptation is rate-control, not root-cause remediation; retraining and data quality interventions are still manual decisions.
