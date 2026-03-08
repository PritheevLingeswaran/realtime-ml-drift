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
- Statistical validity: `PASS: alerts=3155 meets minimum required 50.`
- Throughput: `585.8597 events/sec` (`35151.58 events/min`)
- Pipeline latency (ms): p50 `1.7196`, p95 `2.0617`, p99 `2.2860`
- Drift detection latency: `0.17068931687105618` sec, `100` events
- Scored events / alerts / anomaly denominator: `70304` / `3155` / `70304`
- Alerts: `3155` (false: `395`; false alert rate: `0.1252`)
- Segment alerts (drift/non-drift): `2760` / `395`
- Segment alert rates (drift/non-drift): `0.0461` / `0.0376`
- Drift confusion (TP/FP/TN/FN): `2760` / `395` / `10102` / `57047`
- Anomaly rate: `0.0449` (denominator: `70304` scored events)
- CPU avg/p95: `98.5142% / 99.0000%`
- CPU avg/p95 normalized by cores: `12.3143% / 12.3750%`
- Peak RSS: `275.2969 MB`
- Queue depth p95: `0.0000`
- Dropped events / drop rate: `0` / `0.000000`
- Processing lag p50/p95/max: `0.001719` / `0.002042` / `0.047668` sec
- Component time per event (feature_engineering/scoring/drift_adwin/drift_ks_psi/adaptation/snapshotting/logging_metrics ms): `0.0064` / `1.0025` / `0.0021` / `0.0053` / `0.0009` / `0.0010` / `0.6325`

## Tuned Pass (adaptation enabled)
- Statistical validity: `PASS: alerts=1435 meets minimum required 50.`
- Throughput: `585.5697 events/sec` (`35134.18 events/min`)
- Pipeline latency (ms): p50 `1.7156`, p95 `2.0428`, p99 `2.2530`
- Drift detection latency: `0.17077385327669592` sec, `100` events
- Scored events / alerts / anomaly denominator: `70269` / `1435` / `70269`
- Alerts: `1435` (false: `228`; false alert rate: `0.1589`)
- Segment alerts (drift/non-drift): `1207` / `228`
- Segment alert rates (drift/non-drift): `0.0202` / `0.0217`
- Drift confusion (TP/FP/TN/FN): `1207` / `228` / `10269` / `58565`
- Anomaly rate: `0.0204` (denominator: `70269` scored events)
- CPU avg/p95: `98.2867% / 98.9000%`
- CPU avg/p95 normalized by cores: `12.2858% / 12.3625%`
- Peak RSS: `244.6094 MB`
- Queue depth p95: `0.0000`
- Dropped events / drop rate: `0` / `0.000000`
- Processing lag p50/p95/max: `0.001707` / `0.002024` / `0.074804` sec
- Component time per event (feature_engineering/scoring/drift_adwin/drift_ks_psi/adaptation/snapshotting/logging_metrics ms): `0.0064` / `0.9994` / `0.0021` / `0.0053` / `0.0048` / `0.0016` / `0.6317`

## Baseline vs Tuned
- False positive reduction: `42.2785%`
- FP reduction validity: `PASS`
- Anomaly rate change (tuned-baseline): `-0.024455`
- Drift detection delay change seconds (tuned-baseline): `8.453640563974019e-05`
- Drift detection delay change events (tuned-baseline): `0`
- Performance gate: `PASS: tuned throughput ratio=0.9995 (min 0.9500), tuned p99 ratio=0.9856 (max 1.1500).`
- Top per-event regressions:
- adaptation: baseline `0.0009 ms/event`, tuned `0.0048 ms/event`, delta `0.0039`
- snapshotting: baseline `0.0010 ms/event`, tuned `0.0016 ms/event`, delta `0.0005`
- drift_ks_psi: baseline `0.0053 ms/event`, tuned `0.0053 ms/event`, delta `0.0000`


## Safety Gate
- Status: `PASS`
- Detail: `PASS: anomaly_rate=0.0204 within safety limit 0.0500. | PASS: tuned throughput ratio=0.9995 (min 0.9500), tuned p99 ratio=0.9856 (max 1.1500). | PASS (guardrail waiver): tuned anomaly_rate=0.0204 is outside target 0.0100 +/-0.0050, but adaptation was blocked by guardrails.`

## Reality Check
- False-alert definition used here: alert fired when `drift_tag is None` (non-drift segment).
- Drift detection is probabilistic. False positives and false negatives are unavoidable.
- Automation can suppress incidents when thresholds are misconfigured; human review remains required.
- Adaptation is rate-control, not root-cause remediation; retraining and data quality interventions are still manual decisions.
