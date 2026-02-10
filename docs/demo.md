# Demo

1. Generate stream:
```bash
python scripts/generate_stream.py --config configs/dev.yaml --out data/raw/streams/dev_stream.jsonl
```

2. Run API:
```bash
python scripts/run_api.py --config configs/dev.yaml
```

3. Watch metrics:
- Open `/metrics` and look at:
  - `current_threshold`
  - `drift_active`
  - `anomaly_rate_recent`
  - `alerts_emitted_total`

4. Fetch alerts:
- `GET /alerts?limit=50`
