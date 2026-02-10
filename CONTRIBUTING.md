# Contributing

This project is designed to mirror how production teams work:
- Small composable modules
- Config-driven behavior
- Strong typing and schemas
- Tests for correctness and regressions
- Observability first

## Dev workflow
```bash
make install
make test
make lint
```

## Guidelines
- No hard-coded thresholds/paths: use config.
- Keep streaming-safe logic (incremental updates, bounded state).
- Add tests for non-trivial logic (window eviction, drift triggers, guardrails).
- Prefer deterministic behavior for replay/debugging (seeded RNG, stable IDs).
