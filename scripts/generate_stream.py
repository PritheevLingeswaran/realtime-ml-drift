from __future__ import annotations

import argparse
import os

import orjson

from src.streaming.sources import DriftScenario, SyntheticTransactionSource
from src.utils.config import load_config


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate deterministic JSONL stream")
    p.add_argument("--config", default=None, help="Config path (defaults to env CONFIG_PATH)")
    p.add_argument("--out", required=True, help="Output JSONL path")
    p.add_argument("--events", type=int, default=20000, help="Number of events")
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    cfg = load_config(args.config).raw
    scfg = cfg["streaming"]
    drift_cfg = scfg.get("drift", {})
    drift = DriftScenario(
        enabled=bool(drift_cfg.get("enabled", True)),
        drift_start_event=int(drift_cfg.get("drift_start_event", 0)),
        drift_type=str(drift_cfg.get("drift_type", "mean_shift_amount")),
    )

    src = SyntheticTransactionSource(
        seed=int(cfg["app"]["seed"]),
        event_rate_per_sec=float(scfg["event_rate_per_sec"]),
        entity_cardinality=int(scfg["entity_cardinality"]),
        max_events=args.events,
        drift=drift,
    )

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    n = 0
    async for e in src.stream():
        with open(args.out, "ab") as f:
            f.write(orjson.dumps(e.model_dump()))
            f.write(b"\n")
        n += 1
        if n >= args.events:
            break

    print(f"Wrote {n} events to {args.out}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
