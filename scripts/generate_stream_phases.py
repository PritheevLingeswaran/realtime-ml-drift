from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

MERCHANT_CATEGORIES = ["grocery", "fuel", "electronics", "fashion", "pharmacy", "travel", "food"]
COUNTRIES = ["US", "IN", "CA", "GB", "AU"]
CHANNELS = ["web", "mobile", "pos"]
DEVICE_TYPES = ["ios", "android", "desktop", "unknown"]


@dataclass
class Phase:
    name: str
    duration_seconds: int
    ground_truth_drift: int
    pattern: str


@dataclass
class WorkloadConfig:
    seed: int
    entity_cardinality: int
    phases: list[Phase]


DEFAULT_CONFIG: dict[str, Any] = {
    "workload": {
        "seed": 42,
        "entity_cardinality": 10000,
        "phases": [
            {
                "name": "normal",
                "duration_seconds": 210,
                "ground_truth_drift": 0,
                "pattern": "baseline",
            },
            {
                "name": "gradual_drift",
                "duration_seconds": 150,
                "ground_truth_drift": 1,
                "pattern": "gradual",
            },
            {
                "name": "sudden_drift",
                "duration_seconds": 120,
                "ground_truth_drift": 1,
                "pattern": "sudden",
            },
            {
                "name": "noisy_non_drift",
                "duration_seconds": 120,
                "ground_truth_drift": 0,
                "pattern": "noisy",
            },
        ],
    }
}


def stable_id(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()[:24]


def load_config(path: str | None) -> WorkloadConfig:
    cfg = DEFAULT_CONFIG
    if path:
        with open(path, encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
        cfg = {**cfg, **loaded}

    raw = cfg.get("workload", {})
    phases_raw = raw.get("phases", [])
    if not phases_raw:
        raise ValueError("workload.phases must not be empty")

    phases = [
        Phase(
            name=str(p["name"]),
            duration_seconds=int(p["duration_seconds"]),
            ground_truth_drift=int(p["ground_truth_drift"]),
            pattern=str(p.get("pattern", "baseline")),
        )
        for p in phases_raw
    ]
    for ph in phases:
        if ph.duration_seconds <= 0:
            raise ValueError(f"Invalid duration_seconds for phase {ph.name}: {ph.duration_seconds}")
        if ph.ground_truth_drift not in (0, 1):
            raise ValueError(f"ground_truth_drift must be 0/1 for phase {ph.name}")

    return WorkloadConfig(
        seed=int(raw.get("seed", 42)),
        entity_cardinality=int(raw.get("entity_cardinality", 10000)),
        phases=phases,
    )


def _event_amount(rng: random.Random, pattern: str, phase_progress: float) -> float:
    base = rng.lognormvariate(3.2, 0.55)
    if pattern == "gradual":
        return base * (1.0 + 1.4 * phase_progress)
    if pattern == "sudden":
        return base * (2.5 if rng.random() < 0.7 else 1.25)
    if pattern == "noisy":
        val = base * (1.0 + rng.uniform(-0.45, 0.45))
        if rng.random() < 0.01:
            val *= 5.0
        return val
    return base


def _phase_dependent_fields(rng: random.Random, pattern: str, phase_progress: float) -> tuple[str, str]:
    country = rng.choice(COUNTRIES)
    mcat = rng.choice(MERCHANT_CATEGORIES)
    if pattern == "gradual":
        if rng.random() < (0.15 + 0.25 * phase_progress):
            mcat = "travel"
    elif pattern == "sudden":
        country = rng.choices(["US", "GB", "AU", "IN"], weights=[1, 4, 3, 1], k=1)[0]
        mcat = rng.choices(["electronics", "travel", "fashion", "food"], weights=[4, 4, 2, 1], k=1)[0]
    return country, mcat


def generate_events(config: WorkloadConfig, events_per_second: int, start_ts: float) -> list[dict[str, Any]]:
    rng = random.Random(config.seed)
    out: list[dict[str, Any]] = []
    seq = 0

    cursor_ts = start_ts
    for ph in config.phases:
        phase_events = int(ph.duration_seconds * events_per_second)
        for i in range(phase_events):
            seq += 1
            progress = i / max(1, phase_events - 1)
            amount = _event_amount(rng, ph.pattern, progress)
            country, mcat = _phase_dependent_fields(rng, ph.pattern, progress)
            entity_id = f"acct_{rng.randrange(config.entity_cardinality):06d}"
            merchant_id = f"m_{rng.randrange(3000):04d}"
            channel = rng.choice(CHANNELS)
            device = rng.choice(DEVICE_TYPES)
            event_id = stable_id("phase-gen", seq, entity_id, cursor_ts, amount, ph.name)

            out.append(
                {
                    "request_id": event_id,
                    "timestamp": cursor_ts,
                    "phase": ph.name,
                    "ground_truth_drift": int(ph.ground_truth_drift),
                    "event": {
                        "event_id": event_id,
                        "ts": float(cursor_ts),
                        "entity_id": entity_id,
                        "amount": float(max(0.01, amount)),
                        "merchant_id": merchant_id,
                        "merchant_category": mcat,
                        "country": country,
                        "channel": channel,
                        "device_type": device,
                        "drift_tag": ph.name if ph.ground_truth_drift == 1 else None,
                    },
                }
            )
            cursor_ts += 1.0 / max(1, events_per_second)

    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate labeled phased stream for live benchmark/evaluation.")
    p.add_argument("--config", default="configs/benchmark_phases.yaml")
    p.add_argument("--events_per_second", type=int, default=50)
    p.add_argument("--start_ts", type=float, default=None)
    p.add_argument("--out_jsonl", type=Path, default=Path("artifacts/live_run/generated_stream.jsonl"))
    p.add_argument("--out_csv", type=Path, default=Path("artifacts/live_run/generated_stream.csv"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    start_ts = float(args.start_ts) if args.start_ts else time.time()
    rows = generate_events(cfg, events_per_second=int(args.events_per_second), start_ts=start_ts)

    args.out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_jsonl, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    with open(args.out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["request_id", "timestamp", "phase", "ground_truth_drift", "event_json"],
        )
        w.writeheader()
        for row in rows:
            w.writerow(
                {
                    "request_id": row["request_id"],
                    "timestamp": row["timestamp"],
                    "phase": row["phase"],
                    "ground_truth_drift": row["ground_truth_drift"],
                    "event_json": json.dumps(row["event"]),
                }
            )

    print(
        json.dumps(
            {
                "events": len(rows),
                "events_per_second": args.events_per_second,
                "out_jsonl": str(args.out_jsonl),
                "out_csv": str(args.out_csv),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
