from __future__ import annotations

import argparse
import asyncio
import time

import orjson

from src.schemas.event_schema import Event
from src.streaming.runner import build_state, process_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="configs/dev.yaml")
    p.add_argument("--replay", default="data/raw/streams/dev_stream.jsonl")
    return p.parse_args()


def main():
    args = parse_args()
    state = build_state(args.config)

    drift_start_ts = None
    drift_start_idx = None
    detect_ts = None
    detect_idx = None

    total = 0
    scored = 0
    alerts = 0
    false_alerts = 0

    t0 = time.time()

    with asyncio.Runner() as runner:
        with open(args.replay, "rb") as f:
            for idx, line in enumerate(f, start=1):
                e = Event(**orjson.loads(line))
                total += 1

                out = __run(runner, state, e)
                if out.get("status") != "scored":
                    continue

                scored += 1

                # Ground-truth drift start.
                if drift_start_ts is None and e.drift_tag is not None:
                    drift_start_ts = e.ts
                    drift_start_idx = idx

                # First detection moment.
                if drift_start_ts is not None and detect_ts is None and out.get("drift_active") is True:
                    detect_ts = e.ts
                    detect_idx = idx

                # Alert counting.
                if out.get("is_anomaly") is True:
                    alerts += 1
                    if e.drift_tag is None:
                        false_alerts += 1

    t1 = time.time()
    duration = max(1e-9, t1 - t0)
    eps = scored / duration
    epm = eps * 60

    print("\n=== Resume Metrics ===")
    print(f"Events processed (total): {total}")
    print(f"Events processed (scored): {scored}")
    print(f"Throughput: {eps:.2f} events/sec  ({epm:.0f} events/min)")

    if drift_start_ts and detect_ts:
        print(f"Drift detection delay (time): {float(detect_ts) - float(drift_start_ts):.3f} sec")
        print(f"Drift detection delay (events): {int(detect_idx) - int(drift_start_idx)} events")
    else:
        print("Drift detection delay: NOT MEASURED (no drift or no detection)")

    if alerts > 0:
        fp_rate = false_alerts / alerts
        print(f"Alerts: {alerts}")
        print(f"False alerts (during non-drift): {false_alerts}")
        print(f"False alert rate: {fp_rate:.4f}")
    else:
        print("Alerts: 0 (threshold too high or model not warmed up)")


def __run(runner: asyncio.Runner, state, e):
    return runner.run(process_event(state, e, source="eval"))


if __name__ == "__main__":
    main()
