from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

import orjson

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.monitoring import metrics as m  # noqa: E402
from src.monitoring.benchmarking import (  # noqa: E402
    ResourceSampler,
    environment_info,
    percentile,
    read_errors_total_best_effort,
)
from src.schemas.event_schema import Event  # noqa: E402
from src.streaming.runner import build_state, process_event  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Long-running soak stability test for stream processor.")
    p.add_argument("--config", default="configs/dev.yaml")
    p.add_argument("--replay", default="data/raw/streams/dev_stream.jsonl")
    p.add_argument("--duration_hours", type=float, default=6.0)
    p.add_argument("--max_events", type=int, default=0, help="0 => no cap")
    p.add_argument(
        "--smoke_mode",
        action="store_true",
        help="CI smoke run (overrides duration to smoke_seconds).",
    )
    p.add_argument("--smoke_seconds", type=int, default=45)
    p.add_argument("--rolling_window_events", type=int, default=1000)
    p.add_argument("--rolling_emit_every_events", type=int, default=250)
    p.add_argument("--report_md", default="docs/soak_report.md")
    p.add_argument("--report_json", default="data/processed/snapshots/soak_report.json")
    return p.parse_args()


def load_replay(path: Path) -> list[Event]:
    if not path.exists():
        raise FileNotFoundError(f"Replay file not found: {path}")
    out: list[Event] = []
    with open(path, "rb") as f:
        for line in f:
            out.append(Event(**orjson.loads(line)))
    if not out:
        raise ValueError(f"Replay file has no events: {path}")
    return out


def compute_availability_percent(runtime_seconds: float, downtime_seconds: float) -> float:
    if runtime_seconds <= 0:
        return 0.0
    avail = 1.0 - (max(0.0, downtime_seconds) / runtime_seconds)
    avail = max(0.0, min(1.0, avail))
    return avail * 100.0


def summarize_latency(latencies_ms: list[float]) -> dict[str, float]:
    return {
        "p50_ms": percentile(latencies_ms, 0.50),
        "p95_ms": percentile(latencies_ms, 0.95),
        "p99_ms": percentile(latencies_ms, 0.99),
    }


def rolling_p95_summary(series: list[dict[str, float]]) -> dict[str, float | None]:
    if not series:
        return {"avg_ms": None, "max_ms": None, "latest_ms": None}
    vals = [float(x["rolling_p95_ms"]) for x in series]
    return {
        "avg_ms": sum(vals) / len(vals),
        "max_ms": max(vals),
        "latest_ms": vals[-1],
    }


def render_md(report: dict[str, Any]) -> str:
    lat = report["latency"]
    roll = report["rolling_latency_p95"]
    return f"""# Soak Test Report

## Context
- Config: `{report['inputs']['config']}`
- Replay: `{report['inputs']['replay']}`
- Duration target (hours): `{report['inputs']['duration_hours']}`
- Smoke mode: `{report['inputs']['smoke_mode']}`
- Max events cap: `{report['inputs']['max_events']}`
- Actual runtime: `{report['runtime_seconds']:.3f} sec`
- Host OS: `{report['environment']['os']}`
- Python: `{report['environment']['python_version']}`

## Stability
- Crash count: `{report['crash_count']}`
- Restart count: `{report['restart_count']}`
- Downtime seconds: `{report['downtime_seconds']:.6f}`
- Soak availability: `{report['soak_availability_percent']:.6f}%`

## Processing
- Events seen: `{report['events_seen']}`
- Events processed: `{report['events_processed']}`
- Events dropped: `{report['events_dropped']}`
- Throughput: `{report['events_per_sec']:.6f} events/sec`

## Error Health
- Internal exceptions: `{report['internal_exception_count']}`
- errors_total start/end/delta: `{report['errors_total_start']}` / `{report['errors_total_end']}` / `{report['errors_total_delta']}`

## Latency Health
- Event latency p50/p95/p99: `{lat['p50_ms']:.4f}` / `{lat['p95_ms']:.4f}` / `{lat['p99_ms']:.4f}` ms
- Rolling p95 avg/max/latest: `{roll['avg_ms']}` / `{roll['max_ms']}` / `{roll['latest_ms']}` ms

## Resource Efficiency
- CPU avg/p95: `{report['resource_efficiency']['cpu_avg_percent']:.4f}% / {report['resource_efficiency']['cpu_p95_percent']:.4f}%`
- Peak RSS: `{report['resource_efficiency']['peak_rss_mb']:.4f} MB`

## Reality Check
- 100% availability in a finite soak window does not prove long-term reliability.
- Counter-based error totals can miss silent data-quality failures; incident review is still required.
- Automated adaptation and alerting reduce toil but can hide incidents if guardrails are weak.
"""


def main() -> int:
    args = parse_args()
    replay = load_replay(Path(args.replay))

    duration_sec = max(1.0, float(args.duration_hours) * 3600.0)
    if args.smoke_mode:
        # CI/smoke path: short but non-trivial runtime window.
        duration_sec = float(max(30, min(60, int(args.smoke_seconds))))

    start_wall = time.perf_counter()
    end_wall = start_wall + duration_sec

    state = build_state(args.config)
    runner = asyncio.Runner()

    crashes = 0
    restarts = 0
    downtime = 0.0
    internal_errors = 0
    processed = 0
    dropped = 0
    seen = 0
    cursor = 0

    latencies_ms: list[float] = []
    rolling_window = deque(maxlen=max(50, int(args.rolling_window_events)))
    rolling_series: list[dict[str, float]] = []

    errors_total_start = read_errors_total_best_effort(0)

    sampler = ResourceSampler(sample_sec=1.0)
    sampler.start()

    while time.perf_counter() < end_wall:
        if args.max_events > 0 and seen >= int(args.max_events):
            break

        e = replay[cursor % len(replay)]
        cursor += 1
        seen += 1

        t0 = time.perf_counter()
        try:
            out = runner.run(process_event(state, e, source="soak"))
            lat_ms = (time.perf_counter() - t0) * 1000.0
            latencies_ms.append(lat_ms)

            if out.get("status") == "scored":
                processed += 1
                rolling_window.append(lat_ms)
            else:
                dropped += 1

            if processed > 0 and (processed % max(1, int(args.rolling_emit_every_events))) == 0:
                rolling_series.append(
                    {
                        "events_processed": float(processed),
                        "rolling_p95_ms": float(percentile(list(rolling_window), 0.95)),
                    }
                )

        except Exception:  # noqa: BLE001
            # Why: soak test must measure resilience, not just fast-fail.
            crashes += 1
            internal_errors += 1
            dropped += 1
            m.ERRORS_TOTAL.labels(component="stream").inc()

            down_start = time.perf_counter()
            with contextlib.suppress(Exception):
                runner.close()

            state = build_state(args.config)
            runner = asyncio.Runner()
            restarts += 1
            downtime += max(0.0, time.perf_counter() - down_start)

    sampler.stop()
    with contextlib.suppress(Exception):
        runner.close()

    elapsed = max(1e-9, time.perf_counter() - start_wall)
    availability_percent = compute_availability_percent(elapsed, downtime)
    errors_total_end = read_errors_total_best_effort(internal_errors)

    report = {
        "inputs": {
            "config": args.config,
            "replay": args.replay,
            "duration_hours": float(args.duration_hours),
            "smoke_mode": bool(args.smoke_mode),
            "max_events": int(args.max_events),
        },
        "environment": environment_info(),
        "runtime_seconds": elapsed,
        "events_seen": seen,
        "events_processed": processed,
        "events_dropped": dropped,
        "events_per_sec": processed / elapsed,
        "crash_count": crashes,
        "restart_count": restarts,
        "downtime_seconds": downtime,
        "soak_availability_percent": availability_percent,
        "errors_total_start": errors_total_start,
        "errors_total_end": errors_total_end,
        "errors_total_delta": max(0, errors_total_end - errors_total_start),
        "internal_exception_count": internal_errors,
        "latency": summarize_latency(latencies_ms),
        "rolling_latency_p95": {
            **rolling_p95_summary(rolling_series),
            "series": rolling_series,
        },
        "resource_efficiency": sampler.stats(),
        "generated_at_unix": time.time(),
    }

    md_path = Path(args.report_md)
    json_path = Path(args.report_json)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    md = render_md(report)
    md_path.write_text(md, encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(md)
    print(f"\nWrote soak Markdown report: {md_path}")
    print(f"Wrote soak JSON report: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
