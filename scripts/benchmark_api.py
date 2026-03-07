from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import hashlib
import json
import random
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
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
class BenchmarkConfig:
    base_url: str
    duration_minutes: float
    warmup_seconds: int
    concurrency: int
    max_connections: int
    pace_sleep_ms: int
    fixed_threshold: float
    resource_sample_seconds: float
    endpoint_weights: dict[str, float]
    seed: int
    entity_cardinality: int
    phases: list[Phase]


@dataclass
class RequestRecord:
    timestamp: float
    request_id: str
    endpoint: str
    status_code: int
    latency_ms: float
    phase: str
    ground_truth_drift: int
    drift_evaluated: int
    warning_alert: int
    critical_alert: int
    adaptive_alert: int
    fixed_alert: int
    score: float
    threshold: float
    error: str


@dataclass
class ResourceSample:
    timestamp: float
    process_cpu_seconds_total: float
    process_resident_memory_bytes: float
    process_cpu_percent: float


DEFAULT_BENCHMARK_CFG = {
    "benchmark": {
        "base_url": "http://127.0.0.1:8000",
        "duration_minutes": 10,
        "warmup_seconds": 30,
        "concurrency": 20,
        "max_connections": 200,
        "pace_sleep_ms": 0,
        "fixed_threshold": 0.88,
        "resource_sample_seconds": 2.0,
        "endpoint_weights": {"/score": 0.5, "/predict": 0.3, "/detect_drift": 0.2},
    },
    "workload": {
        "seed": 42,
        "entity_cardinality": 10000,
        "phases": [
            {"name": "normal", "duration_seconds": 210, "ground_truth_drift": 0, "pattern": "baseline"},
            {"name": "gradual_drift", "duration_seconds": 150, "ground_truth_drift": 1, "pattern": "gradual"},
            {"name": "sudden_drift", "duration_seconds": 120, "ground_truth_drift": 1, "pattern": "sudden"},
            {"name": "noisy_non_drift", "duration_seconds": 120, "ground_truth_drift": 0, "pattern": "noisy"},
        ],
    },
}


def stable_id(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()[:24]


def _deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: str) -> BenchmarkConfig:
    with open(path, encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}
    merged = _deep_merge(DEFAULT_BENCHMARK_CFG, loaded)

    b = merged["benchmark"]
    w = merged["workload"]
    phases = [
        Phase(
            name=str(p["name"]),
            duration_seconds=int(p["duration_seconds"]),
            ground_truth_drift=int(p["ground_truth_drift"]),
            pattern=str(p.get("pattern", "baseline")),
        )
        for p in w["phases"]
    ]
    for ph in phases:
        if ph.duration_seconds <= 0:
            raise ValueError(f"Invalid phase duration for {ph.name}: {ph.duration_seconds}")
        if ph.ground_truth_drift not in (0, 1):
            raise ValueError(f"ground_truth_drift must be 0/1 in {ph.name}")

    endpoint_weights = {str(k): float(v) for k, v in b["endpoint_weights"].items()}
    if not endpoint_weights or sum(endpoint_weights.values()) <= 0:
        raise ValueError("benchmark.endpoint_weights must sum to > 0")

    return BenchmarkConfig(
        base_url=str(b["base_url"]).rstrip("/"),
        duration_minutes=float(b["duration_minutes"]),
        warmup_seconds=int(b["warmup_seconds"]),
        concurrency=int(b["concurrency"]),
        max_connections=int(b["max_connections"]),
        pace_sleep_ms=int(b["pace_sleep_ms"]),
        fixed_threshold=float(b["fixed_threshold"]),
        resource_sample_seconds=float(b["resource_sample_seconds"]),
        endpoint_weights=endpoint_weights,
        seed=int(w["seed"]),
        entity_cardinality=int(w["entity_cardinality"]),
        phases=phases,
    )


def phase_at(elapsed_seconds: float, phases: list[Phase]) -> tuple[Phase, float]:
    cursor = 0.0
    for ph in phases:
        nxt = cursor + ph.duration_seconds
        if elapsed_seconds < nxt:
            denom = max(1.0, float(ph.duration_seconds))
            return ph, (elapsed_seconds - cursor) / denom
        cursor = nxt
    return phases[-1], 1.0


def _event_amount(rng: random.Random, pattern: str, progress: float) -> float:
    base = rng.lognormvariate(3.2, 0.55)
    if pattern == "gradual":
        return base * (1.0 + 1.4 * max(0.0, min(1.0, progress)))
    if pattern == "sudden":
        return base * (2.4 if rng.random() < 0.75 else 1.3)
    if pattern == "noisy":
        val = base * (1.0 + rng.uniform(-0.45, 0.45))
        if rng.random() < 0.01:
            val *= 5.0
        return val
    return base


def _phase_dependent_fields(rng: random.Random, pattern: str, progress: float) -> tuple[str, str]:
    country = rng.choice(COUNTRIES)
    mcat = rng.choice(MERCHANT_CATEGORIES)
    if pattern == "gradual" and rng.random() < (0.15 + 0.25 * max(0.0, min(1.0, progress))):
        mcat = "travel"
    elif pattern == "sudden":
        country = rng.choices(["US", "GB", "AU", "IN"], weights=[1, 4, 3, 1], k=1)[0]
        mcat = rng.choices(["electronics", "travel", "fashion", "food"], weights=[4, 4, 2, 1], k=1)[0]
    return country, mcat


def build_event(rng: random.Random, seq: int, cfg: BenchmarkConfig, phase: Phase, progress: float) -> dict[str, Any]:
    ts = time.time()
    entity_id = f"acct_{rng.randrange(cfg.entity_cardinality):06d}"
    merchant_id = f"m_{rng.randrange(3000):04d}"
    channel = rng.choice(CHANNELS)
    device = rng.choice(DEVICE_TYPES)
    amount = _event_amount(rng, phase.pattern, progress)
    country, mcat = _phase_dependent_fields(rng, phase.pattern, progress)

    request_id = stable_id("live-bench", seq, entity_id, ts, amount, phase.name)
    return {
        "request_id": request_id,
        "phase": phase.name,
        "ground_truth_drift": int(phase.ground_truth_drift),
        "payload": {
            "event_id": request_id,
            "ts": float(ts),
            "entity_id": entity_id,
            "amount": float(max(0.01, amount)),
            "merchant_id": merchant_id,
            "merchant_category": mcat,
            "country": country,
            "channel": channel,
            "device_type": device,
            "drift_tag": phase.name if phase.ground_truth_drift else None,
        },
    }


def parse_prometheus_metrics(raw_text: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in raw_text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        key, value = parts[0], parts[1]
        if "{" in key:
            continue
        try:
            out[key] = float(value)
        except ValueError:
            continue
    return out


def write_request_log(path: Path, rows: list[RequestRecord]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp",
                "request_id",
                "endpoint",
                "status_code",
                "latency_ms",
                "phase",
                "ground_truth_drift",
                "drift_evaluated",
                "warning_alert",
                "critical_alert",
                "adaptive_alert",
                "fixed_alert",
                "score",
                "threshold",
                "error",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "timestamp": f"{r.timestamp:.6f}",
                    "request_id": r.request_id,
                    "endpoint": r.endpoint,
                    "status_code": r.status_code,
                    "latency_ms": f"{r.latency_ms:.4f}",
                    "phase": r.phase,
                    "ground_truth_drift": r.ground_truth_drift,
                    "drift_evaluated": r.drift_evaluated,
                    "warning_alert": r.warning_alert,
                    "critical_alert": r.critical_alert,
                    "adaptive_alert": r.adaptive_alert,
                    "fixed_alert": r.fixed_alert,
                    "score": f"{r.score:.6f}",
                    "threshold": f"{r.threshold:.6f}",
                    "error": r.error,
                }
            )


def write_resource_log(path: Path, rows: list[ResourceSample]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp",
                "process_cpu_seconds_total",
                "process_resident_memory_bytes",
                "process_cpu_percent",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "timestamp": f"{r.timestamp:.6f}",
                    "process_cpu_seconds_total": f"{r.process_cpu_seconds_total:.6f}",
                    "process_resident_memory_bytes": f"{r.process_resident_memory_bytes:.2f}",
                    "process_cpu_percent": f"{r.process_cpu_percent:.4f}",
                }
            )


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    idx = (len(vals) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


def _sample_ps(service_pid: int) -> tuple[float, float] | None:
    try:
        out = subprocess.check_output(
            ["ps", "-p", str(service_pid), "-o", "%cpu=,rss="], text=True
        ).strip()
        if not out:
            return None
        cpu_str, rss_kb_str = out.split(None, 1)
        cpu_percent = float(cpu_str)
        rss_bytes = float(rss_kb_str) * 1024.0
        return cpu_percent, rss_bytes
    except Exception:
        return None


async def run_benchmark(cfg: BenchmarkConfig, out_dir: Path, service_pid: int | None) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    run_started = time.time()

    normalized_weights = {k: v / sum(cfg.endpoint_weights.values()) for k, v in cfg.endpoint_weights.items()}
    endpoints = list(normalized_weights.keys())
    weights = [normalized_weights[e] for e in endpoints]

    rng = random.Random(cfg.seed)
    seq_counter = 0
    request_rows: list[RequestRecord] = []
    resource_rows: list[ResourceSample] = []

    limits = httpx.Limits(max_connections=cfg.max_connections, max_keepalive_connections=100)
    async with httpx.AsyncClient(limits=limits) as client:
        health = await client.get(f"{cfg.base_url}/health", timeout=5.0)
        if health.status_code >= 400:
            raise RuntimeError(f"Health check failed: {health.status_code} {health.text}")

        # Fail fast on endpoint availability.
        sample = build_event(rng, seq=0, cfg=cfg, phase=cfg.phases[0], progress=0.0)
        for ep in endpoints:
            resp = await client.post(f"{cfg.base_url}{ep}", json=sample["payload"], timeout=8.0)
            if resp.status_code >= 400:
                raise RuntimeError(f"Endpoint check failed for {ep}: {resp.status_code} {resp.text}")

        warmup_end = time.time() + cfg.warmup_seconds
        while time.time() < warmup_end:
            seq_counter += 1
            event = build_event(rng, seq=seq_counter, cfg=cfg, phase=cfg.phases[0], progress=0.0)
            with contextlib.suppress(Exception):
                await client.post(f"{cfg.base_url}/score", json=event["payload"], timeout=8.0)

        bench_start = time.time()
        duration_seconds = int(cfg.duration_minutes * 60)
        bench_end = bench_start + duration_seconds

        async def sample_resources() -> None:
            while time.time() < bench_end:
                ts = time.time()
                try:
                    r = await client.get(f"{cfg.base_url}/metrics", timeout=8.0)
                    if r.status_code < 400:
                        parsed = parse_prometheus_metrics(r.text)
                        cpu_seconds = float(parsed.get("process_cpu_seconds_total", 0.0))
                        mem_bytes = float(parsed.get("process_resident_memory_bytes", 0.0))
                        if cpu_seconds > 0.0 or mem_bytes > 0.0:
                            resource_rows.append(
                                ResourceSample(
                                    timestamp=ts,
                                    process_cpu_seconds_total=cpu_seconds,
                                    process_resident_memory_bytes=mem_bytes,
                                    process_cpu_percent=0.0,
                                )
                            )
                        elif service_pid:
                            ps_sample = _sample_ps(service_pid)
                            if ps_sample:
                                cpu_percent, rss_bytes = ps_sample
                                resource_rows.append(
                                    ResourceSample(
                                        timestamp=ts,
                                        process_cpu_seconds_total=0.0,
                                        process_resident_memory_bytes=rss_bytes,
                                        process_cpu_percent=cpu_percent,
                                    )
                                )
                    elif service_pid:
                        ps_sample = _sample_ps(service_pid)
                        if ps_sample:
                            cpu_percent, rss_bytes = ps_sample
                            resource_rows.append(
                                ResourceSample(
                                    timestamp=ts,
                                    process_cpu_seconds_total=0.0,
                                    process_resident_memory_bytes=rss_bytes,
                                    process_cpu_percent=cpu_percent,
                                )
                            )
                except Exception:
                    if service_pid:
                        ps_sample = _sample_ps(service_pid)
                        if ps_sample:
                            cpu_percent, rss_bytes = ps_sample
                            resource_rows.append(
                                ResourceSample(
                                    timestamp=ts,
                                    process_cpu_seconds_total=0.0,
                                    process_resident_memory_bytes=rss_bytes,
                                    process_cpu_percent=cpu_percent,
                                )
                            )
                await asyncio.sleep(cfg.resource_sample_seconds)

        async def worker(worker_id: int) -> None:
            nonlocal seq_counter
            wrng = random.Random(cfg.seed + worker_id + 100)
            while time.time() < bench_end:
                now = time.time()
                elapsed = max(0.0, now - bench_start)
                phase, progress = phase_at(elapsed, cfg.phases)

                seq_counter += 1
                event = build_event(wrng, seq=seq_counter, cfg=cfg, phase=phase, progress=progress)
                endpoint = wrng.choices(endpoints, weights=weights, k=1)[0]

                t0 = time.perf_counter()
                status_code = 0
                err = ""
                adaptive_alert = 0
                fixed_alert = 0
                drift_evaluated = 0
                warning_alert = 0
                critical_alert = 0
                score = 0.0
                threshold = 0.0

                try:
                    response = await client.post(f"{cfg.base_url}{endpoint}", json=event["payload"], timeout=10.0)
                    status_code = int(response.status_code)
                    if response.status_code < 400:
                        body = response.json()
                        if body.get("status") == "scored":
                            score = float(body.get("drift_score", body.get("score", 0.0)))
                            threshold = float(body.get("drift_threshold", cfg.fixed_threshold))
                            drift_evaluated = int(bool(body.get("drift_evaluated", False)))
                            warning_alert = int(bool(body.get("drift_warning_active", False)))
                            critical_alert = int(bool(body.get("drift_active", False)))
                            adaptive_alert = critical_alert
                            fixed_alert = int(bool(body.get("drift_fixed_active", False)))
                    else:
                        err = response.text[:200]
                except Exception as ex:  # noqa: BLE001
                    err = str(ex)

                latency_ms = (time.perf_counter() - t0) * 1000.0
                request_rows.append(
                    RequestRecord(
                        timestamp=time.time(),
                        request_id=str(event["request_id"]),
                        endpoint=endpoint,
                        status_code=status_code,
                        latency_ms=latency_ms,
                        phase=phase.name,
                        ground_truth_drift=int(event["ground_truth_drift"]),
                        drift_evaluated=drift_evaluated,
                        warning_alert=warning_alert,
                        critical_alert=critical_alert,
                        adaptive_alert=adaptive_alert,
                        fixed_alert=fixed_alert,
                        score=score,
                        threshold=threshold,
                        error=err,
                    )
                )

                if cfg.pace_sleep_ms > 0:
                    await asyncio.sleep(cfg.pace_sleep_ms / 1000.0)

        await asyncio.gather(sample_resources(), *(worker(i) for i in range(cfg.concurrency)))

        # Save one final raw prometheus snapshot for reproducibility.
        metrics_raw = await client.get(f"{cfg.base_url}/metrics", timeout=8.0)
        metrics_path = out_dir / "prometheus_final.txt"
        metrics_path.write_text(metrics_raw.text if metrics_raw.status_code < 400 else "", encoding="utf-8")

    run_finished = time.time()
    duration_real = max(1e-9, run_finished - bench_start)
    ok = [r for r in request_rows if 200 <= r.status_code < 400]
    errors = len(request_rows) - len(ok)
    latencies = [r.latency_ms for r in ok]

    summary = {
        "run_started_ts": run_started,
        "run_finished_ts": run_finished,
        "duration_seconds_target": int(cfg.duration_minutes * 60),
        "duration_seconds_actual": round(duration_real, 3),
        "base_url": cfg.base_url,
        "requests_total": len(request_rows),
        "requests_ok": len(ok),
        "requests_error": errors,
        "throughput_rps": round(len(request_rows) / duration_real, 3),
        "latency_p50_ms": round(percentile(latencies, 0.50), 3),
        "latency_p95_ms": round(percentile(latencies, 0.95), 3),
        "latency_p99_ms": round(percentile(latencies, 0.99), 3),
        "error_rate_percent": round((errors / len(request_rows) * 100.0) if request_rows else 0.0, 4),
        "request_log": str(out_dir / "requests.csv"),
        "resource_log": str(out_dir / "system_metrics.csv"),
    }

    write_request_log(out_dir / "requests.csv", request_rows)
    write_resource_log(out_dir / "system_metrics.csv", resource_rows)
    (out_dir / "benchmark_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark live realtime-ml-drift API with phased traffic.")
    p.add_argument("--config", default="configs/benchmark_phases.yaml")
    p.add_argument("--base_url", default=None)
    p.add_argument("--duration_minutes", type=float, default=None)
    p.add_argument("--concurrency", type=int, default=None)
    p.add_argument("--service_pid", type=int, default=None)
    p.add_argument("--out_dir", type=Path, default=Path("artifacts/live_run"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)

    # CLI overrides for quick experiments.
    if args.base_url:
        cfg.base_url = args.base_url.rstrip("/")
    if args.duration_minutes is not None:
        cfg.duration_minutes = float(args.duration_minutes)
    if args.concurrency is not None:
        cfg.concurrency = int(args.concurrency)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    run_id = time.strftime("%Y%m%d_%H%M%S")
    run_dir = args.out_dir / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Persist exact config used.
    with open(run_dir / "benchmark_config_resolved.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "base_url": cfg.base_url,
                "duration_minutes": cfg.duration_minutes,
                "warmup_seconds": cfg.warmup_seconds,
                "concurrency": cfg.concurrency,
                "max_connections": cfg.max_connections,
                "pace_sleep_ms": cfg.pace_sleep_ms,
                "fixed_threshold": cfg.fixed_threshold,
                "resource_sample_seconds": cfg.resource_sample_seconds,
                "endpoint_weights": cfg.endpoint_weights,
                "seed": cfg.seed,
                "entity_cardinality": cfg.entity_cardinality,
                "phases": [ph.__dict__ for ph in cfg.phases],
            },
            f,
            indent=2,
        )
    shutil.copyfile(args.config, run_dir / "benchmark_config_input.yaml")

    summary = asyncio.run(run_benchmark(cfg, run_dir, service_pid=args.service_pid))
    print(json.dumps(summary, indent=2))
    print(f"\nSaved benchmark artifacts to: {run_dir}")


if __name__ == "__main__":
    main()
