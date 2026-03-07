from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

MERCHANT_CATEGORIES = ["grocery", "fuel", "electronics", "fashion", "pharmacy", "travel", "food"]
COUNTRIES = ["US", "IN", "CA", "GB", "AU"]
CHANNELS = ["web", "mobile", "pos"]
DEVICE_TYPES = ["ios", "android", "desktop", "unknown"]


def stable_id(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()[:24]


@dataclass
class RequestRecord:
    latency_ms: float
    endpoint: str
    ok: bool
    adaptive_alert: int
    fixed_alert: int
    true_drift: int
    phase: str


@dataclass
class SamplePoint:
    ts: float
    process_cpu_seconds_total: float
    process_resident_memory_bytes: float


class LoadScenario:
    def __init__(self, entity_cardinality: int, fixed_threshold: float, seed: int = 42) -> None:
        self.rng = random.Random(seed)
        self.entity_cardinality = entity_cardinality
        self.fixed_threshold = fixed_threshold
        self.count = 0

    def phase_at(self, t_norm: float) -> tuple[str, bool]:
        if t_norm < 0.35:
            return "normal", False
        if t_norm < 0.60:
            return "gradual_drift", True
        if t_norm < 0.80:
            return "sudden_drift", True
        return "noisy_non_drift", False

    def make_event(self, t_norm: float) -> tuple[dict[str, Any], int, str]:
        self.count += 1
        phase, drift = self.phase_at(t_norm)

        ts = time.time()
        entity_id = f"acct_{self.rng.randrange(self.entity_cardinality):06d}"
        merchant_id = f"m_{self.rng.randrange(3000):04d}"
        country = self.rng.choice(COUNTRIES)
        channel = self.rng.choice(CHANNELS)
        device = self.rng.choice(DEVICE_TYPES)
        mcat = self.rng.choice(MERCHANT_CATEGORIES)

        base = self.rng.lognormvariate(3.2, 0.55)
        amount = base

        if phase == "gradual_drift":
            progress = max(0.0, min(1.0, (t_norm - 0.35) / 0.25))
            amount = base * (1.0 + 1.4 * progress)
            if self.rng.random() < 0.20 + (0.20 * progress):
                mcat = "travel"
        elif phase == "sudden_drift":
            amount = base * (2.4 if self.rng.random() < 0.75 else 1.3)
            country = self.rng.choices(["US", "GB", "AU", "IN"], weights=[1, 4, 3, 1], k=1)[0]
            mcat = self.rng.choices(
                ["electronics", "travel", "fashion", "food"], weights=[4, 4, 2, 1], k=1
            )[0]
        elif phase == "noisy_non_drift":
            amount = base * (1.0 + self.rng.uniform(-0.45, 0.45))
            if self.rng.random() < 0.01:
                amount *= 5.0

        event_id = stable_id("bench", self.count, entity_id, ts, amount, merchant_id, phase)
        payload = {
            "event_id": event_id,
            "ts": float(ts),
            "entity_id": entity_id,
            "amount": float(max(0.01, amount)),
            "merchant_id": merchant_id,
            "merchant_category": mcat,
            "country": country,
            "channel": channel,
            "device_type": device,
            "drift_tag": phase if drift else None,
        }
        return payload, int(drift), phase


def _percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = (len(sorted_values) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac


def _precision_recall_f1(y_true: list[int], y_pred: list[int]) -> dict[str, float | int]:
    tp = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 1 and p == 1)
    fp = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 0 and p == 1)
    fn = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 1 and p == 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if precision + recall else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def _parse_prometheus_metrics(raw_text: str) -> dict[str, float]:
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


async def _sample_resources(client: httpx.AsyncClient, base_url: str) -> SamplePoint | None:
    try:
        r = await client.get(f"{base_url}/metrics", timeout=5.0)
        r.raise_for_status()
        parsed = _parse_prometheus_metrics(r.text)
        return SamplePoint(
            ts=time.time(),
            process_cpu_seconds_total=float(parsed.get("process_cpu_seconds_total", 0.0)),
            process_resident_memory_bytes=float(parsed.get("process_resident_memory_bytes", 0.0)),
        )
    except Exception:
        return None


async def _worker(
    worker_id: int,
    client: httpx.AsyncClient,
    base_url: str,
    end_ts: float,
    scenario: LoadScenario,
    records: list[RequestRecord],
    fixed_threshold: float,
    pace_sleep_s: float,
    endpoint_weights: list[tuple[str, float]],
) -> None:
    rng = random.Random(1000 + worker_id)
    endpoints = [e for e, _ in endpoint_weights]
    weights = [w for _, w in endpoint_weights]

    while time.time() < end_ts:
        now = time.time()
        total_duration = max(end_ts - (end_ts - 1), 1.0)
        elapsed = max(0.0, now - (end_ts - total_duration))
        t_norm = max(0.0, min(1.0, elapsed / total_duration))
        payload, true_drift, phase = scenario.make_event(t_norm=t_norm)

        endpoint = rng.choices(endpoints, weights=weights, k=1)[0]
        t0 = time.perf_counter()
        ok = False
        adaptive_alert = 0
        fixed_alert = 0

        try:
            r = await client.post(f"{base_url}{endpoint}", json=payload, timeout=10.0)
            ok = r.status_code < 400
            if ok:
                body = r.json()
                if body.get("status") == "scored":
                    score = float(body.get("score", 0.0))
                    threshold = float(body.get("threshold", fixed_threshold))
                    adaptive_alert = int(score >= threshold)
                    fixed_alert = int(score >= fixed_threshold)
        except Exception:
            ok = False

        t1 = time.perf_counter()
        records.append(
            RequestRecord(
                latency_ms=(t1 - t0) * 1000.0,
                endpoint=endpoint,
                ok=ok,
                adaptive_alert=adaptive_alert,
                fixed_alert=fixed_alert,
                true_drift=true_drift,
                phase=phase,
            )
        )

        if pace_sleep_s > 0:
            await asyncio.sleep(pace_sleep_s)


async def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    base_url = args.base_url.rstrip("/")
    duration_s = int(args.duration_seconds)
    warmup_s = int(args.warmup_seconds)
    fixed_threshold = float(args.fixed_threshold)

    scenario = LoadScenario(
        entity_cardinality=int(args.entity_cardinality),
        fixed_threshold=fixed_threshold,
        seed=int(args.seed),
    )
    records: list[RequestRecord] = []
    samples: list[SamplePoint] = []

    endpoint_weights = [
        ("/score", float(args.weight_score)),
        ("/predict", float(args.weight_predict)),
        ("/detect_drift", float(args.weight_detect_drift)),
    ]
    total_w = sum(w for _, w in endpoint_weights)
    if total_w <= 0:
        raise ValueError("Endpoint weights must sum to > 0")
    endpoint_weights = [(e, w / total_w) for e, w in endpoint_weights]

    limits = httpx.Limits(max_connections=int(args.max_connections), max_keepalive_connections=100)
    async with httpx.AsyncClient(limits=limits) as client:
        health = await client.get(f"{base_url}/health", timeout=5.0)
        health.raise_for_status()

        warmup_end = time.time() + warmup_s
        while time.time() < warmup_end:
            payload, _, _ = scenario.make_event(t_norm=0.0)
            with contextlib.suppress(Exception):
                await client.post(f"{base_url}/score", json=payload, timeout=10.0)

        bench_start = time.time()
        bench_end = bench_start + duration_s
        total_duration = max(bench_end - bench_start, 1.0)

        async def sampler() -> None:
            while time.time() < bench_end:
                sp = await _sample_resources(client, base_url)
                if sp is not None:
                    samples.append(sp)
                await asyncio.sleep(float(args.resource_sample_seconds))

        async def worker(worker_idx: int) -> None:
            rng = random.Random(2000 + worker_idx)
            endpoints = [e for e, _ in endpoint_weights]
            weights = [w for _, w in endpoint_weights]
            while time.time() < bench_end:
                now = time.time()
                t_norm = max(0.0, min(1.0, (now - bench_start) / total_duration))
                payload, true_drift, phase = scenario.make_event(t_norm=t_norm)
                endpoint = rng.choices(endpoints, weights=weights, k=1)[0]
                t0 = time.perf_counter()
                ok = False
                adaptive_alert = 0
                fixed_alert = 0

                try:
                    r = await client.post(f"{base_url}{endpoint}", json=payload, timeout=10.0)
                    ok = r.status_code < 400
                    if ok:
                        body = r.json()
                        if body.get("status") == "scored":
                            score = float(body.get("score", 0.0))
                            threshold = float(body.get("threshold", fixed_threshold))
                            adaptive_alert = int(score >= threshold)
                            fixed_alert = int(score >= fixed_threshold)
                except Exception:
                    ok = False

                t1 = time.perf_counter()
                records.append(
                    RequestRecord(
                        latency_ms=(t1 - t0) * 1000.0,
                        endpoint=endpoint,
                        ok=ok,
                        adaptive_alert=adaptive_alert,
                        fixed_alert=fixed_alert,
                        true_drift=true_drift,
                        phase=phase,
                    )
                )

                if args.pace_sleep_ms > 0:
                    await asyncio.sleep(args.pace_sleep_ms / 1000.0)

        await asyncio.gather(sampler(), *(worker(i) for i in range(int(args.concurrency))))

    elapsed = max(duration_s, 1)
    total = len(records)
    ok_records = [r for r in records if r.ok]
    lats = sorted([r.latency_ms for r in ok_records])
    errors = total - len(ok_records)

    y_true = [r.true_drift for r in ok_records]
    y_adaptive = [r.adaptive_alert for r in ok_records]
    y_fixed = [r.fixed_alert for r in ok_records]
    adaptive_metrics = _precision_recall_f1(y_true, y_adaptive)
    fixed_metrics = _precision_recall_f1(y_true, y_fixed)
    false_alert_reduction_pct = (
        ((fixed_metrics["fp"] - adaptive_metrics["fp"]) / fixed_metrics["fp"]) * 100.0
        if fixed_metrics["fp"]
        else 0.0
    )

    endpoint_counts: dict[str, int] = {}
    phase_counts: dict[str, int] = {}
    for r in records:
        endpoint_counts[r.endpoint] = endpoint_counts.get(r.endpoint, 0) + 1
        phase_counts[r.phase] = phase_counts.get(r.phase, 0) + 1

    cpu_avg_pct = 0.0
    cpu_p95_pct = 0.0
    mem_avg_mb = 0.0
    mem_p95_mb = 0.0
    if len(samples) >= 2:
        cpu_pcts: list[float] = []
        mem_mbs = sorted([s.process_resident_memory_bytes / (1024 * 1024) for s in samples])
        for prev, cur in zip(samples[:-1], samples[1:], strict=False):
            dt = cur.ts - prev.ts
            d_cpu = cur.process_cpu_seconds_total - prev.process_cpu_seconds_total
            if dt > 0 and d_cpu >= 0:
                cpu_pcts.append((d_cpu / dt) * 100.0)
        if cpu_pcts:
            cpu_pcts_sorted = sorted(cpu_pcts)
            cpu_avg_pct = float(statistics.mean(cpu_pcts_sorted))
            cpu_p95_pct = _percentile(cpu_pcts_sorted, 0.95)
        if mem_mbs:
            mem_avg_mb = float(statistics.mean(mem_mbs))
            mem_p95_mb = _percentile(mem_mbs, 0.95)

    return {
        "benchmark": {
            "base_url": base_url,
            "duration_seconds": duration_s,
            "warmup_seconds": warmup_s,
            "concurrency": int(args.concurrency),
            "pace_sleep_ms": int(args.pace_sleep_ms),
            "requests_total": total,
            "requests_ok": len(ok_records),
            "requests_error": errors,
            "error_rate_percent": round((errors / total) * 100.0, 4) if total else 0.0,
            "throughput_rps": round(total / elapsed, 2),
            "throughput_events_per_minute": round((total / elapsed) * 60.0, 2),
        },
        "latency_ms": {
            "p50": round(_percentile(lats, 0.50), 3),
            "p95": round(_percentile(lats, 0.95), 3),
            "p99": round(_percentile(lats, 0.99), 3),
            "avg": round(float(statistics.mean(lats)) if lats else 0.0, 3),
        },
        "drift_quality": {
            "adaptive": adaptive_metrics,
            "fixed_baseline": fixed_metrics,
            "false_alert_reduction_percent_vs_fixed": round(false_alert_reduction_pct, 3),
        },
        "system_reliability": {
            "uptime_percent": round(
                (len(ok_records) / total) * 100.0 if total else 0.0,
                4,
            ),
            "error_rate_percent": round((errors / total) * 100.0, 4) if total else 0.0,
        },
        "resources": {
            "sample_count": len(samples),
            "process_cpu_percent_avg": round(cpu_avg_pct, 3),
            "process_cpu_percent_p95": round(cpu_p95_pct, 3),
            "process_memory_mb_avg": round(mem_avg_mb, 3),
            "process_memory_mb_p95": round(mem_p95_mb, 3),
        },
        "traffic_mix": {
            "endpoints": endpoint_counts,
            "phases": phase_counts,
            "phase_definition": {
                "normal": "baseline traffic, no drift",
                "gradual_drift": "slow distribution shift",
                "sudden_drift": "abrupt distribution shift",
                "noisy_non_drift": "higher variance without true drift label",
            },
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sustained end-to-end benchmark for realtime-ml-drift API.")
    parser.add_argument("--base_url", default="http://127.0.0.1:8000")
    parser.add_argument("--duration_seconds", type=int, default=300)
    parser.add_argument("--warmup_seconds", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--pace_sleep_ms", type=int, default=0)
    parser.add_argument("--max_connections", type=int, default=200)
    parser.add_argument("--entity_cardinality", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--fixed_threshold", type=float, default=0.88)
    parser.add_argument("--weight_score", type=float, default=0.5)
    parser.add_argument("--weight_predict", type=float, default=0.3)
    parser.add_argument("--weight_detect_drift", type=float, default=0.2)
    parser.add_argument("--resource_sample_seconds", type=float, default=2.0)
    parser.add_argument("--out_json", type=Path, default=Path("benchmark_e2e_metrics.json"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = asyncio.run(run_benchmark(args))

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))
    print(f"\nSaved benchmark metrics to: {args.out_json}")


if __name__ == "__main__":
    main()
