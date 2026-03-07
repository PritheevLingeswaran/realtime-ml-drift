from __future__ import annotations

from scripts.soak_test import (
    compute_availability_percent,
    rolling_p95_summary,
    summarize_latency,
)
from src.monitoring.benchmarking import ResourceSampler


def test_availability_math() -> None:
    assert abs(compute_availability_percent(100.0, 5.0) - 95.0) < 1e-9
    assert compute_availability_percent(100.0, 0.0) == 100.0


def test_latency_summary_math() -> None:
    lat = summarize_latency([10.0, 20.0, 30.0, 40.0])
    assert lat["p50_ms"] == 25.0
    assert lat["p95_ms"] > 35.0


def test_rolling_p95_summary() -> None:
    series = [
        {"events_processed": 100.0, "rolling_p95_ms": 1.1},
        {"events_processed": 200.0, "rolling_p95_ms": 1.5},
        {"events_processed": 300.0, "rolling_p95_ms": 1.3},
    ]
    s = rolling_p95_summary(series)
    assert s["max_ms"] == 1.5
    assert s["latest_ms"] == 1.3


def test_resource_sampler_stability_smoke() -> None:
    sampler = ResourceSampler(sample_sec=0.1)
    sampler.start()
    sampler.stop()
    stats = sampler.stats()
    assert "cpu_avg_percent" in stats
    assert "cpu_p95_percent" in stats
    assert "peak_rss_mb" in stats
