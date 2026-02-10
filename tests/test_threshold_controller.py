from __future__ import annotations

from src.drift.adaptation import AdaptationConfig, ThresholdController


def test_guardrails_max_step_and_bounds() -> None:
    cfg = AdaptationConfig(
        enabled=True,
        target_anomaly_rate=0.1,
        initial_threshold=0.9,
        min_threshold=0.6,
        max_threshold=0.99,
        max_step=0.02,
        cooldown_seconds=0,
    )
    tc = ThresholdController(cfg, window_size=1000)
    ts = 1000.0

    # Feed scores that would propose a much lower threshold
    for _i in range(500):
        ts += 1.0
        tc.update(score=0.2, ts=ts, drift_active=False)
    # Max step should limit change
    assert tc.threshold >= 0.9 - 0.02

    # Force proposed above max threshold
    for _i in range(500):
        ts += 1.0
        tc.update(score=1.0, ts=ts, drift_active=False)
    assert tc.threshold <= cfg.max_threshold
