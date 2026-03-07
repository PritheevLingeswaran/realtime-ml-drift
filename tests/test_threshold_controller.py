from __future__ import annotations

from src.drift.adaptation import AdaptationConfig, ThresholdController


def test_anomaly_rate_converges_toward_target_on_stationary_scores() -> None:
    cfg = AdaptationConfig(
        enabled=True,
        target_anomaly_rate=0.1,
        initial_threshold=0.5,
        min_threshold=0.0,
        max_threshold=1.0,
        max_step=1.0,
        cooldown_seconds=0,
        min_history=200,
    )
    tc = ThresholdController(cfg, window_size=4000)

    # Deterministic score stream in [0, 1): true 90th percentile is ~0.9.
    ts = 0.0
    for i in range(1, 4001):
        ts += 1.0
        score = (i % 1000) / 1000.0
        tc.update(score=score, ts=ts, drift_active=False)

    assert 0.88 <= tc.threshold <= 0.92
    assert abs(tc.anomaly_rate() - cfg.target_anomaly_rate) <= 0.02


def test_cooldown_and_max_step_behavior() -> None:
    cfg = AdaptationConfig(
        enabled=True,
        target_anomaly_rate=0.2,
        initial_threshold=0.5,
        min_threshold=0.0,
        max_threshold=1.0,
        max_step=0.05,
        cooldown_seconds=10,
        min_history=11,
    )
    tc = ThresholdController(cfg, window_size=128)

    # Warm history with high scores so quantile proposes a high threshold.
    ts = 0.0
    for _ in range(10):
        ts += 1.0
        tc.update(score=0.95, ts=ts, drift_active=False)

    first = tc.threshold
    # First update after history should move by max_step only.
    ts += 1.0
    tc.update(score=0.95, ts=ts, drift_active=False)
    assert abs(tc.threshold - first) <= cfg.max_step + 1e-12
    moved = tc.threshold

    # Inside cooldown -> no movement.
    ts += 1.0
    tc.update(score=0.10, ts=ts, drift_active=False)
    assert tc.threshold == moved

    # After cooldown expires -> may move again.
    ts += 11.0
    tc.update(score=0.10, ts=ts, drift_active=False)
    assert tc.threshold != moved


def test_no_adaptation_during_drift() -> None:
    cfg = AdaptationConfig(
        enabled=True,
        target_anomaly_rate=0.05,
        initial_threshold=0.7,
        min_threshold=0.0,
        max_threshold=1.0,
        max_step=0.1,
        cooldown_seconds=0,
        min_history=20,
    )
    tc = ThresholdController(cfg, window_size=256)
    ts = 0.0
    for _ in range(100):
        ts += 1.0
        tc.update(score=0.99, ts=ts, drift_active=True)
    assert tc.threshold == cfg.initial_threshold
