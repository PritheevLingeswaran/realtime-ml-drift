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


def test_benchmark_style_controller_converges_within_tighter_tolerance() -> None:
    cfg = AdaptationConfig(
        enabled=True,
        target_anomaly_rate=0.01,
        initial_threshold=0.9960,
        min_threshold=0.0,
        max_threshold=1.0,
        max_step=0.0010,
        cooldown_seconds=1,
        min_history=100,
        rate_feedback_enabled=True,
        target_tolerance_abs=0.0025,
    )
    tc = ThresholdController(cfg, window_size=6000)
    ts = 0.0
    for i in range(1, 6001):
        ts += 1.0
        score = 0.90 + ((i % 1000) / 10000.0)
        tc.update(score=score, ts=ts, drift_active=False)

    assert 0.998 <= tc.threshold <= 1.0
    assert abs(tc.anomaly_rate() - cfg.target_anomaly_rate) <= 0.005


def test_feedback_guardrail_blocks_wrong_way_threshold_move() -> None:
    cfg = AdaptationConfig(
        enabled=True,
        target_anomaly_rate=0.01,
        initial_threshold=0.9964,
        min_threshold=0.9955,
        max_threshold=0.9995,
        max_step=0.0020,
        cooldown_seconds=0,
        min_history=5,
        adapt_during_drift=True,
        rate_feedback_enabled=True,
        target_tolerance_abs=0.0025,
    )
    tc = ThresholdController(cfg, window_size=32)
    ts = 0.0
    for score in [0.999, 0.999, 0.998, 0.998, 0.997]:
        ts += 1.0
        tc.update(score=score, ts=ts, drift_active=False)
    prior = tc.threshold
    tc.update(score=0.996, ts=ts + 1.0, drift_active=False)
    assert tc.threshold >= prior


def test_adapt_during_drift_flag_allows_benchmark_mode_updates() -> None:
    cfg = AdaptationConfig(
        enabled=True,
        target_anomaly_rate=0.01,
        initial_threshold=0.9964,
        min_threshold=0.9955,
        max_threshold=0.9995,
        max_step=0.0020,
        cooldown_seconds=0,
        min_history=5,
        adapt_during_drift=True,
    )
    tc = ThresholdController(cfg, window_size=32)
    ts = 0.0
    for score in [0.999, 0.998, 0.997, 0.996, 0.995]:
        ts += 1.0
        tc.update(score=score, ts=ts, drift_active=True)
    assert tc.threshold != cfg.initial_threshold
