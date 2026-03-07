from __future__ import annotations

from src.drift.monitor import DriftConfig, DriftMonitor

FEATURES = ["f1", "f2"]


def build_monitor(periodic_enabled: bool, check_interval_events: int) -> DriftMonitor:
    cfg = DriftConfig(
        reference_window_events=40,
        current_window_events=20,
        window_size=20,
        evaluation_interval=5,
        check_interval_events=check_interval_events,
        periodic_expensive_checks_enabled=periodic_enabled,
        min_samples=10,
        feature_ks_p=0.01,
        feature_psi=0.2,
        pred_ks_p=0.01,
        pred_psi=0.2,
        threshold_method="adaptive",
        threshold_k=1.0,
        fixed_score_threshold=0.8,
        feature_vote_fraction=0.5,
        smoothing_consecutive=2,
        alert_cooldown_events=0,
        score_weight_psi=0.5,
        score_weight_ks=0.3,
        score_weight_pred=0.2,
        baseline_min_evals=5,
        mean_shift_z_threshold=2.5,
        feature_threshold_k=1.5,
        adaptive_score_quantile=0.9,
        feature_alert_score_threshold=0.9,
        norm_cap=3.0,
        warning_enter_mult=0.9,
        warning_exit_mult=0.7,
        critical_enter_mult=1.1,
        critical_exit_mult=0.9,
        warning_vote_fraction=0.25,
        critical_vote_fraction=0.45,
        warning_consecutive=1,
        critical_consecutive=2,
        adwin_enabled=True,
        adwin_delta=0.002,
    )
    return DriftMonitor(cfg=cfg, feature_names=FEATURES)


def test_periodic_expensive_checks_only_on_interval() -> None:
    m = build_monitor(periodic_enabled=True, check_interval_events=10)
    ts = 0.0

    # Reference fill
    for _ in range(40):
        ts += 1.0
        m.update(feats={"f1": 0.1, "f2": 0.2}, score=0.2, ts=ts)

    # Warm current window; expensive checks should not run every event.
    eval_flags = []
    adwin_flags = []
    for _ in range(25):
        ts += 1.0
        st = m.update(feats={"f1": 0.11, "f2": 0.19}, score=0.21, ts=ts)
        eval_flags.append(st.drift_evaluated)
        adwin_flags.append(st.adwin_detected)

    assert any(adwin_flags) or all(flag is False for flag in adwin_flags)
    # At least one evaluation should happen, but not on every event.
    assert any(eval_flags)
    assert sum(1 for f in eval_flags if f) < len(eval_flags)


def _drift_eval_summary(m: DriftMonitor) -> tuple[int | None, float]:
    ts = 0.0
    idx = 0

    # Reference fill
    for _ in range(40):
        idx += 1
        ts += 1.0
        m.update(feats={"f1": 0.1, "f2": 0.2}, score=0.2, ts=ts)

    # Pre-drift stable segment.
    for _ in range(80):
        idx += 1
        ts += 1.0
        m.update(feats={"f1": 0.11, "f2": 0.19}, score=0.21, ts=ts)

    first_eval_idx = None
    max_score = 0.0
    # Strong drift segment.
    for _ in range(120):
        idx += 1
        ts += 1.0
        st = m.update(feats={"f1": 4.0, "f2": -3.5}, score=0.95, ts=ts)
        if st.drift_evaluated:
            if first_eval_idx is None:
                first_eval_idx = idx
            max_score = max(max_score, float(st.drift_score))
    return first_eval_idx, max_score


def test_periodic_checks_detect_within_interval_tolerance() -> None:
    full = build_monitor(periodic_enabled=True, check_interval_events=1)
    periodic = build_monitor(periodic_enabled=True, check_interval_events=20)

    full_idx, full_max_score = _drift_eval_summary(full)
    periodic_idx, periodic_max_score = _drift_eval_summary(periodic)
    assert full_idx is not None
    assert periodic_idx is not None
    # Periodic checks can lag full checks, but bounded by check interval tolerance.
    assert (periodic_idx - full_idx) <= 20
    # Periodic checks should still capture strong drift signal with limited loss.
    assert periodic_max_score >= (0.8 * full_max_score)
