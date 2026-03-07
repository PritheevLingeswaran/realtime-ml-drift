from __future__ import annotations

from src.drift.monitor import DriftConfig, DriftMonitor

FEATURES = ["f1", "f2", "f3", "f4"]


def build_monitor() -> DriftMonitor:
    cfg = DriftConfig(
        reference_window_events=100,
        current_window_events=50,
        window_size=50,
        evaluation_interval=5,
        min_samples=20,
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
        adwin_enabled=False,
        adwin_delta=0.002,
    )
    return DriftMonitor(cfg=cfg, feature_names=FEATURES)


def test_feature_voting_and_smoothing() -> None:
    m = build_monitor()
    ts = 0.0

    # Fill stable reference
    for i in range(100):
        ts += 1.0
        base = (i % 10) / 100.0
        m.update(
            feats={"f1": 0.1 + base, "f2": 0.2 + base, "f3": 0.3 + base, "f4": 0.4 + base},
            score=0.2 + base,
            ts=ts,
        )

    # Mild shift only on one feature should fail voting ratio.
    for i in range(60):
        ts += 1.0
        base = (i % 10) / 100.0
        st = m.update(
            feats={"f1": 0.8, "f2": 0.2 + base, "f3": 0.3 + base, "f4": 0.4 + base},
            score=0.2 + base,
            ts=ts,
        )
    assert st.drift_active is False

    # Multi-feature strong shift should trigger, but smoothing needs consecutive evaluations.
    hit = False
    for _ in range(80):
        ts += 1.0
        st = m.update(feats={"f1": 4.0, "f2": 4.0, "f3": 4.0, "f4": 0.1}, score=0.9, ts=ts)
        if st.drift_active:
            hit = True
            break

    assert hit is True
    assert st.vote_ratio >= 0.5
