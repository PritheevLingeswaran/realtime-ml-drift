from __future__ import annotations

import time

from src.monitoring.benchmarking import (
    ResourceSampler,
    check_target_anomaly_tolerance,
    compute_alert_metrics,
    compute_detection_delay,
    compute_fp_reduction_percent,
    safety_check_anomaly_rate,
)
from scripts.benchmark import evaluate_performance_gate


def test_events_to_detect_logic_without_replay_sleep() -> None:
    d = compute_detection_delay(
        drift_start_event_idx=100,
        detect_event_idx=140,
        events_per_sec=200.0,
        drift_start_ts=1000.0,
        detect_ts=1005.0,
        replay_uses_real_sleep=False,
    )
    assert d.events_to_detect == 40
    assert d.seconds_to_detect is not None
    assert abs(d.seconds_to_detect - 0.2) < 1e-9


def test_false_positive_counting_logic() -> None:
    # alerts on non-drift at positions 0 and 3 -> 2 false alerts out of 3 alerts.
    m = compute_alert_metrics(
        alert_flags=[True, False, True, True],
        drift_flags=[False, True, True, False],
    )
    assert m.alerts == 3
    assert m.false_alerts == 2
    assert abs(m.false_alert_rate - (2 / 3)) < 1e-9
    assert abs(m.anomaly_rate - (3 / 4)) < 1e-9
    assert m.anomaly_rate_denominator == 4
    assert m.alerts_during_drift == 1
    assert m.alerts_during_non_drift == 2
    assert abs((m.alert_rate_drift_segment or 0.0) - 0.5) < 1e-9
    assert abs((m.alert_rate_non_drift_segment or 0.0) - 1.0) < 1e-9
    assert m.tp == 1
    assert m.fp == 2
    assert m.tn == 0
    assert m.fn == 1


def test_metrics_marked_na_when_drift_labels_missing() -> None:
    m = compute_alert_metrics(
        alert_flags=[True, False, True],
        drift_flags=None,
    )
    assert m.labels_available is False
    assert m.false_alerts is None
    assert m.false_alert_rate is None
    assert m.alerts_during_drift is None
    assert m.alerts_during_non_drift is None
    assert m.tp is None
    assert m.fp is None
    assert m.tn is None
    assert m.fn is None


def test_baseline_vs_tuned_fp_reduction_math() -> None:
    reduction = compute_fp_reduction_percent(fp_baseline=20, fp_tuned=8)
    assert abs(reduction - 60.0) < 1e-9


def test_resource_sampler_does_not_crash() -> None:
    sampler = ResourceSampler(sample_sec=0.1)
    sampler.start()
    time.sleep(0.25)
    sampler.stop()
    stats = sampler.stats()
    assert "cpu_avg_percent" in stats
    assert "cpu_p95_percent" in stats
    assert "peak_rss_mb" in stats


def test_alert_rate_safety_check_triggers_on_spam() -> None:
    passed, msg = safety_check_anomaly_rate(0.12, hard_limit=0.05)
    assert not passed
    assert "FAIL" in msg


def test_target_anomaly_tolerance_check() -> None:
    passed, _msg = check_target_anomaly_tolerance(
        observed_anomaly_rate=0.013,
        target_anomaly_rate=0.010,
        tolerance_abs=0.005,
    )
    assert passed

    failed, _msg = check_target_anomaly_tolerance(
        observed_anomaly_rate=0.030,
        target_anomaly_rate=0.010,
        tolerance_abs=0.005,
    )
    assert not failed

    waived, msg = check_target_anomaly_tolerance(
        observed_anomaly_rate=0.030,
        target_anomaly_rate=0.010,
        tolerance_abs=0.005,
        guardrail_blocked=True,
    )
    assert waived
    assert "waiver" in msg


def test_benchmark_performance_gate() -> None:
    passed, throughput_ratio, p99_ratio, _msg = evaluate_performance_gate(
        baseline_events_per_sec=1000.0,
        tuned_events_per_sec=960.0,
        baseline_p99_ms=2.0,
        tuned_p99_ms=2.2,
    )
    assert passed
    assert abs(throughput_ratio - 0.96) < 1e-9
    assert abs(p99_ratio - 1.1) < 1e-9

    failed, _throughput_ratio, _p99_ratio, msg = evaluate_performance_gate(
        baseline_events_per_sec=1000.0,
        tuned_events_per_sec=900.0,
        baseline_p99_ms=2.0,
        tuned_p99_ms=2.5,
    )
    assert not failed
    assert "FAIL" in msg
