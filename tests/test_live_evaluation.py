from __future__ import annotations

import csv
from pathlib import Path

from scripts.evaluate_live_run import detection_latency_seconds, evaluate_run, percentile


def test_percentile_basic() -> None:
    vals = [1.0, 2.0, 3.0, 4.0]
    assert percentile(vals, 0.5) == 2.5
    assert percentile(vals, 0.95) > 3.0


def test_detection_latency_computation() -> None:
    rows = [
        {"timestamp": "0.0", "ground_truth_drift": "0", "adaptive_alert": "0"},
        {"timestamp": "1.0", "ground_truth_drift": "1", "adaptive_alert": "0"},
        {"timestamp": "2.5", "ground_truth_drift": "1", "adaptive_alert": "1"},
        {"timestamp": "3.0", "ground_truth_drift": "0", "adaptive_alert": "0"},
    ]
    out = detection_latency_seconds(rows, pred_col="adaptive_alert")
    assert out["segments_total"] == 1
    assert out["segments_detected"] == 1
    assert out["avg_detection_latency_seconds"] == 1.5


def test_evaluate_run_metrics(tmp_path: Path) -> None:
    req = tmp_path / "requests.csv"
    res = tmp_path / "system.csv"

    with open(req, "w", encoding="utf-8", newline="") as f:
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
                "adaptive_alert",
                "fixed_alert",
                "score",
                "threshold",
                "error",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "timestamp": "1.0",
                "request_id": "a",
                "endpoint": "/score",
                "status_code": "200",
                "latency_ms": "10",
                "phase": "normal",
                "ground_truth_drift": "0",
                "adaptive_alert": "0",
                "fixed_alert": "1",
                "score": "0.2",
                "threshold": "0.8",
                "error": "",
            }
        )
        w.writerow(
            {
                "timestamp": "2.0",
                "request_id": "b",
                "endpoint": "/score",
                "status_code": "200",
                "latency_ms": "20",
                "phase": "drift",
                "ground_truth_drift": "1",
                "adaptive_alert": "1",
                "fixed_alert": "1",
                "score": "0.95",
                "threshold": "0.8",
                "error": "",
            }
        )

    with open(res, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["timestamp", "process_cpu_seconds_total", "process_resident_memory_bytes"],
        )
        w.writeheader()
        w.writerow(
            {
                "timestamp": "1.0",
                "process_cpu_seconds_total": "10.0",
                "process_resident_memory_bytes": str(100 * 1024 * 1024),
            }
        )
        w.writerow(
            {
                "timestamp": "2.0",
                "process_cpu_seconds_total": "10.5",
                "process_resident_memory_bytes": str(120 * 1024 * 1024),
            }
        )

    out = evaluate_run(req, res)
    assert out["sustained_rps"] > 0
    assert out["alert_precision"] == 1.0
    assert out["alert_recall"] == 1.0
    assert out["false_alert_reduction_percent"] >= 0.0
