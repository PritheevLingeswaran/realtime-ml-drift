from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from pathlib import Path
from typing import Any


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


def precision_recall_f1(y_true: list[int], y_pred: list[int]) -> dict[str, float | int]:
    tp = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 1 and p == 1)
    fp = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 0 and p == 1)
    fn = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 1 and p == 0)
    tn = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 0 and p == 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2.0 * precision * recall / (precision + recall) if precision + recall else 0.0
    false_alert_rate = fp / (fp + tn) if fp + tn else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_alert_rate": false_alert_rate,
    }


def detection_latency_seconds(rows: list[dict[str, Any]], pred_col: str) -> dict[str, Any]:
    segments = 0
    detected = 0
    latencies: list[float] = []

    i = 0
    while i < len(rows):
        if int(rows[i]["ground_truth_drift"]) == 1:
            segments += 1
            start_ts = float(rows[i]["timestamp"])
            detected_ts: float | None = None
            while i < len(rows) and int(rows[i]["ground_truth_drift"]) == 1:
                if int(rows[i][pred_col]) == 1 and detected_ts is None:
                    detected_ts = float(rows[i]["timestamp"])
                i += 1
            if detected_ts is not None:
                detected += 1
                latencies.append(max(0.0, detected_ts - start_ts))
        else:
            i += 1

    return {
        "segments_total": segments,
        "segments_detected": detected,
        "avg_detection_latency_seconds": statistics.mean(latencies) if latencies else 0.0,
        "p95_detection_latency_seconds": percentile(latencies, 0.95),
    }


def load_request_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    rows.sort(key=lambda x: float(x["timestamp"]))
    return rows


def load_resource_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with open(path, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    rows.sort(key=lambda x: float(x["timestamp"]))
    return rows


def compute_cpu_mem(resources: list[dict[str, Any]]) -> dict[str, float]:
    if len(resources) < 2:
        cpu_samples = [float(r.get("process_cpu_percent", 0.0)) for r in resources]
        mem_samples = [
            float(r.get("process_resident_memory_bytes", 0.0)) / (1024 * 1024) for r in resources
        ]
        return {
            "cpu_avg_percent": statistics.mean(cpu_samples) if cpu_samples else 0.0,
            "cpu_p95_percent": percentile(cpu_samples, 0.95),
            "memory_avg_mb": statistics.mean(mem_samples) if mem_samples else 0.0,
            "memory_p95_mb": percentile(mem_samples, 0.95),
        }

    cpu_samples: list[float] = []
    mem_samples: list[float] = []
    for row in resources:
        mem_samples.append(float(row["process_resident_memory_bytes"]) / (1024 * 1024))
        cpu_direct = float(row.get("process_cpu_percent", 0.0))
        if cpu_direct > 0:
            cpu_samples.append(cpu_direct)

    for prev, cur in zip(resources[:-1], resources[1:], strict=False):
        dt = float(cur["timestamp"]) - float(prev["timestamp"])
        prev_cpu = float(prev["process_cpu_seconds_total"])
        cur_cpu = float(cur["process_cpu_seconds_total"])
        dcpu = cur_cpu - prev_cpu
        if dt > 0 and dcpu >= 0 and (prev_cpu > 0.0 or cur_cpu > 0.0):
            cpu_samples.append((dcpu / dt) * 100.0)

    return {
        "cpu_avg_percent": statistics.mean(cpu_samples) if cpu_samples else 0.0,
        "cpu_p95_percent": percentile(cpu_samples, 0.95),
        "memory_avg_mb": statistics.mean(mem_samples) if mem_samples else 0.0,
        "memory_p95_mb": percentile(mem_samples, 0.95),
    }


def evaluate_run(request_csv: Path, resource_csv: Path) -> dict[str, Any]:
    rows = load_request_rows(request_csv)
    if not rows:
        raise ValueError(f"No request rows in {request_csv}")

    ok_rows = [r for r in rows if 200 <= int(r["status_code"]) < 400]
    duration = max(1e-9, float(rows[-1]["timestamp"]) - float(rows[0]["timestamp"]))

    latencies = [float(r["latency_ms"]) for r in ok_rows]
    drift_rows = [r for r in ok_rows if int(r.get("drift_evaluated", "1")) == 1]
    if not drift_rows:
        drift_rows = ok_rows

    # Split for calibration on validation traffic (time-ordered).
    split_idx = max(1, int(len(drift_rows) * 0.3))
    valid_rows = drift_rows[:split_idx]
    test_rows = drift_rows[split_idx:] if split_idx < len(drift_rows) else drift_rows

    def pred_with_multiplier(rows: list[dict[str, Any]], mult: float) -> list[int]:
        out: list[int] = []
        for r in rows:
            score = float(r.get("score", 0.0))
            thr = float(r.get("threshold", 0.0))
            out.append(int(score >= (thr * mult)))
        return out

    def pred_warning(rows: list[dict[str, Any]]) -> list[int]:
        return [int(r.get("warning_alert", "0")) for r in rows]

    def pred_critical(rows: list[dict[str, Any]]) -> list[int]:
        return [int(r.get("critical_alert", r.get("adaptive_alert", "0"))) for r in rows]

    # Calibrate on validation for better recall/false-alert tradeoff.
    target_far = 0.20
    target_precision = 0.75
    target_recall = 0.80
    best_mode = "critical"
    best_mult = 1.0
    best_tuple = (-1.0, -1.0, -1.0, -1.0)
    y_valid = [int(r["ground_truth_drift"]) for r in valid_rows]
    for mode, mult in (
        [("critical", 1.0), ("warning", 1.0)]
        + [("score_mult", m) for m in [0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]]
    ):
        if mode == "critical":
            p = pred_critical(valid_rows)
        elif mode == "warning":
            p = pred_warning(valid_rows)
        else:
            p = pred_with_multiplier(valid_rows, mult)
        m = precision_recall_f1(y_valid, p)
        far = float(m["false_alert_rate"])
        precision = float(m["precision"])
        recall = float(m["recall"])
        meets = float(
            precision >= target_precision and recall >= target_recall and far <= target_far
        )
        score_tuple = (
            meets,
            -far,
            1.0 if far <= target_far else 0.0,
            float(m["f1"]),
            precision + recall,
        )
        if score_tuple > best_tuple:
            best_tuple = score_tuple
            best_mode = mode
            best_mult = mult

    y_true = [int(r["ground_truth_drift"]) for r in test_rows]
    if best_mode == "critical":
        y_adaptive = pred_critical(test_rows)
    elif best_mode == "warning":
        y_adaptive = pred_warning(test_rows)
    else:
        y_adaptive = pred_with_multiplier(test_rows, best_mult)
    y_fixed = [int(r["fixed_alert"]) for r in test_rows]

    adaptive = precision_recall_f1(y_true, y_adaptive)
    baseline = precision_recall_f1(y_true, y_fixed)

    fp_baseline = int(baseline["fp"])
    fp_adaptive = int(adaptive["fp"])
    false_alert_reduction_percent = (
        ((fp_baseline - fp_adaptive) / fp_baseline) * 100.0 if fp_baseline > 0 else 0.0
    )

    adaptive_latency = detection_latency_seconds(
        [{**r, "adaptive_alert": str(v)} for r, v in zip(test_rows, y_adaptive, strict=False)],
        pred_col="adaptive_alert",
    )
    baseline_latency = detection_latency_seconds(test_rows, pred_col="fixed_alert")

    resources = compute_cpu_mem(load_resource_rows(resource_csv))

    summary = {
        "sustained_rps": len(rows) / duration,
        "latency_p50_ms": percentile(latencies, 0.50),
        "latency_p95_ms": percentile(latencies, 0.95),
        "latency_p99_ms": percentile(latencies, 0.99),
        "error_rate_percent": ((len(rows) - len(ok_rows)) / len(rows) * 100.0),
        "cpu_avg_percent": resources["cpu_avg_percent"],
        "cpu_p95_percent": resources["cpu_p95_percent"],
        "memory_avg_mb": resources["memory_avg_mb"],
        "memory_p95_mb": resources["memory_p95_mb"],
        "alert_precision": adaptive["precision"],
        "alert_recall": adaptive["recall"],
        "alert_f1": adaptive["f1"],
        "avg_detection_latency_seconds": adaptive_latency["avg_detection_latency_seconds"],
        "p95_detection_latency_seconds": adaptive_latency["p95_detection_latency_seconds"],
        "false_alert_rate": adaptive["false_alert_rate"],
        "false_alert_reduction_percent": false_alert_reduction_percent,
        "baseline_vs_adaptive": {
            "fixed_baseline": {**baseline, **baseline_latency},
            "adaptive": {**adaptive, **adaptive_latency},
        },
        "calibration": {
            "validation_points": len(valid_rows),
            "test_points": len(test_rows),
            "target_false_alert_rate": target_far,
            "target_precision": target_precision,
            "target_recall": target_recall,
            "selected_mode": best_mode,
            "selected_threshold_multiplier": best_mult,
        },
        "system_reliability": {
            "uptime_percent": (len(ok_rows) / len(rows) * 100.0),
            "error_rate_percent": ((len(rows) - len(ok_rows)) / len(rows) * 100.0),
        },
        "drift_eval_points": len(test_rows),
        "inputs": {
            "request_csv": str(request_csv),
            "resource_csv": str(resource_csv),
        },
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    return summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate a live benchmark run into production KPIs.")
    p.add_argument("--request_csv", type=Path, required=True)
    p.add_argument("--resource_csv", type=Path, required=True)
    p.add_argument("--out_json", type=Path, default=Path("artifacts/live_run_summary.json"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    summary = evaluate_run(args.request_csv, args.resource_csv)

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2))
    print(f"\nSaved live run summary to: {args.out_json}")


if __name__ == "__main__":
    main()
