from __future__ import annotations

import argparse
import csv
import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import yaml


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if int(row.get("drift_evaluated", "0")) == 1:
                rows.append(row)
    rows.sort(key=lambda x: float(x["timestamp"]))
    return rows


def confusion(y_true: list[int], y_pred: list[int]) -> dict[str, float | int]:
    tp = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 1 and p == 1)
    fp = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 0 and p == 1)
    tn = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 0 and p == 0)
    fn = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 1 and p == 0)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2.0 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    false_alert_rate = fp / (tp + fp) if (tp + fp) else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_alert_rate": false_alert_rate,
    }


@dataclass
class SweepParams:
    warning_enter_mult: float
    warning_exit_mult: float
    critical_enter_mult: float
    critical_exit_mult: float
    warning_vote_fraction: float
    critical_vote_fraction: float
    warning_consecutive: int
    critical_consecutive: int
    cooldown_windows: int


def simulate(rows: list[dict[str, Any]], p: SweepParams) -> list[int]:
    warning_state = False
    critical_state = False
    warning_streak = 0
    critical_streak = 0
    last_critical_idx = -10**9
    preds: list[int] = []

    for i, r in enumerate(rows):
        score = float(r.get("score", 0.0))
        thr = float(r.get("threshold", 1.0))
        vote = float(r.get("vote_ratio", 0.0))

        w_enter = thr * p.warning_enter_mult
        w_exit = thr * p.warning_exit_mult
        c_enter = thr * p.critical_enter_mult
        c_exit = thr * p.critical_exit_mult

        if warning_state:
            w_raw = score >= w_exit and vote >= max(0.0, p.warning_vote_fraction * 0.8)
        else:
            w_raw = score >= w_enter and vote >= p.warning_vote_fraction

        if w_raw:
            warning_streak += 1
        else:
            warning_streak = 0

        warning_state = warning_streak >= max(1, p.warning_consecutive)

        if critical_state:
            c_raw = score >= c_exit and vote >= max(0.0, p.critical_vote_fraction * 0.8)
        else:
            c_raw = warning_state and score >= c_enter and vote >= p.critical_vote_fraction

        if c_raw:
            critical_streak += 1
        else:
            critical_streak = 0

        ready = critical_streak >= max(1, p.critical_consecutive)
        cooldown = (i - last_critical_idx) < p.cooldown_windows
        if critical_state and c_raw:
            critical_state = True
        elif ready and not cooldown:
            critical_state = True
            last_critical_idx = i
        else:
            critical_state = False

        preds.append(1 if critical_state else 0)

    return preds


def objective(m: dict[str, float | int], far_target: float = 0.20) -> float:
    f1 = float(m["f1"])
    far = float(m["false_alert_rate"])
    precision = float(m["precision"])
    recall = float(m["recall"])
    penalty_far = max(0.0, far - far_target) * 3.0
    penalty_prec = max(0.0, 0.75 - precision) * 2.0
    penalty_rec = max(0.0, 0.80 - recall) * 2.0
    return f1 - penalty_far - penalty_prec - penalty_rec


def _parse_float_list(raw: str) -> list[float]:
    return [float(x.strip()) for x in raw.split(",") if x.strip()]


def _parse_int_list(raw: str) -> list[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def _load_drift_defaults(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return dict(cfg.get("drift", {}))


def sweep(
    rows: list[dict[str, Any]],
    warning_enter_mult: float,
    warning_exit_mult: float,
    critical_exit_mult: float,
    warning_vote_fraction: float,
    warning_consecutive: int,
    cooldown_windows: int,
    critical_enter_values: list[float],
    critical_vote_values: list[float],
    critical_consecutive_values: list[int],
) -> dict[str, Any]:
    if len(rows) < 20:
        raise ValueError("Need at least 20 drift-evaluated rows for sweep")

    # Phase-stratified split to avoid holdout containing only one class.
    by_phase: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        ph = str(r.get("phase", "unknown"))
        by_phase.setdefault(ph, []).append(r)

    cv_rows: list[dict[str, Any]] = []
    holdout: list[dict[str, Any]] = []
    for phase_rows in by_phase.values():
        split = max(1, int(len(phase_rows) * 0.8))
        cv_rows.extend(phase_rows[:split])
        holdout.extend(phase_rows[split:])
    cv_rows.sort(key=lambda x: float(x["timestamp"]))
    holdout.sort(key=lambda x: float(x["timestamp"]))
    k_folds = min(4, max(2, len(cv_rows) // 20))

    grid = itertools.product(
        [warning_enter_mult],
        [warning_exit_mult],
        critical_enter_values,
        [critical_exit_mult],
        [warning_vote_fraction],
        critical_vote_values,
        [warning_consecutive],
        critical_consecutive_values,
        [cooldown_windows],
    )

    best = None
    rows_out: list[dict[str, Any]] = []

    for vals in grid:
        p = SweepParams(*vals)
        fold_metrics = []
        fold_scores = []
        for fold in range(k_folds):
            val = [r for idx, r in enumerate(cv_rows) if idx % k_folds == fold]
            y_val = [int(r["ground_truth_drift"]) for r in val]
            pred = simulate(val, p)
            m = confusion(y_val, pred)
            fold_metrics.append(m)
            fold_scores.append(objective(m))

        mean_obj = sum(fold_scores) / max(1, len(fold_scores))
        std_obj = (
            (sum((x - mean_obj) ** 2 for x in fold_scores) / max(1, len(fold_scores))) ** 0.5
        )
        score = mean_obj - (0.2 * std_obj)

        mean_precision = sum(float(m["precision"]) for m in fold_metrics) / len(fold_metrics)
        mean_recall = sum(float(m["recall"]) for m in fold_metrics) / len(fold_metrics)
        mean_far = sum(float(m["false_alert_rate"]) for m in fold_metrics) / len(fold_metrics)
        mean_f1 = sum(float(m["f1"]) for m in fold_metrics) / len(fold_metrics)

        rec = {
            "params": p.__dict__,
            "cv_mean": {
                "precision": mean_precision,
                "recall": mean_recall,
                "f1": mean_f1,
                "false_alert_rate": mean_far,
            },
            "cv_scores": fold_scores,
            "objective": score,
        }
        rows_out.append(rec)

        if best is None or score > best["objective"]:
            best = rec

    assert best is not None

    y_test = [int(r["ground_truth_drift"]) for r in holdout]
    best_params = SweepParams(**best["params"])
    test_pred = simulate(holdout, best_params)
    test_metrics = confusion(y_test, test_pred)

    return {
        "cv_points": len(cv_rows),
        "holdout_points": len(holdout),
        "k_folds": k_folds,
        "sweep_space": {
            "critical_enter_values": critical_enter_values,
            "critical_vote_values": critical_vote_values,
            "critical_consecutive_values": critical_consecutive_values,
            "fixed_warning_enter_mult": warning_enter_mult,
            "fixed_warning_exit_mult": warning_exit_mult,
            "fixed_critical_exit_mult": critical_exit_mult,
            "fixed_warning_vote_fraction": warning_vote_fraction,
            "fixed_warning_consecutive": warning_consecutive,
            "fixed_cooldown_windows": cooldown_windows,
        },
        "best": {
            "params": best_params.__dict__,
            "cv_mean": best["cv_mean"],
            "holdout_metrics": test_metrics,
            "objective": best["objective"],
        },
        "recommended_config": {"drift": best_params.__dict__},
        "top5_cv": sorted(rows_out, key=lambda r: r["objective"], reverse=True)[:5],
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sweep drift thresholds on validation traffic.")
    p.add_argument("--request_csv", type=Path, required=True)
    p.add_argument("--base_config", type=Path, default=Path("configs/base.yaml"))
    p.add_argument("--critical_enter_values", default="0.8,1.0,1.2")
    p.add_argument("--critical_vote_values", default="0.35,0.40,0.45,0.50,0.55,0.60")
    p.add_argument("--critical_consecutive_values", default="1,2,3")
    p.add_argument("--out_json", type=Path, default=Path("artifacts/threshold_sweep.json"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_rows(args.request_csv)
    drift_cfg = _load_drift_defaults(args.base_config)
    out = sweep(
        rows=rows,
        warning_enter_mult=float(drift_cfg.get("warning_enter_mult", 0.8)),
        warning_exit_mult=float(drift_cfg.get("warning_exit_mult", 0.6)),
        critical_exit_mult=float(drift_cfg.get("critical_exit_mult", 0.8)),
        warning_vote_fraction=float(drift_cfg.get("warning_vote_fraction", 0.2)),
        warning_consecutive=int(drift_cfg.get("warning_consecutive", 1)),
        cooldown_windows=int(drift_cfg.get("alert_cooldown_events", 2)),
        critical_enter_values=_parse_float_list(args.critical_enter_values),
        critical_vote_values=_parse_float_list(args.critical_vote_values),
        critical_consecutive_values=_parse_int_list(args.critical_consecutive_values),
    )
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out["best"], indent=2))
    print(f"\nSaved sweep results: {args.out_json}")


if __name__ == "__main__":
    main()
