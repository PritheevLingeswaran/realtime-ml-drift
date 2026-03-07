from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd


def safe_mean(values: list[float]) -> float:
    return float(np.mean(values)) if values else 0.0


def safe_percent(num: float, den: float) -> float:
    return (num / den * 100.0) if den else 0.0


def precision_recall_f1(y_true: list[int], y_pred: list[int]) -> dict[str, float]:
    tp = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 1 and p == 1)
    fp = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 0 and p == 1)
    fn = sum(1 for t, p in zip(y_true, y_pred, strict=False) if t == 1 and p == 0)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(np.array(values, dtype=float), q))


def psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    expected = np.asarray(expected, dtype=float)
    actual = np.asarray(actual, dtype=float)

    expected = expected[np.isfinite(expected)]
    actual = actual[np.isfinite(actual)]

    if len(expected) == 0 or len(actual) == 0:
        return 0.0

    breakpoints = np.percentile(expected, np.linspace(0, 100, bins + 1))
    breakpoints[0] = -np.inf
    breakpoints[-1] = np.inf

    expected_counts, _ = np.histogram(expected, bins=breakpoints)
    actual_counts, _ = np.histogram(actual, bins=breakpoints)

    expected_perc = expected_counts / max(len(expected), 1)
    actual_perc = actual_counts / max(len(actual), 1)

    expected_perc = np.where(expected_perc == 0, 1e-6, expected_perc)
    actual_perc = np.where(actual_perc == 0, 1e-6, actual_perc)

    value = np.sum((actual_perc - expected_perc) * np.log(actual_perc / expected_perc))
    return float(value)


def make_synthetic_stream(
    n_events: int = 120000,
    drift_starts: list[int] | None = None,
    random_seed: int = 42,
) -> pd.DataFrame:
    rng = np.random.default_rng(random_seed)
    if drift_starts is None:
        drift_starts = [30000, 70000, 100000]

    timestamps = pd.date_range("2026-01-01", periods=n_events, freq="s")
    drift_label = np.zeros(n_events, dtype=int)

    f1 = rng.normal(0, 1, n_events)
    f2 = rng.normal(5, 1.5, n_events)
    f3 = rng.normal(-2, 0.8, n_events)

    pred_score = rng.beta(2, 5, n_events)

    segment_len = 8000
    for i, start in enumerate(drift_starts):
        end = min(start + segment_len, n_events)
        drift_label[start:end] = 1

        if i % 3 == 0:
            f1[start:end] += 1.8
            pred_score[start:end] = np.clip(pred_score[start:end] + 0.25, 0, 1)
        elif i % 3 == 1:
            f2[start:end] *= 1.35
            f3[start:end] -= 1.2
        else:
            f1[start:end] -= 1.5
            f2[start:end] += 2.0
            pred_score[start:end] = np.clip(pred_score[start:end] - 0.2, 0, 1)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "feature_1": f1,
            "feature_2": f2,
            "feature_3": f3,
            "pred_score": pred_score,
            "drift_label": drift_label,
        }
    )


@dataclass
class WindowResult:
    start_idx: int
    end_idx: int
    timestamp_start: pd.Timestamp
    timestamp_end: pd.Timestamp
    true_drift: int
    max_psi: float
    alert_fixed: int
    alert_adaptive: int


class RealtimeDriftEvaluator:
    def __init__(
        self,
        df: pd.DataFrame,
        feature_cols: list[str],
        pred_col: str | None,
        label_col: str,
        timestamp_col: str,
        reference_size: int = 5000,
        window_size: int = 1000,
        step_size: int = 1000,
        fixed_threshold: float = 0.20,
        adaptive_history_windows: int = 20,
        adaptive_quantile: float = 0.95,
    ) -> None:
        self.df = df.copy()
        self.feature_cols = feature_cols
        self.pred_col = pred_col
        self.label_col = label_col
        self.timestamp_col = timestamp_col
        self.reference_size = reference_size
        self.window_size = window_size
        self.step_size = step_size
        self.fixed_threshold = fixed_threshold
        self.adaptive_history_windows = adaptive_history_windows
        self.adaptive_quantile = adaptive_quantile

        if timestamp_col in self.df.columns:
            self.df[timestamp_col] = pd.to_datetime(self.df[timestamp_col])

    def _reference_frame(self) -> pd.DataFrame:
        return self.df.iloc[: self.reference_size]

    def _window_iter(self):
        start = self.reference_size
        n = len(self.df)
        while start + self.window_size <= n:
            end = start + self.window_size
            yield start, end, self.df.iloc[start:end]
            start += self.step_size

    def _compute_window_score(self, ref_df: pd.DataFrame, cur_df: pd.DataFrame) -> float:
        scores = []
        for col in self.feature_cols:
            scores.append(psi(ref_df[col].values, cur_df[col].values))
        if self.pred_col:
            scores.append(psi(ref_df[self.pred_col].values, cur_df[self.pred_col].values))
        return max(scores) if scores else 0.0

    def run(self) -> dict[str, object]:
        ref_df = self._reference_frame()

        scores_history = []
        results: list[WindowResult] = []

        total_windows = 0
        processing_errors = 0
        processing_latencies_ms: list[float] = []

        process_start = time.perf_counter()

        for start_idx, end_idx, cur_df in self._window_iter():
            t0 = time.perf_counter()
            total_windows += 1

            try:
                max_psi = self._compute_window_score(ref_df, cur_df)
                true_drift = int(cur_df[self.label_col].mean() >= 0.5)
                alert_fixed = int(max_psi >= self.fixed_threshold)

                if len(scores_history) >= max(5, self.adaptive_history_windows):
                    history_slice = scores_history[-self.adaptive_history_windows :]
                    adaptive_threshold = float(np.quantile(history_slice, self.adaptive_quantile))
                    adaptive_threshold = max(adaptive_threshold, self.fixed_threshold * 0.7)
                else:
                    adaptive_threshold = self.fixed_threshold

                alert_adaptive = int(max_psi >= adaptive_threshold)

                results.append(
                    WindowResult(
                        start_idx=start_idx,
                        end_idx=end_idx,
                        timestamp_start=cur_df[self.timestamp_col].iloc[0],
                        timestamp_end=cur_df[self.timestamp_col].iloc[-1],
                        true_drift=true_drift,
                        max_psi=max_psi,
                        alert_fixed=alert_fixed,
                        alert_adaptive=alert_adaptive,
                    )
                )

                scores_history.append(max_psi)
            except Exception:
                processing_errors += 1

            t1 = time.perf_counter()
            processing_latencies_ms.append((t1 - t0) * 1000.0)

        process_end = time.perf_counter()

        runtime_seconds = max(process_end - process_start, 1e-9)
        total_events_processed = len(self.df) - self.reference_size
        offline_eval_events_per_min = total_events_processed / runtime_seconds * 60.0

        fixed_true = [r.true_drift for r in results]
        fixed_pred = [r.alert_fixed for r in results]
        adaptive_pred = [r.alert_adaptive for r in results]

        fixed_metrics = precision_recall_f1(fixed_true, fixed_pred)
        adaptive_metrics = precision_recall_f1(fixed_true, adaptive_pred)

        false_alert_reduction = safe_percent(
            fixed_metrics["fp"] - adaptive_metrics["fp"],
            fixed_metrics["fp"] if fixed_metrics["fp"] > 0 else 1,
        )

        fixed_latency = self._detection_latency(results, mode="fixed")
        adaptive_latency = self._detection_latency(results, mode="adaptive")

        uptime_percent = 100.0 - safe_percent(processing_errors, total_windows if total_windows else 1)
        error_rate_percent = safe_percent(processing_errors, total_windows if total_windows else 1)

        return {
            "dataset_size_events": int(len(self.df)),
            "reference_size_events": int(self.reference_size),
            "window_size_events": int(self.window_size),
            "step_size_events": int(self.step_size),
            "num_windows": int(total_windows),
            "offline_eval_events_per_minute": round(offline_eval_events_per_min, 2),
            "window_processing_latency_avg_ms": round(safe_mean(processing_latencies_ms), 4),
            "window_processing_latency_p95_ms": round(percentile(processing_latencies_ms, 95), 4),
            "fixed_threshold": round(self.fixed_threshold, 4),
            "adaptive_quantile": self.adaptive_quantile,
            "fixed_detector": {
                **self._round_metrics(fixed_metrics),
                "avg_detection_latency_seconds": round(fixed_latency["avg_latency_seconds"], 2),
                "avg_detection_latency_windows": round(fixed_latency["avg_latency_windows"], 2),
                "detected_drift_segments": fixed_latency["detected_segments"],
                "total_drift_segments": fixed_latency["total_segments"],
            },
            "adaptive_detector": {
                **self._round_metrics(adaptive_metrics),
                "avg_detection_latency_seconds": round(adaptive_latency["avg_latency_seconds"], 2),
                "avg_detection_latency_windows": round(adaptive_latency["avg_latency_windows"], 2),
                "detected_drift_segments": adaptive_latency["detected_segments"],
                "total_drift_segments": adaptive_latency["total_segments"],
                "false_alert_reduction_percent_vs_fixed": round(false_alert_reduction, 2),
            },
            "system_reliability": {
                "uptime_percent": round(uptime_percent, 4),
                "error_rate_percent": round(error_rate_percent, 4),
            },
        }

    def _detection_latency(self, results: list[WindowResult], mode: str) -> dict[str, float]:
        latencies_seconds = []
        latencies_windows = []
        i = 0
        total_segments = 0
        detected_segments = 0

        while i < len(results):
            if results[i].true_drift == 1:
                total_segments += 1
                seg_start = i
                while i < len(results) and results[i].true_drift == 1:
                    i += 1
                seg_end = i - 1

                detected_idx = None
                for j in range(seg_start, seg_end + 1):
                    alert = results[j].alert_fixed if mode == "fixed" else results[j].alert_adaptive
                    if alert == 1:
                        detected_idx = j
                        break

                if detected_idx is not None:
                    detected_segments += 1
                    start_ts = results[seg_start].timestamp_start
                    detect_ts = results[detected_idx].timestamp_start
                    latency_seconds = (detect_ts - start_ts).total_seconds()
                    latencies_seconds.append(latency_seconds)
                    latencies_windows.append(detected_idx - seg_start)
            else:
                i += 1

        return {
            "avg_latency_seconds": safe_mean(latencies_seconds),
            "avg_latency_windows": safe_mean(latencies_windows),
            "detected_segments": detected_segments,
            "total_segments": total_segments,
        }

    @staticmethod
    def _round_metrics(metrics: dict[str, float]) -> dict[str, float]:
        return {k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()}


def load_csv_data(
    path: str,
    timestamp_col: str,
    label_col: str,
    pred_col: str | None,
    feature_cols: list[str] | None,
) -> tuple[pd.DataFrame, list[str]]:
    df = pd.read_csv(path)

    for c in [timestamp_col, label_col]:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    if pred_col and pred_col not in df.columns:
        raise ValueError(f"Missing pred_col: {pred_col}")

    if feature_cols is None or len(feature_cols) == 0:
        excluded = {timestamp_col, label_col}
        if pred_col:
            excluded.add(pred_col)
        feature_cols = [c for c in df.columns if c not in excluded]

    if not feature_cols:
        raise ValueError("No feature columns found.")

    return df, feature_cols


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate real-time ML drift metrics.")
    parser.add_argument("--mode", choices=["synthetic", "csv"], default="synthetic")
    parser.add_argument("--csv_path", type=str, default=None)
    parser.add_argument("--timestamp_col", type=str, default="timestamp")
    parser.add_argument("--label_col", type=str, default="drift_label")
    parser.add_argument("--pred_col", type=str, default="pred_score")
    parser.add_argument("--feature_cols", type=str, default="")
    parser.add_argument("--n_events", type=int, default=120000)
    parser.add_argument("--reference_size", type=int, default=5000)
    parser.add_argument("--window_size", type=int, default=1000)
    parser.add_argument("--step_size", type=int, default=1000)
    parser.add_argument("--fixed_threshold", type=float, default=0.20)
    parser.add_argument("--adaptive_history_windows", type=int, default=20)
    parser.add_argument("--adaptive_quantile", type=float, default=0.95)
    parser.add_argument("--out_json", type=str, default="drift_metrics.json")
    args = parser.parse_args()

    if args.mode == "synthetic":
        df = make_synthetic_stream(n_events=args.n_events)
        feature_cols = ["feature_1", "feature_2", "feature_3"]
        pred_col = args.pred_col
    else:
        if not args.csv_path:
            raise ValueError("--csv_path is required in csv mode")
        feature_cols_arg = [c.strip() for c in args.feature_cols.split(",") if c.strip()] if args.feature_cols else None
        df, feature_cols = load_csv_data(
            path=args.csv_path,
            timestamp_col=args.timestamp_col,
            label_col=args.label_col,
            pred_col=args.pred_col if args.pred_col else None,
            feature_cols=feature_cols_arg,
        )
        pred_col = args.pred_col if args.pred_col else None

    evaluator = RealtimeDriftEvaluator(
        df=df,
        feature_cols=feature_cols,
        pred_col=pred_col,
        label_col=args.label_col,
        timestamp_col=args.timestamp_col,
        reference_size=args.reference_size,
        window_size=args.window_size,
        step_size=args.step_size,
        fixed_threshold=args.fixed_threshold,
        adaptive_history_windows=args.adaptive_history_windows,
        adaptive_quantile=args.adaptive_quantile,
    )

    metrics = evaluator.run()

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print(json.dumps(metrics, indent=2))
    print(f"\nSaved metrics to: {args.out_json}")

    print("\nOffline evaluator summary (not API/system throughput):")
    print(f"- Offline eval events/min: {metrics['offline_eval_events_per_minute']}")
    print(f"- Dataset size: {metrics['dataset_size_events']}")
    print(
        f"- Fixed detector precision/recall: "
        f"{metrics['fixed_detector']['precision']} / {metrics['fixed_detector']['recall']}"
    )
    print(
        f"- Adaptive detector precision/recall: "
        f"{metrics['adaptive_detector']['precision']} / {metrics['adaptive_detector']['recall']}"
    )
    print(
        f"- Adaptive avg detection latency (sec): "
        f"{metrics['adaptive_detector']['avg_detection_latency_seconds']}"
    )
    print(
        f"- False alert reduction vs fixed: "
        f"{metrics['adaptive_detector']['false_alert_reduction_percent_vs_fixed']}%"
    )
    print(f"- Uptime: {metrics['system_reliability']['uptime_percent']}%")
    print(f"- Error rate: {metrics['system_reliability']['error_rate_percent']}%")


if __name__ == "__main__":
    main()
