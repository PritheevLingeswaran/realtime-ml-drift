from __future__ import annotations

import argparse
import cProfile
import io
import json
import pstats
import random
import time
from pathlib import Path

from src.drift.monitor import DriftConfig, DriftMonitor

FEATURES = [
    "cnt_5m",
    "amt_sum_5m",
    "amt_mean_5m",
    "amt_std_5m",
    "uniq_merchant_5m",
    "uniq_country_5m",
    "time_since_last",
    "rate_per_min",
]


def make_monitor(evaluation_interval: int) -> DriftMonitor:
    cfg = DriftConfig(
        reference_window_events=2000,
        current_window_events=800,
        window_size=1000,
        evaluation_interval=evaluation_interval,
        check_interval_events=200,
        periodic_expensive_checks_enabled=False,
        min_samples=200,
        feature_ks_p=0.001,
        feature_psi=0.25,
        pred_ks_p=0.001,
        pred_psi=0.25,
        threshold_method="adaptive",
        threshold_k=2.0,
        fixed_score_threshold=1.0,
        feature_vote_fraction=0.3,
        smoothing_consecutive=3,
        alert_cooldown_events=300,
        score_weight_psi=0.5,
        score_weight_ks=0.3,
        score_weight_pred=0.2,
        baseline_min_evals=20,
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


def _profile_once(n_events: int, evaluation_interval: int) -> dict:
    rng = random.Random(42)
    monitor = make_monitor(evaluation_interval=evaluation_interval)

    pr = cProfile.Profile()
    pr.enable()
    ts = time.time()
    for i in range(n_events):
        ts += 0.01
        drifted = i > int(n_events * 0.55)
        feats = {
            "cnt_5m": rng.random() * (3.0 if drifted else 1.0),
            "amt_sum_5m": rng.lognormvariate(3.0, 0.4) * (1.8 if drifted else 1.0),
            "amt_mean_5m": rng.lognormvariate(2.5, 0.3) * (1.6 if drifted else 1.0),
            "amt_std_5m": rng.random() * (2.2 if drifted else 1.0),
            "uniq_merchant_5m": rng.randint(1, 20) * (1.4 if drifted else 1.0),
            "uniq_country_5m": rng.randint(1, 5) * (1.2 if drifted else 1.0),
            "time_since_last": rng.random() * 60,
            "rate_per_min": rng.random() * (2.0 if drifted else 1.0),
        }
        score = rng.random() * (1.2 if drifted else 1.0)
        monitor.update(feats=feats, score=score, ts=ts)
    pr.disable()

    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats(20)

    top3 = []
    for (filename, lineno, func), stat in sorted(
        ps.stats.items(), key=lambda item: item[1][3], reverse=True
    )[:3]:
        cc, nc, tt, ct, _ = stat
        top3.append(
            {
                "function": f"{Path(filename).name}:{lineno}:{func}",
                "primitive_calls": cc,
                "total_calls": nc,
                "total_time_s": tt,
                "cumulative_time_s": ct,
            }
        )

    return {
        "events_profiled": n_events,
        "evaluation_interval": evaluation_interval,
        "top3_cpu_functions": top3,
        "total_cumulative_time_s": top3[0]["cumulative_time_s"] if top3 else 0.0,
        "cprofile_top20": s.getvalue(),
    }


def run_profile(n_events: int, out_json: Path) -> None:
    baseline = _profile_once(n_events=n_events, evaluation_interval=1)
    optimized = _profile_once(n_events=n_events, evaluation_interval=200)

    try:
        from line_profiler import LineProfiler  # type: ignore

        lp = LineProfiler()
        lp.add_function(DriftMonitor.update)
        wrapped = lp(run_profile_inner)
        wrapped(2000)
        buf = io.StringIO()
        lp.print_stats(stream=buf)
        line_profiler_out = buf.getvalue()
    except Exception:
        line_profiler_out = "line_profiler not installed; skipped"

    ratio = (
        baseline["total_cumulative_time_s"] / max(1e-9, optimized["total_cumulative_time_s"])
    )
    out = {
        "baseline": baseline,
        "optimized": optimized,
        "speedup_x": ratio,
        "line_profiler": line_profiler_out,
    }

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "saved": str(out_json),
                "baseline_top3": baseline["top3_cpu_functions"],
                "optimized_top3": optimized["top3_cpu_functions"],
                "speedup_x": ratio,
            },
            indent=2,
        )
    )


def run_profile_inner(n_events: int) -> None:
    rng = random.Random(7)
    monitor = make_monitor(evaluation_interval=200)
    ts = time.time()
    for _ in range(n_events):
        ts += 0.01
        feats = {k: rng.random() for k in FEATURES}
        monitor.update(feats=feats, score=rng.random(), ts=ts)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Profile drift CPU hotspots.")
    p.add_argument("--events", type=int, default=15000)
    p.add_argument("--out_json", type=Path, default=Path("artifacts/drift_cpu_profile.json"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_profile(n_events=int(args.events), out_json=args.out_json)


if __name__ == "__main__":
    main()
