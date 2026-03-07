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


def make_monitor() -> DriftMonitor:
    cfg = DriftConfig(
        reference_window_events=2000,
        current_window_events=800,
        window_size=1000,
        evaluation_interval=100,
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
        adwin_enabled=True,
        adwin_delta=0.002,
    )
    return DriftMonitor(cfg=cfg, feature_names=FEATURES)


def run_profile(n_events: int, out_json: Path) -> None:
    rng = random.Random(42)
    monitor = make_monitor()

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

    out = {
        "events_profiled": n_events,
        "top3_cpu_functions": top3,
        "cprofile_top20": s.getvalue(),
    }

    try:
        from line_profiler import LineProfiler  # type: ignore

        lp = LineProfiler()
        lp.add_function(DriftMonitor.update)
        wrapped = lp(run_profile_inner)
        wrapped(2000)
        buf = io.StringIO()
        lp.print_stats(stream=buf)
        out["line_profiler"] = buf.getvalue()
    except Exception:
        out["line_profiler"] = "line_profiler not installed; skipped"

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({"saved": str(out_json), "top3_cpu_functions": top3}, indent=2))


def run_profile_inner(n_events: int) -> None:
    rng = random.Random(7)
    monitor = make_monitor()
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
