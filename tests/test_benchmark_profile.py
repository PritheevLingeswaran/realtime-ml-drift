from __future__ import annotations

from pathlib import Path

from scripts.benchmark import load_replay_events, run_pass
from src.utils.config import load_config


def test_benchmark_profile_produces_measurable_alert_volume_on_replay() -> None:
    config_path = "configs/benchmark.yaml"
    replay_path = Path("data/raw/streams/dev_stream.jsonl")
    assert replay_path.exists()

    cfg = load_config(config_path).raw
    replay_events, meta = load_replay_events(replay_path)
    # Bound the pass by a fixed event budget rather than wall-clock time: the
    # pipeline's scores are non-stationary as the model calibrates, so a
    # duration-based window produces a machine-dependent score distribution
    # (and alert volume). A fixed budget makes the pass deterministic and
    # reproducible on any host, including CI. duration_sec is a safety cap only.
    baseline, _applied = run_pass(
        pass_name="baseline",
        config_path=config_path,
        cfg_raw=cfg,
        replay_events=replay_events,
        duration_sec=300,
        adaptation_enabled=False,
        labels_available=bool(meta.get("drift_tag_present_all", False)),
        max_events=12000,
    )
    assert baseline.alerts >= 50
    assert 0.005 <= baseline.anomaly_rate <= 0.02
