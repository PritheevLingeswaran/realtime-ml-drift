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
    baseline, _applied = run_pass(
        pass_name="baseline",
        config_path=config_path,
        cfg_raw=cfg,
        replay_events=replay_events,
        duration_sec=10,
        adaptation_enabled=False,
        labels_available=bool(meta.get("drift_tag_present_all", False)),
    )
    assert baseline.alerts >= 50
    assert 0.005 <= baseline.anomaly_rate <= 0.02
