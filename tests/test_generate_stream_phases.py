from __future__ import annotations

from scripts.generate_stream_phases import Phase, WorkloadConfig, generate_events


def test_generate_stream_phases_labels_and_counts() -> None:
    cfg = WorkloadConfig(
        seed=1,
        entity_cardinality=100,
        phases=[
            Phase(name="normal", duration_seconds=2, ground_truth_drift=0, pattern="baseline"),
            Phase(name="drift", duration_seconds=1, ground_truth_drift=1, pattern="sudden"),
        ],
    )

    rows = generate_events(cfg, events_per_second=10, start_ts=1000.0)

    assert len(rows) == 30
    assert sum(1 for r in rows if r["ground_truth_drift"] == 1) == 10
    assert rows[0]["phase"] == "normal"
    assert rows[-1]["phase"] == "drift"
    assert "event_id" in rows[0]["event"]
