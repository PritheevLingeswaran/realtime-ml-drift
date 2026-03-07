from __future__ import annotations

import asyncio
import time
from pathlib import Path

from src.schemas.event_schema import Event
from src.streaming.runner import build_state, process_event, process_source_with_backpressure
from src.streaming.state_store import SnapshotStore, build_snapshot_payload


def _event(i: int, ts: float) -> Event:
    return Event(
        event_id=f"evt_{i}",
        ts=ts,
        entity_id=f"acct_{i % 11}",
        amount=50.0 + (i % 7),
        merchant_id=f"m_{i % 17}",
        merchant_category="grocery",
        country="US",
        channel="web",
        device_type="desktop",
        drift_tag=None,
    )


class _BurstSource:
    def __init__(self, events: list[Event]) -> None:
        self._events = events

    async def stream(self):  # type: ignore[no-untyped-def]
        for e in self._events:
            yield e


def _isolated_config(tmp_path: Path) -> str:
    cfg_overlay = tmp_path / "test_runtime.yaml"
    cfg_overlay.write_text(
        "\n".join(
            [
                "state:",
                "  restore_on_startup: false",
                "  snapshot:",
                "    enabled: false",
            ]
        ),
        encoding="utf-8",
    )
    return str(cfg_overlay)


def test_duplicate_event_is_idempotent_for_state_and_alerts(tmp_path: Path) -> None:
    state = build_state(_isolated_config(tmp_path))
    state.scorer._ready = True  # type: ignore[attr-defined]
    state.scorer.score = lambda _f: (1.0, 1.0)  # type: ignore[assignment]
    state.threshold.threshold = 0.5

    e = _event(1, 1000.0)
    with asyncio.Runner() as runner:
        out1 = runner.run(process_event(state, e, source="stream", ingest_wall_ts=time.time()))
        out2 = runner.run(process_event(state, e, source="stream", ingest_wall_ts=time.time()))

    assert out1["status"] == "scored"
    assert out2["status"] == "duplicate"
    assert len(list(state.featurizer.store.get_window(e.entity_id))) == 1
    assert len(state.alerts.list(limit=10)) == 1


def test_backpressure_no_drops_by_default_and_lag_metrics_update(tmp_path: Path) -> None:
    state = build_state(_isolated_config(tmp_path))
    state.config["streaming"]["micro_batch"]["enabled"] = False
    state.config["streaming"]["ingestion"]["queue_max_size"] = 8
    state.config["streaming"]["ingestion"]["drop_on_overflow"] = False
    state.config["streaming"]["ingestion"]["lag_circuit_breaker_seconds"] = 9999.0
    state.scorer._ready = True  # type: ignore[attr-defined]
    state.scorer.score = lambda _f: (0.2, -0.1)  # type: ignore[assignment]

    events = [_event(i, 1000.0 + i) for i in range(250)]
    src = _BurstSource(events)

    with asyncio.Runner() as runner:
        runner.run(process_source_with_backpressure(state, src, is_kafka=False))

    assert state.runtime.dropped_events_total == 0
    assert state.runtime.max_processing_lag_seconds > 0.0


def test_snapshot_restore_persists_threshold_and_model_state(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "snapshots" / "state.json"
    cfg_overlay = tmp_path / "test_state.yaml"
    cfg_overlay.write_text(
        "\n".join(
            [
                "model:",
                "  type: zscore",
                "  warmup_events: 1",
                "state:",
                "  restore_on_startup: true",
                "  snapshot:",
                "    enabled: true",
                f"    path: {snapshot_path}",
                "    interval_seconds: 1",
            ]
        ),
        encoding="utf-8",
    )

    state1 = build_state(str(cfg_overlay))
    state1.threshold.threshold = 0.777
    state1.scorer._ready = True  # type: ignore[attr-defined]
    state1.scorer.normalizer.load_state(0.12, 0.34)
    store = SnapshotStore(path=str(snapshot_path))
    store.save(build_snapshot_payload(state1))

    state2 = build_state(str(cfg_overlay))
    assert state2.runtime.restored is True
    assert abs(state2.threshold.threshold - 0.777) < 1e-9
    assert state2.scorer.ready is True
    mean, std = state2.scorer.normalizer.state()
    assert abs(mean - 0.12) < 1e-9
    assert abs(std - 0.34) < 1e-9
