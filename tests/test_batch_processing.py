from __future__ import annotations

import asyncio

from src.schemas.event_schema import Event
from src.streaming.runner import build_state, process_event, process_events_batch


def _event(i: int, ts: float) -> Event:
    return Event(
        event_id=f"e_{i}",
        ts=ts,
        entity_id=f"acct_{i % 7}",
        amount=20.0 + (i % 5),
        merchant_id=f"m_{i % 11}",
        merchant_category="grocery",
        country="US",
        channel="web",
        device_type="desktop",
        drift_tag=None,
    )


def test_micro_batch_processing_keeps_output_shape() -> None:
    state = build_state("configs/dev.yaml")
    # Keep test runtime small while preserving model behavior.
    state.scorer.warmup_events = 10

    events = [_event(i, 1000.0 + i) for i in range(50)]

    with asyncio.Runner() as runner:
        outs_seq = [runner.run(process_event(state, e, source="test")) for e in events[:20]]
        outs_batch = runner.run(process_events_batch(state, events[20:], source="test"))

    assert len(outs_seq) == 20
    assert len(outs_batch) == 30
    assert all("status" in o for o in outs_seq)
    assert all("status" in o for o in outs_batch)
    assert any(o.get("status") == "scored" for o in outs_batch)
