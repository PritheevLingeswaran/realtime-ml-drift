from __future__ import annotations

import asyncio
import math

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


def test_micro_batch_processing_matches_single_event_outputs() -> None:
    state_seq = build_state("configs/dev.yaml")
    state_batch = build_state("configs/dev.yaml")
    # Keep runtime small and deterministic for both states.
    state_seq.scorer.warmup_events = 20
    state_batch.scorer.warmup_events = 20

    events = [_event(i, 1000.0 + i) for i in range(120)]
    outs_batch: list[dict[str, object]] = []

    with asyncio.Runner() as runner:
        outs_seq = [runner.run(process_event(state_seq, e, source="test")) for e in events]
        for i in range(0, len(events), 16):
            outs_batch.extend(
                runner.run(process_events_batch(state_batch, events[i : i + 16], source="test"))
            )

    assert len(outs_seq) == len(outs_batch) == len(events)
    for a, b in zip(outs_seq, outs_batch, strict=False):
        assert a.get("status") == b.get("status")
        assert a.get("event_id") == b.get("event_id")
        if a.get("status") != "scored":
            continue
        for k in ("is_anomaly", "drift_active", "drift_evaluated"):
            assert a.get(k) == b.get(k)
        for k in ("score", "raw_score", "threshold", "drift_score"):
            av = float(a.get(k, 0.0))
            bv = float(b.get(k, 0.0))
            assert math.isclose(av, bv, rel_tol=1e-9, abs_tol=1e-12), k
