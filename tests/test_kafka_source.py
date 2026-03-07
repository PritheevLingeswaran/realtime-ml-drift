from __future__ import annotations

import asyncio
from dataclasses import dataclass

from src.streaming.kafka_source import KafkaConfig, KafkaEventSource


@dataclass(frozen=True)
class _TP:
    topic: str
    partition: int


@dataclass
class _Rec:
    value: bytes
    offset: int
    topic_partition: _TP


class _FakeConsumer:
    def __init__(self) -> None:
        self.started = False
        self.committed: list[tuple[_TP, int]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def getmany(self, timeout_ms: int):  # type: ignore[no-untyped-def]
        _ = timeout_ms
        tp = _TP(topic="rtml.events", partition=0)
        rec = _Rec(
            value=(
                b'{\"event_id\":\"e1\",\"ts\":1.0,\"entity_id\":\"acct_1\",\"amount\":10.0,'
                b'\"merchant_id\":\"m1\",\"merchant_category\":\"food\",\"country\":\"US\",'
                b'\"channel\":\"web\",\"device_type\":\"desktop\",\"drift_tag\":null}'
            ),
            offset=5,
            topic_partition=tp,
        )
        # Return one batch once, then empty forever.
        if not hasattr(self, "_done"):
            self._done = True
            return {tp: [rec]}
        await asyncio.sleep(0.01)
        return {}

    async def commit(self, offsets):  # type: ignore[no-untyped-def]
        for tp, off in offsets.items():
            self.committed.append((tp, off))


def test_kafka_source_stream_and_commit_with_mocked_consumer() -> None:
    fake = _FakeConsumer()
    src = KafkaEventSource(
        KafkaConfig(
            enabled=True,
            bootstrap_servers="localhost:9092",
            topic="rtml.events",
            group_id="g1",
            dlq_topic=None,
            poll_timeout_ms=100,
        ),
        consumer_factory=lambda: fake,
    )

    async def _run():  # type: ignore[no-untyped-def]
        async for evt, rec in src.stream():
            assert evt.event_id == "e1"
            await src.commit(rec)
            break
        await src.stop()

    with asyncio.Runner() as runner:
        runner.run(_run())

    assert fake.committed
    assert fake.committed[0][1] == 6
