from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

import orjson

from src.monitoring import metrics as m
from src.schemas.event_schema import Event


@dataclass
class KafkaConfig:
    enabled: bool
    bootstrap_servers: str
    topic: str
    group_id: str
    dlq_topic: str | None
    poll_timeout_ms: int


class KafkaEventSource:
    """Optional Kafka/Redpanda source with at-least-once commit semantics."""

    def __init__(
        self,
        cfg: KafkaConfig,
        *,
        consumer_factory: Callable[..., Any] | None = None,
        producer_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.cfg = cfg
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory
        self._consumer: Any = None
        self._producer: Any = None

    async def _build_consumer(self) -> Any:
        if self._consumer_factory is not None:
            return self._consumer_factory()
        try:
            from aiokafka import AIOKafkaConsumer  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Kafka source requires aiokafka for runtime ingestion") from exc
        return AIOKafkaConsumer(
            self.cfg.topic,
            bootstrap_servers=self.cfg.bootstrap_servers,
            group_id=self.cfg.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    async def _build_producer(self) -> Any | None:
        if not self.cfg.dlq_topic:
            return None
        if self._producer_factory is not None:
            return self._producer_factory()
        try:
            from aiokafka import AIOKafkaProducer  # type: ignore
        except Exception:
            return None
        return AIOKafkaProducer(bootstrap_servers=self.cfg.bootstrap_servers)

    async def start(self) -> None:
        self._consumer = await self._build_consumer()
        await self._consumer.start()
        self._producer = await self._build_producer()
        if self._producer is not None:
            await self._producer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
        if self._producer is not None:
            await self._producer.stop()

    async def stream(self) -> AsyncIterator[tuple[Event, Any]]:
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None
        while True:
            batches = await self._consumer.getmany(timeout_ms=int(self.cfg.poll_timeout_ms))
            for tp, records in batches.items():
                m.CONSUMER_LAG.labels(topic=str(tp.topic), partition=str(tp.partition)).set(
                    float(max(0, records[-1].offset - records[0].offset + 1 if records else 0))
                )
                for rec in records:
                    try:
                        obj = orjson.loads(rec.value)
                        yield Event(**obj), rec
                    except Exception:  # noqa: BLE001
                        m.DLQ_COUNT.inc()
                        if self._producer is not None and self.cfg.dlq_topic:
                            await self._producer.send_and_wait(self.cfg.dlq_topic, rec.value)

    async def commit(self, rec: Any) -> None:
        if self._consumer is None:
            return
        t0 = time.perf_counter()
        tp = rec.topic_partition
        offset = rec.offset + 1
        try:
            from aiokafka.structs import OffsetAndMetadata  # type: ignore

            await self._consumer.commit({tp: OffsetAndMetadata(offset, "")})
        except Exception:
            await self._consumer.commit({tp: offset})
        m.COMMIT_LATENCY_SECONDS.observe(max(0.0, time.perf_counter() - t0))
