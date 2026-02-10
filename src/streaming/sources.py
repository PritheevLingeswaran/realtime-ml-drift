from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator
from dataclasses import dataclass

import orjson

from src.schemas.event_schema import Event
from src.utils.ids import stable_id
from src.utils.time import now_ts

MERCHANT_CATEGORIES = ["grocery", "fuel", "electronics", "fashion", "pharmacy", "travel", "food"]
COUNTRIES = ["US", "IN", "CA", "GB", "AU"]
CHANNELS = ["web", "mobile", "pos"]
DEVICE_TYPES = ["ios", "android", "desktop", "unknown"]


@dataclass
class DriftScenario:
    enabled: bool
    drift_start_event: int
    drift_type: str


class EventSource:
    async def stream(self) -> AsyncIterator[Event]:
        raise NotImplementedError


@dataclass
class SyntheticTransactionSource(EventSource):
    seed: int
    event_rate_per_sec: float
    entity_cardinality: int
    max_events: int
    drift: DriftScenario

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self._i = 0

    def _amount(self, drifted: bool) -> float:
        # Pre-drift: lognormal-ish; drift shifts mean or increases tails.
        base = self.rng.lognormvariate(mu=3.2, sigma=0.55)  # ~25 mean
        if not drifted:
            return float(base)

        if self.drift.drift_type == "mean_shift_amount":
            return float(base * 2.0)  # mean shift
        if self.drift.drift_type == "bursty_entities":
            # occasionally large spikes
            if self.rng.random() < 0.03:
                return float(base * 12.0)
            return float(base)
        return float(base)

    def _merchant_category(self, drifted: bool) -> str:
        if drifted and self.drift.drift_type == "new_category":
            # Introduce a new category distribution shift
            cats = MERCHANT_CATEGORIES + ["crypto_exchange", "gaming"]
            weights = [1, 1, 1, 1, 1, 1, 1, 0.8, 0.8]
            return self.rng.choices(cats, weights=weights, k=1)[0]
        return self.rng.choice(MERCHANT_CATEGORIES)

    async def stream(self) -> AsyncIterator[Event]:
        # If event_rate is high, sleeping each event is inefficient; but ok for local demo.
        # Production uses broker backpressure and batch poll loops.
        delay = 1.0 / max(1e-9, float(self.event_rate_per_sec))
        while True:
            self._i += 1
            if self.max_events and self._i > self.max_events:
                return

            drifted = bool(self.drift.enabled and self._i >= self.drift.drift_start_event)

            ts = now_ts()
            entity_id = f"acct_{self.rng.randrange(self.entity_cardinality):06d}"
            merchant_id = f"m_{self.rng.randrange(2000):04d}"
            country = self.rng.choice(COUNTRIES)
            channel = self.rng.choice(CHANNELS)
            device = self.rng.choice(DEVICE_TYPES)

            amount = self._amount(drifted=drifted)
            mcat = self._merchant_category(drifted=drifted)

            event_id = stable_id(self.seed, self._i, entity_id, ts, amount, merchant_id)
            e = Event(
                event_id=event_id,
                ts=ts,
                entity_id=entity_id,
                amount=float(amount),
                merchant_id=merchant_id,
                merchant_category=mcat,
                country=country,
                channel=channel,  # type: ignore[arg-type]
                device_type=device,  # type: ignore[arg-type]
                drift_tag=self.drift.drift_type if drifted else None,
            )
            yield e
            await asyncio.sleep(delay)


@dataclass
class JSONLReplaySource(EventSource):
    path: str
    speedup: float = 1.0  # 1.0 => real-time; >1 => faster

    async def stream(self) -> AsyncIterator[Event]:
        # Deterministic replay: preserve original timestamps but optionally speed up sleep.
        prev_ts: float | None = None
        with open(self.path, "rb") as f:
            for line in f:
                obj = orjson.loads(line)
                e = Event(**obj)
                if prev_ts is not None:
                    dt = max(0.0, float(e.ts - prev_ts))
                    await asyncio.sleep(dt / max(1e-9, self.speedup))
                prev_ts = float(e.ts)
                yield e
