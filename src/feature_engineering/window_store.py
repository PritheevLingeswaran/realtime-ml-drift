from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable
from dataclasses import dataclass, field

import numpy as np

from src.schemas.event_schema import Event


@dataclass
class WindowConfig:
    window_seconds: int
    max_events_per_entity: int


@dataclass
class _EntityStats:
    """Incremental bounded stats for one entity window.

    Why this structure:
    - Avoid per-event list copies and full-window scans for common aggregates.
    - Preserve bounded memory via deque maxlen + time eviction.
    - Keep exact uniq-counts with reference maps and safe decrements on eviction.
    """

    max_events: int
    events: deque[Event] = field(init=False)
    sum_amount: float = 0.0
    sumsq_amount: float = 0.0
    merchant_counts: dict[str, int] = field(default_factory=dict)
    country_counts: dict[str, int] = field(default_factory=dict)
    last_event_ts: float | None = None

    def __post_init__(self) -> None:
        self.events = deque(maxlen=self.max_events)


class EntityWindowStore:
    """Bounded per-entity event windows for streaming-safe features.

    Why this design:
    - Bounded memory (max_events_per_entity)
    - Time-based eviction (window_seconds)
    - No future leakage: only past events are present when computing features
    """

    def __init__(self, cfg: WindowConfig) -> None:
        self.cfg = cfg
        self._entities: dict[str, _EntityStats] = defaultdict(
            lambda: _EntityStats(max_events=cfg.max_events_per_entity)
        )

    def add(self, e: Event) -> None:
        st = self._entities[e.entity_id]

        # If deque is at capacity, remove the leftmost event before append so
        # incremental aggregates remain exact with bounded memory.
        if len(st.events) == st.events.maxlen and st.events:
            self._remove_from_stats(st, st.events[0])
            st.events.popleft()

        st.events.append(e)
        self._add_to_stats(st, e)
        st.last_event_ts = e.ts
        self._evict_old(e.entity_id, now_ts=e.ts)

    def _evict_old(self, entity_id: str, now_ts: float) -> None:
        st = self._entities[entity_id]
        cutoff = now_ts - float(self.cfg.window_seconds)
        while st.events and st.events[0].ts < cutoff:
            old = st.events.popleft()
            self._remove_from_stats(st, old)

    def _add_to_stats(self, st: _EntityStats, e: Event) -> None:
        amt = float(e.amount)
        st.sum_amount += amt
        st.sumsq_amount += amt * amt
        st.merchant_counts[e.merchant_id] = st.merchant_counts.get(e.merchant_id, 0) + 1
        st.country_counts[e.country] = st.country_counts.get(e.country, 0) + 1

    def _remove_from_stats(self, st: _EntityStats, e: Event) -> None:
        amt = float(e.amount)
        st.sum_amount -= amt
        st.sumsq_amount -= amt * amt

        mc = st.merchant_counts.get(e.merchant_id, 0)
        if mc <= 1:
            st.merchant_counts.pop(e.merchant_id, None)
        else:
            st.merchant_counts[e.merchant_id] = mc - 1

        cc = st.country_counts.get(e.country, 0)
        if cc <= 1:
            st.country_counts.pop(e.country, None)
        else:
            st.country_counts[e.country] = cc - 1

    def get_window(self, entity_id: str) -> Iterable[Event]:
        st = self._entities.get(entity_id)
        return st.events if st is not None else deque()

    def time_since_last(self, entity_id: str, now_ts: float) -> float:
        st = self._entities.get(entity_id)
        if st is None or st.last_event_ts is None:
            return float("inf")
        return max(0.0, now_ts - st.last_event_ts)

    def compute_features(self, e: Event) -> dict[str, float]:
        """Compute features for event e using window that includes e.

        Safety behavior is preserved: if the most recent event is the current event,
        features are computed on strictly prior events only.
        """
        st = self._entities.get(e.entity_id)
        if st is None or not st.events:
            return {
                "cnt_5m": 0.0,
                "amt_sum_5m": 0.0,
                "amt_mean_5m": 0.0,
                "amt_std_5m": 0.0,
                "uniq_merchant_5m": 0.0,
                "uniq_country_5m": 0.0,
                "time_since_last": float("inf"),
                "rate_per_min": 0.0,
            }

        cnt = len(st.events)
        sum_amount = st.sum_amount
        sumsq_amount = st.sumsq_amount
        uniq_merchant = float(len(st.merchant_counts))
        uniq_country = float(len(st.country_counts))

        # Drop current event when present at tail to avoid leakage.
        if st.events and st.events[-1].event_id == e.event_id:
            tail = st.events[-1]
            cnt -= 1
            amt = float(tail.amount)
            sum_amount -= amt
            sumsq_amount -= amt * amt

            mc = st.merchant_counts.get(tail.merchant_id, 0)
            if mc == 1:
                uniq_merchant -= 1.0

            cc = st.country_counts.get(tail.country, 0)
            if cc == 1:
                uniq_country -= 1.0

        if cnt <= 0:
            return {
                "cnt_5m": 0.0,
                "amt_sum_5m": 0.0,
                "amt_mean_5m": 0.0,
                "amt_std_5m": 0.0,
                "uniq_merchant_5m": 0.0,
                "uniq_country_5m": 0.0,
                "time_since_last": float(self.time_since_last(e.entity_id, e.ts)),
                "rate_per_min": 0.0,
            }

        mean = sum_amount / float(cnt)
        var = max(0.0, (sumsq_amount / float(cnt)) - (mean * mean))
        std = float(np.sqrt(var))

        first_ts = st.events[0].ts
        dur = max(1.0, e.ts - first_ts)
        rate_per_min = (float(cnt) / dur) * 60.0

        return {
            "cnt_5m": float(cnt),
            "amt_sum_5m": float(sum_amount),
            "amt_mean_5m": float(mean),
            "amt_std_5m": std,
            "uniq_merchant_5m": float(max(0.0, uniq_merchant)),
            "uniq_country_5m": float(max(0.0, uniq_country)),
            "time_since_last": float(self.time_since_last(e.entity_id, e.ts)),
            "rate_per_min": float(rate_per_min),
        }

    def snapshot_state(self) -> dict[str, object]:
        # Why: bounded per-entity state must survive restart to avoid feature cold-start regressions.
        out: dict[str, object] = {"entities": {}}
        entities = out["entities"]  # type: ignore[assignment]
        for entity_id, st in self._entities.items():
            entities[entity_id] = {  # type: ignore[index]
                "events": [e.model_dump() for e in st.events],
                "sum_amount": float(st.sum_amount),
                "sumsq_amount": float(st.sumsq_amount),
                "merchant_counts": dict(st.merchant_counts),
                "country_counts": dict(st.country_counts),
                "last_event_ts": st.last_event_ts,
            }
        return out

    def load_snapshot_state(self, state: dict[str, object]) -> None:
        self._entities.clear()
        entities = state.get("entities", {})
        for entity_id, payload in entities.items():  # type: ignore[union-attr]
            st = _EntityStats(max_events=self.cfg.max_events_per_entity)
            for row in payload.get("events", []):  # type: ignore[union-attr]
                st.events.append(Event(**row))  # type: ignore[arg-type]
            st.sum_amount = float(payload.get("sum_amount", 0.0))  # type: ignore[union-attr]
            st.sumsq_amount = float(payload.get("sumsq_amount", 0.0))  # type: ignore[union-attr]
            st.merchant_counts = {
                str(k): int(v) for k, v in payload.get("merchant_counts", {}).items()  # type: ignore[union-attr]
            }
            st.country_counts = {
                str(k): int(v) for k, v in payload.get("country_counts", {}).items()  # type: ignore[union-attr]
            }
            last_ts = payload.get("last_event_ts", None)  # type: ignore[union-attr]
            st.last_event_ts = None if last_ts is None else float(last_ts)
            self._entities[str(entity_id)] = st
