from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np

from src.schemas.event_schema import Event


@dataclass
class WindowConfig:
    window_seconds: int
    max_events_per_entity: int


class EntityWindowStore:
    """Bounded per-entity event windows for streaming-safe features.

    Why this design:
    - Bounded memory (max_events_per_entity)
    - Time-based eviction (window_seconds)
    - No future leakage: only past events are present when computing features
    """

    def __init__(self, cfg: WindowConfig) -> None:
        self.cfg = cfg
        self._events: dict[str, deque[Event]] = defaultdict(
            lambda: deque(maxlen=cfg.max_events_per_entity)
        )
        self._last_ts: dict[str, float] = {}

    def add(self, e: Event) -> None:
        dq = self._events[e.entity_id]
        dq.append(e)
        self._last_ts[e.entity_id] = e.ts
        self._evict_old(e.entity_id, now_ts=e.ts)

    def _evict_old(self, entity_id: str, now_ts: float) -> None:
        dq = self._events[entity_id]
        cutoff = now_ts - float(self.cfg.window_seconds)
        while dq and dq[0].ts < cutoff:
            dq.popleft()

    def get_window(self, entity_id: str) -> Iterable[Event]:
        return self._events.get(entity_id, deque())

    def time_since_last(self, entity_id: str, now_ts: float) -> float:
        last = self._last_ts.get(entity_id)
        if last is None:
            return float("inf")
        return max(0.0, now_ts - last)

    def compute_features(self, e: Event) -> dict[str, float]:
        """Compute features for event e using the window that includes e (after add())."""
        window = list(self.get_window(e.entity_id))
        # Safety: window includes current event; for features that should be strictly prior,
        # drop the last element when it matches event_id.
        if window and window[-1].event_id == e.event_id:
            prior = window[:-1]
        else:
            prior = window

        if not prior:
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

        amts = np.array([x.amount for x in prior], dtype=float)
        cnt = float(len(prior))
        dur = max(1.0, e.ts - prior[0].ts)
        rate_per_min = (cnt / dur) * 60.0

        uniq_merchant = float(len(set(x.merchant_id for x in prior)))
        uniq_country = float(len(set(x.country for x in prior)))

        return {
            "cnt_5m": cnt,
            "amt_sum_5m": float(amts.sum()),
            "amt_mean_5m": float(amts.mean()),
            "amt_std_5m": float(amts.std(ddof=0)),
            "uniq_merchant_5m": uniq_merchant,
            "uniq_country_5m": uniq_country,
            "time_since_last": float(self.time_since_last(e.entity_id, e.ts)),
            "rate_per_min": float(rate_per_min),
        }
