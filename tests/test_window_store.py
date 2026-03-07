from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable

import numpy as np

from src.feature_engineering.window_store import EntityWindowStore, WindowConfig
from src.schemas.event_schema import Event
from src.utils.ids import stable_id


def mk_event(i: int, ts: float, ent: str, amt: float) -> Event:
    return Event(
        event_id=stable_id("t", i, ent, ts),
        ts=ts,
        entity_id=ent,
        amount=amt,
        merchant_id="m1",
        merchant_category="grocery",
        country="US",
        channel="web",
        device_type="desktop",
        drift_tag=None,
    )


def test_window_eviction_by_time() -> None:
    store = EntityWindowStore(WindowConfig(window_seconds=10, max_events_per_entity=100))
    t0 = 1000.0
    ent = "acct_1"
    store.add(mk_event(1, t0, ent, 10.0))
    store.add(mk_event(2, t0 + 5, ent, 20.0))
    store.add(mk_event(3, t0 + 11, ent, 30.0))  # should evict first

    w = list(store.get_window(ent))
    assert len(w) == 2
    assert w[0].amount == 20.0
    assert w[1].amount == 30.0


def test_no_leakage_drop_current_event() -> None:
    store = EntityWindowStore(WindowConfig(window_seconds=300, max_events_per_entity=100))
    t0 = 1000.0
    ent = "acct_1"
    e1 = mk_event(1, t0, ent, 10.0)
    e2 = mk_event(2, t0 + 1, ent, 20.0)
    store.add(e1)
    store.add(e2)
    feats = store.compute_features(e2)
    # prior window should have only e1
    assert feats["cnt_5m"] == 1.0
    assert feats["amt_sum_5m"] == 10.0


def test_incremental_uniq_counts_with_eviction() -> None:
    store = EntityWindowStore(WindowConfig(window_seconds=3, max_events_per_entity=10))
    ent = "acct_uniq"
    t0 = 1000.0
    e1 = mk_event(1, t0, ent, 10.0)
    e2 = mk_event(2, t0 + 1, ent, 20.0)
    e3 = mk_event(3, t0 + 2, ent, 30.0)
    e2.merchant_id = "m2"
    e3.merchant_id = "m2"

    store.add(e1)
    store.add(e2)
    store.add(e3)
    feats = store.compute_features(e3)
    assert feats["uniq_merchant_5m"] == 2.0

    # Event at t0 is out of window and should be evicted from unique counts.
    e4 = mk_event(4, t0 + 5, ent, 40.0)
    e4.merchant_id = "m2"
    store.add(e4)
    feats2 = store.compute_features(e4)
    assert feats2["uniq_merchant_5m"] == 1.0


def _ref_features(window: Iterable[Event], e: Event) -> dict[str, float]:
    w = list(window)
    if w and w[-1].event_id == e.event_id:
        w = w[:-1]
    if not w:
        return {
            "cnt_5m": 0.0,
            "amt_sum_5m": 0.0,
            "amt_mean_5m": 0.0,
            "amt_std_5m": 0.0,
            "uniq_merchant_5m": 0.0,
            "uniq_country_5m": 0.0,
            "time_since_last": 0.0,
            "rate_per_min": 0.0,
        }
    amts = np.array([x.amount for x in w], dtype=float)
    first_ts = min(x.ts for x in w)
    dur = max(1.0, e.ts - first_ts)
    return {
        "cnt_5m": float(len(w)),
        "amt_sum_5m": float(amts.sum()),
        "amt_mean_5m": float(amts.mean()),
        "amt_std_5m": float(amts.std()),
        "uniq_merchant_5m": float(len({x.merchant_id for x in w})),
        "uniq_country_5m": float(len({x.country for x in w})),
        "time_since_last": float(max(0.0, e.ts - w[-1].ts)),
        "rate_per_min": float((len(w) / dur) * 60.0),
    }


def test_incremental_features_match_reference_deque_logic() -> None:
    cfg = WindowConfig(window_seconds=6, max_events_per_entity=8)
    store = EntityWindowStore(cfg)
    ref: dict[str, deque[Event]] = defaultdict(deque)

    events: list[Event] = []
    t0 = 1000.0
    for i in range(25):
        e = mk_event(i, t0 + i, f"acct_{i % 3}", amt=float(10 + (i % 7)))
        e.merchant_id = f"m{i % 4}"
        e.country = ["US", "IN", "GB"][i % 3]
        events.append(e)

    for e in events:
        dq = ref[e.entity_id]
        cutoff = e.ts - float(cfg.window_seconds)
        while dq and dq[0].ts < cutoff:
            dq.popleft()
        if len(dq) >= cfg.max_events_per_entity:
            dq.popleft()
        dq.append(e)

        store.add(e)
        got = store.compute_features(e)
        exp = _ref_features(dq, e)

        for k in (
            "cnt_5m",
            "amt_sum_5m",
            "amt_mean_5m",
            "amt_std_5m",
            "uniq_merchant_5m",
            "uniq_country_5m",
            "rate_per_min",
        ):
            assert abs(got[k] - exp[k]) < 1e-9, k
