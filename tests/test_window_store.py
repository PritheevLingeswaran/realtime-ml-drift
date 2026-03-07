from __future__ import annotations

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
