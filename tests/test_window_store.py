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
