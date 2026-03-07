from __future__ import annotations

import json
from pathlib import Path

from scripts.benchmark import load_replay_events


def _event_payload(event_id: str, drift_tag_included: bool) -> dict[str, object]:
    data: dict[str, object] = {
        "event_id": event_id,
        "ts": 1.0,
        "entity_id": "acct_1",
        "amount": 12.0,
        "merchant_id": "m1",
        "merchant_category": "food",
        "country": "US",
        "channel": "web",
        "device_type": "desktop",
    }
    if drift_tag_included:
        data["drift_tag"] = None
    return data


def test_load_replay_events_detects_missing_drift_tag(tmp_path: Path) -> None:
    replay = tmp_path / "replay.jsonl"
    rows = [
        _event_payload("e1", drift_tag_included=True),
        _event_payload("e2", drift_tag_included=False),
    ]
    replay.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")

    events, meta = load_replay_events(replay)
    assert len(events) == 2
    assert meta["drift_tag_present_all"] is False

