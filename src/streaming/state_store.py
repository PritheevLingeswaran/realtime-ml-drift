from __future__ import annotations

import os
import tempfile
import time
from dataclasses import dataclass
from typing import Any

import orjson

SNAPSHOT_SCHEMA_VERSION = 2


@dataclass
class SnapshotStore:
    path: str

    def save(self, payload: dict[str, Any]) -> None:
        # Why: atomic rename prevents partially-written snapshot corruption on crash.
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(prefix="snapshot_", suffix=".json", dir=os.path.dirname(self.path))
        try:
            with os.fdopen(tmp_fd, "wb") as f:
                f.write(orjson.dumps(payload))
            os.replace(tmp_path, self.path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def load(self) -> dict[str, Any] | None:
        if not os.path.exists(self.path):
            return None
        with open(self.path, "rb") as f:
            return orjson.loads(f.read())


def build_snapshot_payload(state: Any) -> dict[str, Any]:
    return {
        "version": SNAPSHOT_SCHEMA_VERSION,
        "saved_at_unix": time.time(),
        "threshold": state.threshold.snapshot_state(),
        "drift": state.drift.snapshot_state(),
        "features": state.featurizer.store.snapshot_state(),
        "scorer": state.scorer.snapshot_state(),
        "alerts": state.alerts.state_dict(),
        "runtime": {
            # Why: restart recovery should preserve dedupe state so a replay immediately after
            # restart does not double-count windows or re-emit alerts.
            "seen_event_ids": list(state.runtime.seen_event_ids),
            "dropped_events_total": int(state.runtime.dropped_events_total),
            "offered_events_total": int(state.runtime.offered_events_total),
            "last_snapshot_unix": time.time(),
        },
    }


def restore_snapshot_payload(state: Any, payload: dict[str, Any]) -> None:
    # Why: best-effort restore keeps service boot resilient when schema changes.
    version = int(payload.get("version", 0))
    if version != SNAPSHOT_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported_snapshot_version: expected={SNAPSHOT_SCHEMA_VERSION} got={version}"
        )
    state.threshold.load_snapshot_state(payload.get("threshold", {}))
    state.drift.load_snapshot_state(payload.get("drift", {}))
    state.featurizer.store.load_snapshot_state(payload.get("features", {}))
    state.scorer.load_snapshot_state(payload.get("scorer", {}))
    state.alerts.load_state_dict(payload.get("alerts", {}))
    runtime = payload.get("runtime", {})
    state.runtime.seen_event_ids.clear()
    state.runtime.seen_event_set.clear()
    for event_id in runtime.get("seen_event_ids", []):  # type: ignore[union-attr]
        eid = str(event_id)
        state.runtime.seen_event_ids.append(eid)
        state.runtime.seen_event_set.add(eid)
    state.runtime.dropped_events_total = int(runtime.get("dropped_events_total", 0))  # type: ignore[union-attr]
    state.runtime.offered_events_total = int(runtime.get("offered_events_total", 0))  # type: ignore[union-attr]
    state.runtime.last_snapshot_unix = float(
        runtime.get("last_snapshot_unix", payload.get("saved_at_unix", time.time()))  # type: ignore[union-attr]
    )
