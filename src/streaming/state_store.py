from __future__ import annotations

import os
import tempfile
import time
from dataclasses import dataclass
from typing import Any

import orjson


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
        "version": 1,
        "saved_at_unix": time.time(),
        "threshold": state.threshold.snapshot_state(),
        "drift": state.drift.snapshot_state(),
        "features": state.featurizer.store.snapshot_state(),
        "scorer": state.scorer.snapshot_state(),
        "alerts": state.alerts.state_dict(),
    }


def restore_snapshot_payload(state: Any, payload: dict[str, Any]) -> None:
    # Why: best-effort restore keeps service boot resilient when schema changes.
    state.threshold.load_snapshot_state(payload.get("threshold", {}))
    state.drift.load_snapshot_state(payload.get("drift", {}))
    state.featurizer.store.load_snapshot_state(payload.get("features", {}))
    state.scorer.load_snapshot_state(payload.get("scorer", {}))
    state.alerts.load_state_dict(payload.get("alerts", {}))
