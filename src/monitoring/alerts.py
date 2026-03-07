from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass

from src.schemas.alert_schema import Alert


@dataclass
class AlertStore:
    """In-memory ring buffer + optional append-only JSONL sink."""

    max_size: int
    sink_path: str | None = None
    dedupe_size: int = 50000

    def __post_init__(self) -> None:
        self._buf: deque[Alert] = deque(maxlen=self.max_size)
        self._seen_ids: deque[str] = deque(maxlen=self.dedupe_size)
        self._seen_set: set[str] = set()
        if self.sink_path:
            os.makedirs(os.path.dirname(self.sink_path), exist_ok=True)

    def add(self, alert: Alert) -> None:
        # Why: at-least-once processing may replay events; alert_id dedupe keeps emission idempotent.
        if alert.alert_id in self._seen_set:
            return
        if len(self._seen_ids) == self._seen_ids.maxlen and self._seen_ids:
            old = self._seen_ids.popleft()
            self._seen_set.discard(old)
        self._seen_ids.append(alert.alert_id)
        self._seen_set.add(alert.alert_id)
        self._buf.appendleft(alert)  # newest first
        if self.sink_path:
            with open(self.sink_path, "a", encoding="utf-8") as f:
                f.write(alert.model_dump_json())
                f.write("\n")

    def list(self, limit: int = 200) -> list[Alert]:
        out = []
        for i, a in enumerate(self._buf):
            if i >= limit:
                break
            out.append(a)
        return out

    def state_dict(self) -> dict[str, object]:
        return {
            "alerts": [a.model_dump() for a in self._buf],
            "seen_ids": list(self._seen_ids),
            "dedupe_size": int(self.dedupe_size),
        }

    def load_state_dict(self, state: dict[str, object]) -> None:
        self._buf.clear()
        self._seen_ids.clear()
        self._seen_set.clear()
        for row in state.get("alerts", []):  # type: ignore[union-attr]
            self._buf.append(Alert(**row))  # type: ignore[arg-type]
        for alert_id in state.get("seen_ids", []):  # type: ignore[union-attr]
            aid = str(alert_id)
            self._seen_ids.append(aid)
            self._seen_set.add(aid)
