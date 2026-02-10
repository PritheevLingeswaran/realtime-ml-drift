from __future__ import annotations

import json
import os
from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional

from src.schemas.alert_schema import Alert


@dataclass
class AlertStore:
    """In-memory ring buffer + optional append-only JSONL sink."""

    max_size: int
    sink_path: Optional[str] = None

    def __post_init__(self) -> None:
        self._buf: Deque[Alert] = deque(maxlen=self.max_size)
        if self.sink_path:
            os.makedirs(os.path.dirname(self.sink_path), exist_ok=True)

    def add(self, alert: Alert) -> None:
        self._buf.appendleft(alert)  # newest first
        if self.sink_path:
            with open(self.sink_path, "a", encoding="utf-8") as f:
                f.write(alert.model_dump_json())
                f.write("\n")

    def list(self, limit: int = 200) -> List[Alert]:
        out = []
        for i, a in enumerate(self._buf):
            if i >= limit:
                break
            out.append(a)
        return out
