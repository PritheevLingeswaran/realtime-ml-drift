from __future__ import annotations

import hashlib
from typing import Any


def stable_id(*parts: Any) -> str:
    """Stable ID generator for deterministic replay/debugging."""
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()[:24]
