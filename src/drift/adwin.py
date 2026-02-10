from __future__ import annotations

from dataclasses import dataclass

from river.drift import ADWIN


@dataclass
class ADWINDetector:
    """Streaming drift detector over a scalar stream (e.g., mean score)."""

    delta: float
    _d: ADWIN | None = None
    detected: bool = False

    def __post_init__(self) -> None:
        self._d = ADWIN(delta=self.delta)

    def update(self, x: float) -> bool:
        assert self._d is not None
        self.detected = self._d.update(x)  # True when change detected
        return self.detected
