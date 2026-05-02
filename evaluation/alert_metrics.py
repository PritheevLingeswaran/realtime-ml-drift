from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AlertMetrics:
    tp: int = 0
    fp: int = 0
    fn: int = 0

    def precision(self) -> float:
        return self.tp / max(1, (self.tp + self.fp))

    def recall(self) -> float:
        return self.tp / max(1, (self.tp + self.fn))

    def f1(self) -> float:
        p, r = self.precision(), self.recall()
        return 2 * p * r / max(1e-9, (p + r))

