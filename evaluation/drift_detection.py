from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np


@dataclass
class DriftMetrics:
    tp: int = 0
    fp: int = 0
    tn: int = 0
    fn: int = 0

    def precision(self) -> float:
        return self.tp / max(1, (self.tp + self.fp))

    def recall(self) -> float:
        return self.tp / max(1, (self.tp + self.fn))

    def f1(self) -> float:
        p, r = self.precision(), self.recall()
        return 2 * p * r / max(1e-9, (p + r))


def update_confusion(m: DriftMetrics, predicted_drift: bool, true_drift: bool) -> None:
    if predicted_drift and true_drift:
        m.tp += 1
    elif predicted_drift and not true_drift:
        m.fp += 1
    elif not predicted_drift and not true_drift:
        m.tn += 1
    else:
        m.fn += 1
