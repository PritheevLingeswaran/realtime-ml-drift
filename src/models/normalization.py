from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Tuple

import numpy as np


@dataclass
class ScoreNormalizer:
    """Maps raw model scores to [0, 1].

    Why:
    - Different models output scores on different scales.
    - Alerting and drift checks need a consistent bounded score distribution.
    """

    mean_: float = 0.0
    std_: float = 1.0

    def fit(self, raw_scores: np.ndarray) -> None:
        m = float(np.mean(raw_scores))
        s = float(np.std(raw_scores)) or 1.0
        self.mean_, self.std_ = m, s

    def transform(self, raw_scores: np.ndarray) -> np.ndarray:
        z = (raw_scores - self.mean_) / (self.std_ + 1e-9)
        # Sigmoid to [0,1]
        return 1.0 / (1.0 + np.exp(-z))

    def state(self) -> Tuple[float, float]:
        return self.mean_, self.std_
