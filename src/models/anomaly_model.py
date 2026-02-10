from __future__ import annotations

from dataclasses import dataclass
from typing import List, Protocol

import numpy as np
from sklearn.ensemble import IsolationForest


class AnomalyModel(Protocol):
    def fit(self, X: np.ndarray) -> None: ...
    def score(self, X: np.ndarray) -> np.ndarray: ...


@dataclass
class IsolationForestModel:
    n_estimators: int
    contamination: float
    random_state: int

    def __post_init__(self) -> None:
        # Decision: IsolationForest is common in prod for unsupervised anomaly scoring.
        # It is *not* truly online, so we treat it as a stable scoring model and avoid blind retraining.
        self._m = IsolationForest(
            n_estimators=self.n_estimators,
            contamination=self.contamination,
            random_state=self.random_state,
            n_jobs=-1,
        )

    def fit(self, X: np.ndarray) -> None:
        self._m.fit(X)

    def score(self, X: np.ndarray) -> np.ndarray:
        # IsolationForest: higher = more normal; we invert to anomaly-like positive score later.
        return self._m.score_samples(X)


@dataclass
class ZScoreModel:
    """Fallback fully-online baseline: z-score of a single aggregate feature vector norm.
    Not as strong, but stable and incremental.

    We keep it to show how you'd swap models via config, and for deterministic testing.
    """

    mean_: float = 0.0
    var_: float = 1.0
    n_: int = 0

    def fit(self, X: np.ndarray) -> None:
        # Fit incremental mean/var over vector norms
        norms = np.linalg.norm(X, axis=1)
        for v in norms:
            self.update(float(v))

    def update(self, v: float) -> None:
        self.n_ += 1
        delta = v - self.mean_
        self.mean_ += delta / self.n_
        delta2 = v - self.mean_
        self.var_ += delta * delta2

    def score(self, X: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(X, axis=1)
        denom = np.sqrt(max(1e-9, self.var_ / max(1, self.n_ - 1)))
        z = (norms - self.mean_) / denom
        # Higher z => more anomalous
        return z
