from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np

from src.models.anomaly_model import IsolationForestModel, ZScoreModel
from src.models.normalization import ScoreNormalizer


@dataclass
class ModelScorer:
    model_type: str
    enabled_features: List[str]
    warmup_events: int
    iforest_params: Dict[str, float | int]

    def __post_init__(self) -> None:
        if self.model_type == "isolation_forest":
            self.model = IsolationForestModel(
                n_estimators=int(self.iforest_params["n_estimators"]),
                contamination=float(self.iforest_params["contamination"]),
                random_state=int(self.iforest_params["random_state"]),
            )
        elif self.model_type == "zscore":
            self.model = ZScoreModel()
        else:
            raise ValueError(f"Unknown model type: {self.model_type}")

        self.normalizer = ScoreNormalizer()
        self._warm_X: List[np.ndarray] = []
        self._ready = False

    @property
    def ready(self) -> bool:
        return self._ready

    def _vec(self, feats: Dict[str, float]) -> np.ndarray:
        arr = np.array([float(feats[k]) for k in self.enabled_features], dtype=float)

        # Guardrail: sklearn can't handle inf/nan and can choke on huge magnitudes
        arr = np.nan_to_num(arr, nan=0.0, posinf=1e6, neginf=-1e6)
        arr = np.clip(arr, -1e6, 1e6)
        return arr

    def warmup_update(self, feats: Dict[str, float]) -> None:
        if self._ready:
            return
        self._warm_X.append(self._vec(feats))
        if len(self._warm_X) >= self.warmup_events:
            X = np.vstack(self._warm_X)
            self.model.fit(X)
            raw = self.model.score(X)

            # If IsolationForest: raw higher => normal; invert to anomaly-like
            if self.model_type == "isolation_forest":
                raw = -raw

            self.normalizer.fit(raw)
            self._ready = True
            self._warm_X.clear()

    def score(self, feats: Dict[str, float]) -> Tuple[float, float]:
        """Return (normalized_score, raw_score_anomaly_like)."""
        x = self._vec(feats).reshape(1, -1)
        raw = self.model.score(x).reshape(-1)
        if self.model_type == "isolation_forest":
            raw = -raw  # anomaly-like
        norm = float(self.normalizer.transform(raw)[0])
        return norm, float(raw[0])
