from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from src.models.anomaly_model import IsolationForestModel, ZScoreModel
from src.models.normalization import ScoreNormalizer


@dataclass
class ModelScorer:
    model_type: str
    enabled_features: list[str]
    warmup_events: int
    iforest_params: dict[str, float | int]

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
        self._warm_X: list[np.ndarray] = []
        self._ready = False

    @property
    def ready(self) -> bool:
        return self._ready

    def _vec(self, feats: dict[str, float]) -> np.ndarray:
        arr = np.array([float(feats[k]) for k in self.enabled_features], dtype=float)

        # Guardrail: sklearn can't handle inf/nan and can choke on huge magnitudes
        arr = np.nan_to_num(arr, nan=0.0, posinf=1e6, neginf=-1e6)
        arr = np.clip(arr, -1e6, 1e6)
        return arr

    def warmup_update(self, feats: dict[str, float]) -> None:
        if self._ready:
            return
        self._warm_X.append(self._vec(feats))
        if len(self._warm_X) >= self.warmup_events:
            x = np.vstack(self._warm_X)
            self.model.fit(x)
            raw = self.model.score(x)

            # If IsolationForest: raw higher => normal; invert to anomaly-like
            if self.model_type == "isolation_forest":
                raw = -raw

            self.normalizer.fit(raw)
            self._ready = True
            self._warm_X.clear()

    def score(self, feats: dict[str, float]) -> tuple[float, float]:
        """Return (normalized_score, raw_score_anomaly_like)."""
        x = self._vec(feats).reshape(1, -1)
        raw = self.model.score(x).reshape(-1)
        if self.model_type == "isolation_forest":
            raw = -raw  # anomaly-like
        norm = float(self.normalizer.transform(raw)[0])
        return norm, float(raw[0])

    def score_batch(self, feats_batch: list[dict[str, float]]) -> list[tuple[float, float]]:
        """Vectorized scoring for a batch of already-featurized events.

        Why: model inference is one of the highest-frequency steps; batching
        reduces Python overhead while preserving per-event outputs.
        """
        if not feats_batch:
            return []
        x = np.vstack([self._vec(feats) for feats in feats_batch])
        raw = self.model.score(x).reshape(-1)
        if self.model_type == "isolation_forest":
            raw = -raw  # anomaly-like
        norm = self.normalizer.transform(raw)
        return [(float(norm[i]), float(raw[i])) for i in range(len(feats_batch))]

    def snapshot_state(self) -> dict[str, object]:
        mean, std = self.normalizer.state()
        return {
            "ready": bool(self._ready),
            "normalizer_mean": float(mean),
            "normalizer_std": float(std),
            "warm_count": int(len(self._warm_X)),
        }

    def load_snapshot_state(self, state: dict[str, object]) -> None:
        restored_ready = bool(state.get("ready", False))
        # Why: IsolationForest internals are not serialized in snapshots.
        # We must avoid claiming readiness with an unfitted estimator.
        self._ready = restored_ready if self.model_type != "isolation_forest" else False
        self.normalizer.load_state(
            float(state.get("normalizer_mean", 0.0)),
            float(state.get("normalizer_std", 1.0)),
        )
        # Why: warmup vectors are intentionally not restored to keep snapshot compact and stable.
        self._warm_X.clear()
