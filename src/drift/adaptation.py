from __future__ import annotations

from collections import deque
from dataclasses import dataclass

import numpy as np


@dataclass
class AdaptationConfig:
    enabled: bool
    target_anomaly_rate: float
    initial_threshold: float
    min_threshold: float
    max_threshold: float
    max_step: float
    cooldown_seconds: int


class ThresholdController:
    """Guarded threshold adaptation based on recent score distribution.

    Why this approach:
    - Unsupervised anomaly detectors rarely produce calibrated probabilities.
    - Business wants stable alert rate with tight safety controls.
    - We tune threshold to a *target* anomaly rate using quantiles of recent scores.
    - Guardrails prevent runaway automation.
    """

    def __init__(self, cfg: AdaptationConfig, window_size: int = 2000) -> None:
        self.cfg = cfg
        self.threshold = float(cfg.initial_threshold)
        self._scores: deque[float] = deque(maxlen=window_size)
        self._last_change_ts: float = 0.0

    def update(self, score: float, ts: float, drift_active: bool) -> float:
        self._scores.append(float(score))
        if not self.cfg.enabled:
            return self.threshold

        # Safety: do not adapt during suspected drift.
        if drift_active:
            return self.threshold

        # Cooldown to avoid thrashing
        if self._last_change_ts and (ts - self._last_change_ts) < float(self.cfg.cooldown_seconds):
            return self.threshold

        if len(self._scores) < 200:
            return self.threshold

        # Quantile threshold to hit target anomaly rate: threshold is (1 - target_rate) quantile
        q = max(0.0, min(1.0, 1.0 - float(self.cfg.target_anomaly_rate)))
        proposed = float(np.quantile(np.array(self._scores, dtype=float), q))

        # Clamp
        proposed = min(float(self.cfg.max_threshold), max(float(self.cfg.min_threshold), proposed))

        # Step limit
        delta = proposed - self.threshold
        if abs(delta) > float(self.cfg.max_step):
            proposed = self.threshold + float(self.cfg.max_step) * (1.0 if delta > 0 else -1.0)

        if proposed != self.threshold:
            self.threshold = proposed
            self._last_change_ts = ts

        return self.threshold

    def anomaly_rate(self) -> float:
        if not self._scores:
            return 0.0
        arr = np.array(self._scores, dtype=float)
        return float(np.mean(arr >= self.threshold))
