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
    min_history: int = 200
    history_window_size: int = 2000
    adapt_during_drift: bool = False
    rate_feedback_enabled: bool = False
    target_tolerance_abs: float = 0.0


class ThresholdController:
    """Guarded threshold adaptation based on recent score distribution.

    Why this approach:
    - Unsupervised anomaly detectors rarely produce calibrated probabilities.
    - Business wants stable alert rate with tight safety controls.
    - We tune threshold to a *target* anomaly rate using quantiles of recent scores.
    - Guardrails prevent runaway automation.
    """

    def __init__(self, cfg: AdaptationConfig, window_size: int | None = None) -> None:
        self.cfg = cfg
        self.threshold = float(cfg.initial_threshold)
        history_window_size = (
            int(window_size)
            if window_size is not None
            else max(1, int(cfg.history_window_size))
        )
        self._scores: deque[float] = deque(maxlen=history_window_size)
        self._last_change_ts: float = 0.0
        self._frozen: bool = False
        self._freeze_reason: str = ""
        self._skipped_drift = 0
        self._skipped_cooldown = 0
        self._skipped_history = 0
        self._step_limited = 0
        self._bounds_limited = 0
        self._feedback_blocked = 0
        self._updates_applied = 0

    def update(self, score: float, ts: float, drift_active: bool) -> float:
        # Safety: NaN/inf scores corrupt quantile adaptation and can collapse threshold.
        if np.isfinite(score):
            self._scores.append(float(score))
        if not self.cfg.enabled:
            return self.threshold
        if self._frozen:
            return self.threshold

        # Safety: do not adapt during suspected drift.
        if drift_active and not self.cfg.adapt_during_drift:
            self._skipped_drift += 1
            return self.threshold

        # Cooldown to avoid thrashing
        if self._last_change_ts and (ts - self._last_change_ts) < float(self.cfg.cooldown_seconds):
            self._skipped_cooldown += 1
            return self.threshold

        if len(self._scores) < max(1, int(self.cfg.min_history)):
            self._skipped_history += 1
            return self.threshold

        # Quantile threshold to hit target anomaly rate: threshold is (1 - target_rate) quantile
        q = max(0.0, min(1.0, 1.0 - float(self.cfg.target_anomaly_rate)))
        arr = np.array(self._scores, dtype=float)
        arr = arr[np.isfinite(arr)]
        if len(arr) < max(1, int(self.cfg.min_history)):
            self._skipped_history += 1
            return self.threshold
        proposed = float(np.quantile(arr, q))

        # Clamp
        pre_bounds = proposed
        proposed = min(float(self.cfg.max_threshold), max(float(self.cfg.min_threshold), proposed))
        if proposed != pre_bounds:
            self._bounds_limited += 1

        # Step limit
        delta = proposed - self.threshold
        if abs(delta) > float(self.cfg.max_step):
            proposed = self.threshold + float(self.cfg.max_step) * (1.0 if delta > 0 else -1.0)
            self._step_limited += 1

        if self.cfg.rate_feedback_enabled:
            current_rate = self.anomaly_rate()
            upper = float(self.cfg.target_anomaly_rate) + float(self.cfg.target_tolerance_abs)
            lower = float(self.cfg.target_anomaly_rate) - float(self.cfg.target_tolerance_abs)
            # Why: quantile-only updates can move the threshold the wrong way on non-stationary streams.
            # Benchmark-mode feedback forces at least one corrective step in the direction that reduces rate error.
            if current_rate > upper and proposed < self.threshold:
                proposed = self.threshold + float(self.cfg.max_step)
                self._feedback_blocked += 1
            elif current_rate < lower and proposed > self.threshold:
                proposed = self.threshold - float(self.cfg.max_step)
                self._feedback_blocked += 1

        if proposed != self.threshold:
            self.threshold = proposed
            self._last_change_ts = ts
            self._updates_applied += 1

        return self.threshold

    def anomaly_rate(self) -> float:
        if not self._scores:
            return 0.0
        arr = np.array(self._scores, dtype=float)
        arr = arr[np.isfinite(arr)]
        if len(arr) == 0:
            return 0.0
        return float(np.mean(arr >= self.threshold))

    def stats(self) -> dict[str, int | float]:
        return {
            "score_history_len": len(self._scores),
            "frozen": int(self._frozen),
            "freeze_reason": self._freeze_reason,
            "updates_applied": self._updates_applied,
            "skipped_drift": self._skipped_drift,
            "skipped_cooldown": self._skipped_cooldown,
            "skipped_history": self._skipped_history,
            "step_limited": self._step_limited,
            "bounds_limited": self._bounds_limited,
            "feedback_blocked": self._feedback_blocked,
            "min_history": int(self.cfg.min_history),
        }

    def freeze(self, reason: str) -> None:
        # Why: circuit breaker and admin controls need an explicit adaptation stop.
        self._frozen = True
        self._freeze_reason = str(reason)

    def unfreeze(self) -> None:
        self._frozen = False
        self._freeze_reason = ""

    @property
    def is_frozen(self) -> bool:
        return self._frozen

    def snapshot_state(self) -> dict[str, object]:
        return {
            "threshold": float(self.threshold),
            "scores": list(self._scores),
            "last_change_ts": float(self._last_change_ts),
            "frozen": bool(self._frozen),
            "freeze_reason": self._freeze_reason,
            "stats": self.stats(),
        }

    def load_snapshot_state(self, state: dict[str, object]) -> None:
        self.threshold = float(state.get("threshold", self.threshold))
        self._scores.clear()
        for s in state.get("scores", []):  # type: ignore[union-attr]
            if np.isfinite(float(s)):
                self._scores.append(float(s))
        self._last_change_ts = float(state.get("last_change_ts", 0.0))
        if bool(state.get("frozen", False)):
            self.freeze(str(state.get("freeze_reason", "restored")))
        else:
            self.unfreeze()
