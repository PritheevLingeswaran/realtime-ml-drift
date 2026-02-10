from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from src.drift.adwin import ADWINDetector
from src.drift.stats import DriftStat, ks_test, psi_stat


@dataclass
class DriftConfig:
    reference_window_events: int
    current_window_events: int
    min_samples: int
    feature_ks_p: float
    feature_psi: float
    pred_ks_p: float
    pred_psi: float
    adwin_enabled: bool
    adwin_delta: float


@dataclass
class DriftState:
    drift_active: bool = False
    last_update_ts: float = 0.0
    feature_stats: dict[str, list[DriftStat]] = field(default_factory=dict)
    score_stats: list[DriftStat] = field(default_factory=list)
    adwin_detected: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "drift_active": self.drift_active,
            "feature_stats": {k: [s.__dict__ for s in v] for k, v in self.feature_stats.items()},
            "score_stats": [s.__dict__ for s in self.score_stats],
            "adwin_detected": self.adwin_detected,
        }


class DriftMonitor:
    """Maintains reference + current windows and runs drift checks.

    Production behavior:
    - Reference is held stable until you decide to refresh it (human-reviewed).
    - Current slides forward; drift compares current vs reference.
    - Multiple detectors are ensemble-like signals, not ground truth.
    """

    def __init__(self, cfg: DriftConfig, feature_names: list[str]) -> None:
        self.cfg = cfg
        self.feature_names = feature_names
        self._ref_feats: dict[str, list[float]] = {k: [] for k in feature_names}
        self._cur_feats: dict[str, list[float]] = {k: [] for k in feature_names}
        self._ref_scores: list[float] = []
        self._cur_scores: list[float] = []
        self._state = DriftState()
        self._adwin = ADWINDetector(delta=cfg.adwin_delta) if cfg.adwin_enabled else None

    @property
    def state(self) -> DriftState:
        return self._state

    def update(self, feats: dict[str, float], score: float, ts: float) -> DriftState:
        # Fill reference first (cold start)
        if len(self._ref_scores) < self.cfg.reference_window_events:
            for k in self.feature_names:
                self._ref_feats[k].append(float(feats.get(k, 0.0)))
            self._ref_scores.append(float(score))
            self._state.last_update_ts = ts
            self._state.drift_active = False
            return self._state

        # Then accumulate current window (sliding buffer with max size)
        for k in self.feature_names:
            self._cur_feats[k].append(float(feats.get(k, 0.0)))
            if len(self._cur_feats[k]) > self.cfg.current_window_events:
                self._cur_feats[k].pop(0)
        self._cur_scores.append(float(score))
        if len(self._cur_scores) > self.cfg.current_window_events:
            self._cur_scores.pop(0)

        self._state.last_update_ts = ts

        if len(self._cur_scores) < self.cfg.min_samples:
            self._state.drift_active = False
            return self._state

        self._state.feature_stats = {}
        triggered_any = False

        for k in self.feature_names:
            ref = np.array(self._ref_feats[k], dtype=float)
            cur = np.array(self._cur_feats[k], dtype=float)
            stats = [
                ks_test(ref, cur, pvalue_threshold=self.cfg.feature_ks_p),
                psi_stat(ref, cur, threshold=self.cfg.feature_psi, bins=10),
            ]
            self._state.feature_stats[k] = stats
            triggered_any = triggered_any or any(s.triggered for s in stats)

        # Prediction/score drift
        ref_s = np.array(self._ref_scores, dtype=float)
        cur_s = np.array(self._cur_scores, dtype=float)
        self._state.score_stats = [
            ks_test(ref_s, cur_s, pvalue_threshold=self.cfg.pred_ks_p),
            psi_stat(ref_s, cur_s, threshold=self.cfg.pred_psi, bins=10),
        ]
        triggered_any = triggered_any or any(s.triggered for s in self._state.score_stats)

        # Streaming mean shift detector on scores (fast early warning)
        if self._adwin is not None:
            # Use score directly; could also use rolling mean
            self._state.adwin_detected = bool(self._adwin.update(float(score)))
            triggered_any = triggered_any or self._state.adwin_detected
        else:
            self._state.adwin_detected = False

        self._state.drift_active = bool(triggered_any)
        return self._state

    def refresh_reference(self) -> None:
        """Manual action: refresh reference distribution from current window.

        Why manual:
        - Blindly updating reference can hide incidents (you normalize away real problems).
        - In prod, this is a human-reviewed operation with audit trail.
        """
        if len(self._cur_scores) < self.cfg.min_samples:
            return
        for k in self.feature_names:
            self._ref_feats[k] = list(self._cur_feats[k])
        self._ref_scores = list(self._cur_scores)
