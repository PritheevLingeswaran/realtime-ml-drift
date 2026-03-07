from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from src.drift.adwin import ADWINDetector
from src.drift.stats import (
    DriftStat,
    PsiReference,
    build_psi_reference,
    ks_critical_value,
    ks_hist_stat,
    psi_with_reference,
)


@dataclass
class DriftConfig:
    reference_window_events: int
    current_window_events: int
    window_size: int
    evaluation_interval: int
    min_samples: int
    feature_ks_p: float
    feature_psi: float
    pred_ks_p: float
    pred_psi: float
    threshold_method: str
    threshold_k: float
    fixed_score_threshold: float
    feature_vote_fraction: float
    smoothing_consecutive: int
    alert_cooldown_events: int
    score_weight_psi: float
    score_weight_ks: float
    score_weight_pred: float
    baseline_min_evals: int
    mean_shift_z_threshold: float
    feature_threshold_k: float
    adwin_enabled: bool
    adwin_delta: float


@dataclass
class DriftState:
    drift_active: bool = False
    drift_fixed_active: bool = False
    drift_warning_active: bool = False
    drift_evaluated: bool = False
    drift_score: float = 0.0
    drift_threshold: float = 0.0
    vote_ratio: float = 0.0
    last_update_ts: float = 0.0
    feature_stats: dict[str, list[DriftStat]] = field(default_factory=dict)
    score_stats: list[DriftStat] = field(default_factory=list)
    adwin_detected: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "drift_active": self.drift_active,
            "drift_fixed_active": self.drift_fixed_active,
            "drift_warning_active": self.drift_warning_active,
            "drift_evaluated": self.drift_evaluated,
            "drift_score": self.drift_score,
            "drift_threshold": self.drift_threshold,
            "vote_ratio": self.vote_ratio,
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
        self._eval_tick = 0
        self._consecutive_raw = 0
        self._last_alert_eval = -10**9
        self._drift_score_history: deque[float] = deque(maxlen=5000)
        self._ref_feats: dict[str, list[float]] = {k: [] for k in feature_names}
        self._cur_feats: dict[str, deque[float]] = {
            k: deque(maxlen=cfg.window_size) for k in feature_names
        }
        self._ref_scores: list[float] = []
        self._cur_scores: deque[float] = deque(maxlen=cfg.window_size)
        self._ref_psi: dict[str, PsiReference] = {}
        self._ref_sorted_feats: dict[str, np.ndarray] = {}
        self._ref_n_feats: dict[str, int] = {}
        self._ref_mean_feats: dict[str, float] = {}
        self._ref_std_feats: dict[str, float] = {}
        self._feat_psi_hist: dict[str, deque[float]] = {k: deque(maxlen=5000) for k in feature_names}
        self._feat_ks_ratio_hist: dict[str, deque[float]] = {k: deque(maxlen=5000) for k in feature_names}
        self._feat_psi_thr: dict[str, float] = {k: float(cfg.feature_psi) for k in feature_names}
        self._feat_ks_ratio_thr: dict[str, float] = {k: 1.0 for k in feature_names}
        self._ref_psi_score: PsiReference | None = None
        self._ref_sorted_scores: np.ndarray | None = None
        self._ref_n_scores: int = 0
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
            if len(self._ref_scores) == self.cfg.reference_window_events:
                self._build_reference_cache()
            self._state.last_update_ts = ts
            self._state.drift_active = False
            self._state.drift_fixed_active = False
            self._state.drift_warning_active = False
            self._state.drift_evaluated = False
            return self._state

        # Then accumulate current window.
        for k in self.feature_names:
            self._cur_feats[k].append(float(feats.get(k, 0.0)))
        self._cur_scores.append(float(score))

        self._eval_tick += 1
        self._state.last_update_ts = ts

        if len(self._cur_scores) < self.cfg.min_samples:
            self._state.drift_active = False
            self._state.drift_fixed_active = False
            self._state.drift_warning_active = False
            self._state.drift_evaluated = False
            return self._state

        # CPU guard: evaluate every N events rather than every event.
        if (self._eval_tick % max(1, self.cfg.evaluation_interval)) != 0:
            self._state.drift_active = False
            self._state.drift_fixed_active = False
            self._state.drift_warning_active = False
            self._state.drift_evaluated = False
            return self._state

        self._state.drift_evaluated = True
        self._state.feature_stats = {}
        triggered_feature_count = 0
        psi_norm_vals: list[float] = []
        ks_norm_vals: list[float] = []

        for k in self.feature_names:
            cur = np.asarray(self._cur_feats[k], dtype=float)
            if len(cur) < self.cfg.min_samples:
                continue

            psi_ref = self._ref_psi[k]
            psi_value = psi_with_reference(psi_ref, cur)
            psi_thr = float(self._feat_psi_thr.get(k, self.cfg.feature_psi))
            psi_trigger = psi_value >= psi_thr
            psi_norm_vals.append(psi_value / max(1e-9, psi_thr))

            ks_value = ks_hist_stat(psi_ref, cur)
            ks_crit = ks_critical_value(self._ref_n_feats[k], len(cur), self.cfg.feature_ks_p)
            ks_ratio = ks_value / max(1e-9, ks_crit)
            ks_ratio_thr = float(self._feat_ks_ratio_thr.get(k, 1.0))
            ks_trigger = ks_ratio >= ks_ratio_thr
            ks_norm_vals.append(ks_ratio / max(1e-9, ks_ratio_thr))
            ref_mean = self._ref_mean_feats[k]
            ref_std = self._ref_std_feats[k]
            mean_shift_z = abs(float(np.mean(cur)) - ref_mean) / max(1e-9, ref_std)

            stats = [
                DriftStat(kind="ks", value=ks_value, threshold=ks_crit, triggered=ks_trigger),
                DriftStat(
                    kind="psi",
                    value=psi_value,
                    threshold=float(self.cfg.feature_psi),
                    triggered=psi_trigger,
                ),
                DriftStat(
                    kind="mean_shift_z",
                    value=mean_shift_z,
                    threshold=float(self.cfg.mean_shift_z_threshold),
                    triggered=mean_shift_z >= float(self.cfg.mean_shift_z_threshold),
                ),
            ]
            self._state.feature_stats[k] = stats
            # Voting is stricter than score accumulation to reduce noisy false positives.
            if (psi_trigger and ks_trigger) or (mean_shift_z >= float(self.cfg.mean_shift_z_threshold)):
                triggered_feature_count += 1

        # Prediction/score drift.
        cur_s = np.asarray(self._cur_scores, dtype=float)
        assert self._ref_sorted_scores is not None
        assert self._ref_psi_score is not None
        score_psi = psi_with_reference(self._ref_psi_score, cur_s)
        score_psi_trigger = score_psi >= self.cfg.pred_psi
        score_ks = ks_hist_stat(self._ref_psi_score, cur_s)
        score_ks_crit = ks_critical_value(self._ref_n_scores, len(cur_s), self.cfg.pred_ks_p)
        score_ks_trigger = score_ks >= score_ks_crit
        self._state.score_stats = [
            DriftStat(kind="ks", value=score_ks, threshold=score_ks_crit, triggered=score_ks_trigger),
            DriftStat(
                kind="psi",
                value=score_psi,
                threshold=float(self.cfg.pred_psi),
                triggered=score_psi_trigger,
            ),
        ]
        pred_shift_norm = max(
            score_psi / max(1e-9, self.cfg.pred_psi), score_ks / max(1e-9, score_ks_crit)
        )

        # Streaming mean shift detector on scores (fast early warning).
        if self._adwin is not None:
            self._state.adwin_detected = bool(self._adwin.update(float(score)))
        else:
            self._state.adwin_detected = False

        feat_count = max(1, len(self.feature_names))
        vote_ratio = triggered_feature_count / feat_count
        mean_psi_norm = float(np.mean(psi_norm_vals)) if psi_norm_vals else 0.0
        mean_ks_norm = float(np.mean(ks_norm_vals)) if ks_norm_vals else 0.0

        drift_score = (
            float(self.cfg.score_weight_psi) * mean_psi_norm
            + float(self.cfg.score_weight_ks) * mean_ks_norm
            + float(self.cfg.score_weight_pred) * pred_shift_norm
        )

        if self.cfg.threshold_method == "adaptive" and len(self._drift_score_history) >= self.cfg.baseline_min_evals:
            hist = np.asarray(self._drift_score_history, dtype=float)
            drift_threshold = float(np.mean(hist) + float(self.cfg.threshold_k) * np.std(hist))
        else:
            drift_threshold = float(self.cfg.fixed_score_threshold)

        drift_fixed_active = (
            drift_score >= float(self.cfg.fixed_score_threshold)
            and vote_ratio >= float(self.cfg.feature_vote_fraction)
        )
        raw_adaptive = (
            drift_score >= drift_threshold and vote_ratio >= float(self.cfg.feature_vote_fraction)
        )
        if self._state.adwin_detected and vote_ratio >= (float(self.cfg.feature_vote_fraction) / 2.0):
            raw_adaptive = True

        if raw_adaptive:
            self._consecutive_raw += 1
        else:
            self._consecutive_raw = 0

        smoothed = self._consecutive_raw >= max(1, int(self.cfg.smoothing_consecutive))
        cooldown_block = (self._eval_tick - self._last_alert_eval) < max(
            0, int(self.cfg.alert_cooldown_events)
        )
        drift_warning_active = bool(raw_adaptive)
        drift_active = smoothed and not cooldown_block
        if drift_active:
            self._last_alert_eval = self._eval_tick

        if not raw_adaptive:
            self._drift_score_history.append(float(drift_score))
            for k in self.feature_names:
                st = self._state.feature_stats.get(k, [])
                if len(st) >= 2:
                    psi_val = st[1].value
                    ks_ratio_val = st[0].value / max(1e-9, st[0].threshold)
                    self._feat_psi_hist[k].append(float(psi_val))
                    self._feat_ks_ratio_hist[k].append(float(ks_ratio_val))
            self._update_feature_adaptive_thresholds()

        self._state.vote_ratio = float(vote_ratio)
        self._state.drift_score = float(drift_score)
        self._state.drift_threshold = float(drift_threshold)
        self._state.drift_fixed_active = bool(drift_fixed_active)
        self._state.drift_warning_active = bool(drift_warning_active)
        self._state.drift_active = bool(drift_active)
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
            self._ref_feats[k] = list(self._cur_feats[k])[-self.cfg.reference_window_events :]
        self._ref_scores = list(self._cur_scores)[-self.cfg.reference_window_events :]
        self._build_reference_cache()

    def _build_reference_cache(self) -> None:
        for k in self.feature_names:
            ref = np.asarray(self._ref_feats[k], dtype=float)
            self._ref_psi[k] = build_psi_reference(ref, bins=10)
            self._ref_sorted_feats[k] = np.sort(ref)
            self._ref_n_feats[k] = int(len(ref))
            self._ref_mean_feats[k] = float(np.mean(ref))
            self._ref_std_feats[k] = float(np.std(ref) + 1e-9)
        ref_s = np.asarray(self._ref_scores, dtype=float)
        self._ref_psi_score = build_psi_reference(ref_s, bins=10)
        self._ref_sorted_scores = np.sort(ref_s)
        self._ref_n_scores = int(len(ref_s))

    def _update_feature_adaptive_thresholds(self) -> None:
        for k in self.feature_names:
            psi_hist = self._feat_psi_hist[k]
            if len(psi_hist) >= self.cfg.baseline_min_evals:
                arr = np.asarray(psi_hist, dtype=float)
                self._feat_psi_thr[k] = max(
                    float(self.cfg.feature_psi),
                    float(np.mean(arr) + float(self.cfg.feature_threshold_k) * np.std(arr)),
                )

            ks_hist = self._feat_ks_ratio_hist[k]
            if len(ks_hist) >= self.cfg.baseline_min_evals:
                arr = np.asarray(ks_hist, dtype=float)
                self._feat_ks_ratio_thr[k] = max(
                    1.0,
                    float(np.mean(arr) + float(self.cfg.feature_threshold_k) * np.std(arr)),
                )
