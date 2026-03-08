from __future__ import annotations

import time
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
    check_interval_events: int
    periodic_expensive_checks_enabled: bool
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
    adaptive_score_quantile: float
    feature_alert_score_threshold: float
    norm_cap: float
    warning_enter_mult: float
    warning_exit_mult: float
    critical_enter_mult: float
    critical_exit_mult: float
    warning_vote_fraction: float
    critical_vote_fraction: float
    warning_consecutive: int
    critical_consecutive: int
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
    psi_component: float = 0.0
    ks_component: float = 0.0
    prediction_component: float = 0.0
    vote_ratio: float = 0.0
    last_update_ts: float = 0.0
    feature_stats: dict[str, list[DriftStat]] = field(default_factory=dict)
    score_stats: list[DriftStat] = field(default_factory=list)
    adwin_detected: bool = False
    adwin_time_ms: float = 0.0
    expensive_checks_time_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "drift_active": self.drift_active,
            "drift_fixed_active": self.drift_fixed_active,
            "drift_warning_active": self.drift_warning_active,
            "drift_evaluated": self.drift_evaluated,
            "drift_score": self.drift_score,
            "drift_threshold": self.drift_threshold,
            "psi_component": self.psi_component,
            "ks_component": self.ks_component,
            "prediction_component": self.prediction_component,
            "vote_ratio": self.vote_ratio,
            "feature_stats": {k: [s.__dict__ for s in v] for k, v in self.feature_stats.items()},
            "score_stats": [s.__dict__ for s in self.score_stats],
            "adwin_detected": self.adwin_detected,
            "adwin_time_ms": self.adwin_time_ms,
            "expensive_checks_time_ms": self.expensive_checks_time_ms,
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
        self._warning_streak = 0
        self._critical_streak = 0
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

        # ADWIN update is cheap and can run on every event even when expensive
        # distribution checks are periodic.
        adwin_t0 = time.perf_counter()
        if self._adwin is not None:
            self._state.adwin_detected = bool(self._adwin.update(float(score)))
        else:
            self._state.adwin_detected = False
        self._state.adwin_time_ms = (time.perf_counter() - adwin_t0) * 1000.0
        self._state.expensive_checks_time_ms = 0.0

        if len(self._cur_scores) < self.cfg.min_samples:
            self._state.drift_active = False
            self._state.drift_fixed_active = False
            self._state.drift_warning_active = False
            self._state.drift_evaluated = False
            return self._state

        # CPU guard:
        # - legacy mode uses evaluation_interval
        # - periodic mode uses check_interval_events for expensive KS/PSI only
        interval = (
            int(self.cfg.check_interval_events)
            if bool(self.cfg.periodic_expensive_checks_enabled)
            else int(self.cfg.evaluation_interval)
        )
        if (self._eval_tick % max(1, interval)) != 0:
            self._state.drift_active = False
            self._state.drift_fixed_active = False
            self._state.drift_warning_active = False
            self._state.drift_evaluated = False
            return self._state

        expensive_t0 = time.perf_counter()
        self._state.drift_evaluated = True
        self._state.feature_stats = {}
        triggered_feature_count = 0
        psi_norm_vals: list[float] = []
        ks_norm_vals: list[float] = []
        feature_fusion_vals: list[float] = []

        for k in self.feature_names:
            cur = np.asarray(self._cur_feats[k], dtype=float)
            if len(cur) < self.cfg.min_samples:
                continue

            psi_ref = self._ref_psi[k]
            psi_value = psi_with_reference(psi_ref, cur)
            psi_thr = float(self._feat_psi_thr.get(k, self.cfg.feature_psi))
            psi_ratio = psi_value / max(1e-9, psi_thr)
            psi_norm = min(float(np.log1p(max(0.0, psi_ratio))), float(self.cfg.norm_cap))
            psi_trigger = psi_ratio >= 1.0
            psi_norm_vals.append(psi_norm)

            ks_value = ks_hist_stat(psi_ref, cur)
            ks_crit = ks_critical_value(self._ref_n_feats[k], len(cur), self.cfg.feature_ks_p)
            ks_ratio = ks_value / max(1e-9, ks_crit)
            ks_ratio_thr = float(self._feat_ks_ratio_thr.get(k, 1.0))
            ks_ratio_norm = ks_ratio / max(1e-9, ks_ratio_thr)
            ks_norm = min(float(np.log1p(max(0.0, ks_ratio_norm))), float(self.cfg.norm_cap))
            ks_trigger = ks_ratio_norm >= 1.0
            ks_norm_vals.append(ks_norm)
            ref_mean = self._ref_mean_feats[k]
            ref_std = self._ref_std_feats[k]
            mean_shift_z = abs(float(np.mean(cur)) - ref_mean) / max(1e-9, ref_std)
            mean_shift_norm = min(
                float(
                    np.log1p(
                        max(0.0, mean_shift_z / max(1e-9, float(self.cfg.mean_shift_z_threshold)))
                    )
                ),
                float(self.cfg.norm_cap),
            )

            stats = [
                DriftStat(kind="ks", value=ks_value, threshold=ks_crit, triggered=ks_trigger),
                DriftStat(
                    kind="psi",
                    value=psi_value,
                    threshold=float(psi_thr),
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
            feature_fusion = (0.55 * psi_norm) + (0.30 * ks_norm) + (0.15 * mean_shift_norm)
            feature_fusion_vals.append(feature_fusion)
            if feature_fusion >= float(self.cfg.feature_alert_score_threshold):
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
        pred_psi_norm = min(
            float(np.log1p(max(0.0, score_psi / max(1e-9, self.cfg.pred_psi)))),
            float(self.cfg.norm_cap),
        )
        pred_ks_norm = min(
            float(np.log1p(max(0.0, score_ks / max(1e-9, score_ks_crit)))),
            float(self.cfg.norm_cap),
        )
        pred_shift_norm = 0.5 * pred_psi_norm + 0.5 * pred_ks_norm

        feat_count = max(1, len(self.feature_names))
        vote_ratio = triggered_feature_count / feat_count
        mean_psi_norm = float(np.mean(psi_norm_vals)) if psi_norm_vals else 0.0
        mean_ks_norm = float(np.mean(ks_norm_vals)) if ks_norm_vals else 0.0
        mean_feature_fusion = float(np.mean(feature_fusion_vals)) if feature_fusion_vals else 0.0

        psi_component = float(self.cfg.score_weight_psi) * mean_psi_norm
        ks_component = float(self.cfg.score_weight_ks) * mean_ks_norm
        prediction_component = float(self.cfg.score_weight_pred) * pred_shift_norm
        drift_score = (
            psi_component
            + ks_component
            + prediction_component
        )

        if self.cfg.threshold_method == "adaptive" and len(self._drift_score_history) >= self.cfg.baseline_min_evals:
            hist = np.asarray(self._drift_score_history, dtype=float)
            q = float(np.quantile(hist, float(self.cfg.adaptive_score_quantile)))
            drift_threshold = max(float(self.cfg.fixed_score_threshold), q)
        else:
            drift_threshold = float(self.cfg.fixed_score_threshold)

        drift_fixed_active = (
            drift_score >= float(self.cfg.fixed_score_threshold)
            and vote_ratio >= float(self.cfg.critical_vote_fraction)
        )
        warning_enter = drift_threshold * float(self.cfg.warning_enter_mult)
        warning_exit = drift_threshold * float(self.cfg.warning_exit_mult)
        critical_enter = drift_threshold * float(self.cfg.critical_enter_mult)
        critical_exit = drift_threshold * float(self.cfg.critical_exit_mult)
        warning_vote = float(self.cfg.warning_vote_fraction)
        critical_vote = float(self.cfg.critical_vote_fraction)

        warning_raw = False
        if self._state.drift_warning_active:
            warning_raw = drift_score >= warning_exit and vote_ratio >= max(0.0, warning_vote * 0.8)
        else:
            warning_raw = (
                drift_score >= warning_enter
                and vote_ratio >= warning_vote
                and mean_feature_fusion >= float(self.cfg.feature_alert_score_threshold) * 0.9
            )
        if self._state.adwin_detected and vote_ratio >= max(0.0, warning_vote * 0.8):
            warning_raw = True

        if warning_raw:
            self._warning_streak += 1
        else:
            self._warning_streak = 0
        drift_warning_active = self._warning_streak >= max(1, int(self.cfg.warning_consecutive))

        critical_raw = False
        if self._state.drift_active:
            critical_raw = drift_score >= critical_exit and vote_ratio >= max(0.0, critical_vote * 0.8)
        else:
            critical_raw = (
                drift_warning_active
                and drift_score >= critical_enter
                and vote_ratio >= critical_vote
                and mean_feature_fusion >= float(self.cfg.feature_alert_score_threshold)
            )

        if critical_raw:
            self._critical_streak += 1
        else:
            self._critical_streak = 0

        smoothed = self._critical_streak >= max(1, int(self.cfg.critical_consecutive))
        cooldown_block = (self._eval_tick - self._last_alert_eval) < max(
            0, int(self.cfg.alert_cooldown_events)
        )
        if self._state.drift_active and critical_raw:
            drift_active = True
        elif smoothed and not cooldown_block:
            drift_active = True
            self._last_alert_eval = self._eval_tick
        else:
            drift_active = False

        calibration_candidate = (
            vote_ratio <= (warning_vote * 0.8) and not self._state.drift_active
        ) or (len(self._drift_score_history) < max(50, self.cfg.baseline_min_evals * 3))
        if calibration_candidate:
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
        self._state.psi_component = float(psi_component)
        self._state.ks_component = float(ks_component)
        self._state.prediction_component = float(prediction_component)
        self._state.drift_fixed_active = bool(drift_fixed_active)
        self._state.drift_warning_active = bool(drift_warning_active)
        self._state.drift_active = bool(drift_active)
        self._state.expensive_checks_time_ms = (time.perf_counter() - expensive_t0) * 1000.0
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
                    float(
                        np.quantile(arr, float(self.cfg.adaptive_score_quantile))
                        + float(self.cfg.feature_threshold_k) * np.std(arr)
                    ),
                )

            ks_hist = self._feat_ks_ratio_hist[k]
            if len(ks_hist) >= self.cfg.baseline_min_evals:
                arr = np.asarray(ks_hist, dtype=float)
                self._feat_ks_ratio_thr[k] = max(
                    1.0,
                    float(
                        np.quantile(arr, float(self.cfg.adaptive_score_quantile))
                        + float(self.cfg.feature_threshold_k) * np.std(arr)
                    ),
                )

    def snapshot_state(self) -> dict[str, object]:
        # Why: restart-safe operation requires preserving reference/current drift context.
        return {
            "eval_tick": int(self._eval_tick),
            "warning_streak": int(self._warning_streak),
            "critical_streak": int(self._critical_streak),
            "last_alert_eval": int(self._last_alert_eval),
            "drift_score_history": list(self._drift_score_history),
            "ref_feats": {k: list(v) for k, v in self._ref_feats.items()},
            "cur_feats": {k: list(v) for k, v in self._cur_feats.items()},
            "ref_scores": list(self._ref_scores),
            "cur_scores": list(self._cur_scores),
            "feat_psi_hist": {k: list(v) for k, v in self._feat_psi_hist.items()},
            "feat_ks_ratio_hist": {k: list(v) for k, v in self._feat_ks_ratio_hist.items()},
            "feat_psi_thr": dict(self._feat_psi_thr),
            "feat_ks_ratio_thr": dict(self._feat_ks_ratio_thr),
            "state": self._state.to_dict(),
        }

    def load_snapshot_state(self, payload: dict[str, object]) -> None:
        self._eval_tick = int(payload.get("eval_tick", 0))
        self._warning_streak = int(payload.get("warning_streak", 0))
        self._critical_streak = int(payload.get("critical_streak", 0))
        self._last_alert_eval = int(payload.get("last_alert_eval", -10**9))

        self._drift_score_history.clear()
        for x in payload.get("drift_score_history", []):  # type: ignore[union-attr]
            self._drift_score_history.append(float(x))

        for k in self.feature_names:
            self._ref_feats[k] = [float(x) for x in payload.get("ref_feats", {}).get(k, [])]  # type: ignore[union-attr]
            self._cur_feats[k].clear()
            for x in payload.get("cur_feats", {}).get(k, []):  # type: ignore[union-attr]
                self._cur_feats[k].append(float(x))

            self._feat_psi_hist[k].clear()
            for x in payload.get("feat_psi_hist", {}).get(k, []):  # type: ignore[union-attr]
                self._feat_psi_hist[k].append(float(x))
            self._feat_ks_ratio_hist[k].clear()
            for x in payload.get("feat_ks_ratio_hist", {}).get(k, []):  # type: ignore[union-attr]
                self._feat_ks_ratio_hist[k].append(float(x))

            self._feat_psi_thr[k] = float(payload.get("feat_psi_thr", {}).get(k, self.cfg.feature_psi))  # type: ignore[union-attr]
            self._feat_ks_ratio_thr[k] = float(payload.get("feat_ks_ratio_thr", {}).get(k, 1.0))  # type: ignore[union-attr]

        self._ref_scores = [float(x) for x in payload.get("ref_scores", [])]  # type: ignore[union-attr]
        self._cur_scores.clear()
        for x in payload.get("cur_scores", []):  # type: ignore[union-attr]
            self._cur_scores.append(float(x))

        if len(self._ref_scores) >= self.cfg.min_samples:
            self._build_reference_cache()

        state_payload = payload.get("state", {})
        self._state.drift_active = bool(state_payload.get("drift_active", False))  # type: ignore[union-attr]
        self._state.drift_fixed_active = bool(state_payload.get("drift_fixed_active", False))  # type: ignore[union-attr]
        self._state.drift_warning_active = bool(state_payload.get("drift_warning_active", False))  # type: ignore[union-attr]
        self._state.drift_evaluated = bool(state_payload.get("drift_evaluated", False))  # type: ignore[union-attr]
        self._state.drift_score = float(state_payload.get("drift_score", 0.0))  # type: ignore[union-attr]
        self._state.drift_threshold = float(state_payload.get("drift_threshold", 0.0))  # type: ignore[union-attr]
        self._state.last_update_ts = float(state_payload.get("last_update_ts", 0.0))  # type: ignore[union-attr]
