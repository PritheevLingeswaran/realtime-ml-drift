from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy.stats import ks_2samp


@dataclass(frozen=True)
class DriftStat:
    kind: str  # "ks" | "psi"
    value: float
    threshold: float
    triggered: bool


def ks_test(ref: np.ndarray, cur: np.ndarray, pvalue_threshold: float) -> DriftStat:
    if len(ref) < 2 or len(cur) < 2:
        return DriftStat(kind="ks", value=1.0, threshold=pvalue_threshold, triggered=False)
    res = ks_2samp(ref, cur, alternative="two-sided", mode="auto")
    p = float(res.pvalue)
    triggered = p < pvalue_threshold
    # We store p-value as value; lower => worse. Threshold is pvalue threshold.
    return DriftStat(kind="ks", value=p, threshold=pvalue_threshold, triggered=triggered)


def psi(ref: np.ndarray, cur: np.ndarray, bins: int = 10) -> float:
    """Population Stability Index over histograms (reference vs current)."""
    ref = np.asarray(ref, dtype=float)
    cur = np.asarray(cur, dtype=float)

    # Use reference quantiles to define bins (common in prod)
    quantiles = np.quantile(ref, np.linspace(0.0, 1.0, bins + 1))
    quantiles[0] = min(quantiles[0], ref.min(), cur.min()) - 1e-9
    quantiles[-1] = max(quantiles[-1], ref.max(), cur.max()) + 1e-9

    ref_hist, _ = np.histogram(ref, bins=quantiles)
    cur_hist, _ = np.histogram(cur, bins=quantiles)

    ref_perc = ref_hist / max(1, ref_hist.sum())
    cur_perc = cur_hist / max(1, cur_hist.sum())

    # Avoid division by zero
    ref_perc = np.clip(ref_perc, 1e-6, 1.0)
    cur_perc = np.clip(cur_perc, 1e-6, 1.0)

    return float(np.sum((cur_perc - ref_perc) * np.log(cur_perc / ref_perc)))


def psi_stat(ref: np.ndarray, cur: np.ndarray, threshold: float, bins: int = 10) -> DriftStat:
    v = psi(ref, cur, bins=bins)
    return DriftStat(kind="psi", value=v, threshold=threshold, triggered=v >= threshold)
