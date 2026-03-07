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


@dataclass(frozen=True)
class PsiReference:
    edges: np.ndarray
    ref_perc: np.ndarray


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


def build_psi_reference(ref: np.ndarray, bins: int = 10) -> PsiReference:
    """Precompute histogram bins/percentages for reuse against many current windows."""
    ref = np.asarray(ref, dtype=float)
    quantiles = np.quantile(ref, np.linspace(0.0, 1.0, bins + 1))
    quantiles[0] = ref.min() - 1e-9
    quantiles[-1] = ref.max() + 1e-9
    ref_hist, _ = np.histogram(ref, bins=quantiles)
    ref_perc = ref_hist / max(1, ref_hist.sum())
    ref_perc = np.clip(ref_perc, 1e-6, 1.0)
    return PsiReference(edges=quantiles, ref_perc=ref_perc)


def psi_with_reference(reference: PsiReference, cur: np.ndarray) -> float:
    cur = np.asarray(cur, dtype=float)
    cur_hist, _ = np.histogram(cur, bins=reference.edges)
    cur_perc = cur_hist / max(1, cur_hist.sum())
    cur_perc = np.clip(cur_perc, 1e-6, 1.0)
    return float(np.sum((cur_perc - reference.ref_perc) * np.log(cur_perc / reference.ref_perc)))


def ks_stat_threshold(ref_sorted: np.ndarray, cur: np.ndarray, pvalue_threshold: float) -> tuple[float, float]:
    """Fast KS statistic with critical value derived from p-value threshold."""
    cur = np.asarray(cur, dtype=float)
    if len(ref_sorted) < 2 or len(cur) < 2:
        return 0.0, float("inf")

    cur_sorted = np.sort(cur)
    n = float(len(ref_sorted))
    m = float(len(cur_sorted))

    cdf_ref = np.searchsorted(ref_sorted, cur_sorted, side="right") / n
    cdf_cur = np.arange(1, len(cur_sorted) + 1, dtype=float) / m
    d_stat = float(np.max(np.abs(cdf_ref - cdf_cur)))

    # Asymptotic KS critical value from alpha ~ pvalue_threshold.
    alpha = max(1e-12, min(0.5, float(pvalue_threshold)))
    c_alpha = np.sqrt(-0.5 * np.log(alpha / 2.0))
    d_crit = float(c_alpha * np.sqrt((n + m) / (n * m)))
    return d_stat, d_crit


def ks_critical_value(n_ref: int, n_cur: int, pvalue_threshold: float) -> float:
    n = float(n_ref)
    m = float(n_cur)
    if n < 2 or m < 2:
        return float("inf")
    alpha = max(1e-12, min(0.5, float(pvalue_threshold)))
    c_alpha = np.sqrt(-0.5 * np.log(alpha / 2.0))
    return float(c_alpha * np.sqrt((n + m) / (n * m)))


def ks_hist_stat(reference: PsiReference, cur: np.ndarray) -> float:
    """Approximate KS D-statistic using histogram CDF bins (faster than sorting every update)."""
    cur = np.asarray(cur, dtype=float)
    cur_hist, _ = np.histogram(cur, bins=reference.edges)
    cur_perc = cur_hist / max(1, cur_hist.sum())
    cdf_ref = np.cumsum(reference.ref_perc)
    cdf_cur = np.cumsum(cur_perc)
    return float(np.max(np.abs(cdf_ref - cdf_cur)))
