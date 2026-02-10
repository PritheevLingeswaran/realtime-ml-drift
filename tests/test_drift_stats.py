from __future__ import annotations

import numpy as np

from src.drift.stats import psi, psi_stat


def test_psi_increases_on_shift() -> None:
    ref = np.random.normal(0, 1, size=2000)
    cur = np.random.normal(2, 1, size=2000)
    v = psi(ref, cur, bins=10)
    assert v > 0.1
    stat = psi_stat(ref, cur, threshold=0.2, bins=10)
    assert stat.triggered == (v >= 0.2)
