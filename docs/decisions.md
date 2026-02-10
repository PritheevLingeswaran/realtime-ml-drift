# Decisions

## Why Isolation Forest?
- Common unsupervised baseline for anomaly scoring in production.
- Stable scoring if you avoid frequent retraining.
- Works with tabular windowed features.

## Why windowed per-entity features?
- Many real incidents are *behavioral* deviations per entity (account/device).
- Windowed stats are incremental and streaming-friendly.

## Why KS + PSI + ADWIN?
- KS and PSI are standard for distribution shift; complementary failure modes.
- ADWIN provides streaming early-warning on mean shifts.
- Ensemble of signals reduces dependence on a single detector.

## Why guarded adaptation instead of auto-retrain?
- Auto-retrain is risky without labels/ground truth.
- Threshold tuning is easier to audit and roll back.
- Guardrails prevent runaway alert suppression.
