# Drift strategy

## What we detect
We detect **signals** of drift, not “truth.” Drift is inherently ambiguous.

### Data drift (features)
- KS-test p-value vs threshold
- PSI vs threshold

We compute these between:
- **Reference window** (stable baseline distribution; built at startup)
- **Current window** (sliding buffer)

### Prediction drift (scores)
Same KS/PSI checks but applied to normalized scores, plus:

- **ADWIN** streaming detector for fast mean-shift detection

## What we do when drift is detected
- Emit `drift_active=1` metrics
- Include drift state in alert payloads
- **Freeze threshold adaptation** while drift is active (guardrail)

## What we do NOT do automatically
- No automatic retraining by default.
- No automatic reference refresh by default.

Those are operational decisions requiring human review, rollback plans, and post-incident analysis.

## Reality check (mandatory)
- Drift cannot be “solved,” only detected/managed.
- Detectors have false positives and false negatives.
- Automatic adaptation can hide real incidents if misused.
- Human review is required before retraining decisions.
