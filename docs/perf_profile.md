# Performance Profile

## Baseline

| Component | ms/event | % total |
|---|---:|---:|
| `feature_engineering` | 0.0064 | 0.39% |
| `scoring` | 1.0025 | 60.73% |
| `drift_adwin` | 0.0021 | 0.12% |
| `drift_ks_psi` | 0.0053 | 0.32% |
| `adaptation` | 0.0009 | 0.05% |
| `snapshotting` | 0.0010 | 0.06% |
| `logging_metrics` | 0.6325 | 38.31% |

## Tuned

| Component | ms/event | % total |
|---|---:|---:|
| `feature_engineering` | 0.0064 | 0.39% |
| `scoring` | 0.9994 | 60.53% |
| `drift_adwin` | 0.0021 | 0.13% |
| `drift_ks_psi` | 0.0053 | 0.32% |
| `adaptation` | 0.0048 | 0.29% |
| `snapshotting` | 0.0016 | 0.10% |
| `logging_metrics` | 0.6317 | 38.26% |

## Top Regressions

| Component | Baseline ms/event | Tuned ms/event | Delta ms/event | Delta % |
|---|---:|---:|---:|---:|
| `adaptation` | 0.0009 | 0.0048 | 0.0039 | 429.46% |
| `snapshotting` | 0.0010 | 0.0016 | 0.0005 | 50.52% |
| `drift_ks_psi` | 0.0053 | 0.0053 | 0.0000 | 0.30% |