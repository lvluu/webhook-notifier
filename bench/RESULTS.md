# Benchmark Results

| Dashboard category | Benchmark | Report | Status |
|---|---|---|---|
| Reliability | `reliability` | [reliability_stress.md](results/reliability_stress.md) | Stress profile **PASS** — all gates OK |
| Fairness | `fairness` | [fairness_stress.md](results/fairness_stress.md) | Stress profile PASS |
| Scalability | `scalability` | [scalability_stress.md](results/scalability_stress.md) | Stress profile PASS |
| Partner Protection | `partner-protection` | [partner-protection_stress.md](results/partner-protection_stress.md) | Stress profile **PASS** — all gates OK |
| Configuration Tuning | `tuning` | [tuning_stress.md](results/tuning_stress.md) | Stress profile PASS; [dev](results/tuning_dev.md) also available |
| Scheduler Comparison | `scheduler-compare` | [scheduler_compare_stress.md](results/scheduler_compare_stress.md) | RR vs SWRR with MPC tuning (8/16/32); SWRR wins at 32w+ (mpc=8 sweet spot at 32w, mpc=16 at 64w); RR shows high variance at 64w |

Reports use the `<scenario>_<profile>.md` convention so dev and stress runs can
coexist. The listed files are the latest committed evidence for each category.
