# SwanLake TPCH Benchmark Report

This document summarizes the current TPCH benchmark results produced by the CI workflow:

- Workflow: https://github.com/swanlake-io/swanlake/actions/workflows/performance.yml
- Benchmark date in artifacts: 2026-02-21
- Primary comparison in this report: `postgres_local_file` vs `postgres_s3` (both at scale factor `0.1`)
- `postgres_r2` is included as a small-scale sanity run only (scale factor `0.01`)

## TL;DR

At TPCH scale factor `0.1`, `postgres_local_file` is consistently faster than `postgres_s3` in this run:

- `2.14x` higher throughput (`10.428` vs `4.867` req/s)
- `2.14x` lower average latency (`382.751` vs `818.041` ms)
- `2.30x` lower p95 latency (`829.236` vs `1904.023` ms)
- `2.38x` lower p99 latency (`1116.002` vs `2661.619` ms)

## Benchmark Setup (from CI workflow)

Source: `.github/workflows/performance.yml`

- Workflow trigger: `workflow_dispatch`, `pull_request`
- Storage matrix:
  - `postgres_local_file` (`scale_factor=0.1`)
  - `postgres_s3` (`scale_factor=0.1`)
  - `postgres_r2` (`scale_factor=0.01`)
- BenchBase TPCH config:
  - `terminals=4`
  - `warmup=30s`
  - `time=180s`
  - Query mix: TPCH `Q1..Q22`, equal weights
- Cache setting defaults from workflow:
  - `postgres_s3`: `BENCHBASE_ENABLE_CACHE_HTTPFS=true`
  - `postgres_local_file`: `BENCHBASE_ENABLE_CACHE_HTTPFS=false`
  - `postgres_r2`: `BENCHBASE_ENABLE_CACHE_HTTPFS=false`

## Overall Results

| Storage backend | Scale factor | Measured requests | Throughput (req/s) | Goodput (req/s) | Avg latency (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| postgres_local_file | 0.1 | 1877 | 10.428 | 12.022 | 382.751 | 362.547 | 829.236 | 1116.002 | 1212.223 |
| postgres_s3 | 0.1 | 881 | 4.867 | 5.182 | 818.041 | 827.924 | 1904.023 | 2661.619 | 2769.500 |
| postgres_r2 (smoke) | 0.01 | 5323 | 29.572 | 29.661 | 128.972 | 86.332 | 166.285 | 470.461 | 12204.294 |

## Focused Comparison: local_file vs s3 (SF=0.1)

| Metric (SF=0.1 only) | local_file | s3 | Delta / Ratio |
|---|---:|---:|---:|
| Throughput (req/s) | 10.428 | 4.867 | local_file is 2.14x (+114.2%) |
| Measured requests | 1877 | 881 | local_file is 2.13x |
| Average latency (ms) | 382.751 | 818.041 | s3 is 2.14x slower (+113.7%) |
| p50 latency (ms) | 362.547 | 827.924 | s3 is 2.28x slower |
| p95 latency (ms) | 829.236 | 1904.023 | s3 is 2.30x slower |
| p99 latency (ms) | 1116.002 | 2661.619 | s3 is 2.38x slower |

### Quick Visual (higher throughput is better, lower latency is better)

```text
Throughput (req/s)
local_file  10.428 |##############################|
s3           4.867 |##############                |

Average latency (ms)
local_file   382.8 |##############                |
s3           818.0 |##############################|
```

## Query-Level Breakdown (local_file vs s3)

Numbers below are weighted average latencies per query from `results.Q*.csv` using per-interval throughput as weight.

| Query | local_file weighted avg latency (ms) | s3 weighted avg latency (ms) | s3/local ratio |
|---|---:|---:|---:|
| Q1 | 371.154 | 980.897 | 2.64x |
| Q2 | 219.658 | 295.220 | 1.34x |
| Q3 | 366.402 | 850.060 | 2.32x |
| Q4 | 346.003 | 750.536 | 2.17x |
| Q5 | 403.921 | 871.467 | 2.16x |
| Q6 | 325.573 | 576.833 | 1.77x |
| Q7 | 418.827 | 862.287 | 2.06x |
| Q8 | 449.169 | 866.262 | 1.93x |
| Q9 | 446.491 | 1017.710 | 2.28x |
| Q10 | 407.950 | 898.575 | 2.20x |
| Q11 | 177.084 | 273.505 | 1.54x |
| Q12 | 364.693 | 828.099 | 2.27x |
| Q13 | 157.681 | 235.255 | 1.49x |
| Q14 | 309.562 | 644.786 | 2.08x |
| Q15 | 1081.412 | 2621.465 | 2.42x |
| Q16 | 133.001 | 180.317 | 1.36x |
| Q17 | 489.896 | 1079.671 | 2.20x |
| Q18 | 315.088 | 506.105 | 1.61x |
| Q19 | 361.266 | 889.187 | 2.46x |
| Q20 | 388.329 | 693.697 | 1.79x |
| Q21 | 793.802 | 1850.278 | 2.33x |
| Q22 | 165.616 | 237.602 | 1.43x |

Largest latency regressions on S3 in this run:

- Q1: `2.64x` (371.2 ms -> 980.9 ms)
- Q19: `2.46x` (361.3 ms -> 889.2 ms)
- Q15: `2.42x` (1081.4 ms -> 2621.5 ms)
- Q21: `2.33x` (793.8 ms -> 1850.3 ms)
- Q3: `2.32x` (366.4 ms -> 850.1 ms)
- Q9: `2.28x` (446.5 ms -> 1017.7 ms)

## Notes on R2 Result

`postgres_r2` ran with scale factor `0.01`, so it is not directly comparable to the `0.1` local_file/s3 runs. Keep it as a basic connectivity and small-load reference only.

## Limitations

- This report is based on one artifact run per backend in the provided folders.
- Cross-run variance is not included; run multiple workflow executions for confidence intervals.
- Cloud object storage results can vary with network conditions and remote service behavior.

## Reproduce

Use the same workflow:

- https://github.com/swanlake-io/swanlake/actions/workflows/performance.yml

Or run locally with the same script used by CI (`scripts/run-benchbase.sh`) and equivalent environment variables for storage backend, scale factor, warmup, benchmark duration, and terminals.
