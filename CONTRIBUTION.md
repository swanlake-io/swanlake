# Contributing

## BenchBase YCSB (Local pressure test)

Run a heavier local workload with more data and concurrency:

```bash
SCALE_FACTOR=50 TERMINALS=8 scripts/run-benchbase-ycsb.sh
```

Notes:
- `SCALE_FACTOR` controls row count (scale factor * 1000 rows).
- `TERMINALS` controls virtual terminal concurrency.
