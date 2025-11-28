# Duckling Queue (DQ) – DuckDB-Backed Server-Side Buffer

Duckling Queue buffers small writes into a single DuckDB database file instead of per-session DuckDB attachments or per-chunk Parquet files. `INSERT INTO duckling_queue.*` is captured, materialized once, and appended into a staging table inside `buffer.duckdb`. Background workers read ordered slices from those staging tables and flush them into DuckLake.

```
client INSERT ... duckling_queue.table
             │
             ▼
Session executes INSERT source query (SELECT / VALUES)
             │
             ▼
DqCoordinator enqueues Arrow batches into buffer.duckdb staging tables
             │
             ├─ flush now if rows/bytes threshold exceeded
             └─ sweeper triggers age-based flushes
             ▼
Flush workers read slices ordered by dq_seq and insert into
{target_catalog}.{table}; on success they ack/delete flushed rows
```

## Why this design?

- **Single durable store** – DuckDB handles WAL/checkpoint; no Parquet chunk management or directory scans on recovery.
- **Lower enqueue cost** – Appender into local DuckDB is cheaper than per-chunk Parquet writes and avoids filesystem metadata churn.
- **Ordered delivery** – Each staging table has a `dq_seq` so flushers read in order and delete atomically after success.
- **Auto schema mirroring** – On schema mismatch, the buffer re-reads the SwanLake table schema and recreates the staging table only after safely dealing with buffered rows.

## Key Components

- **`DqCoordinator` (`src/dq/coordinator.rs`)**  
  Public API unchanged (`enqueue`, `flush_stale_buffers`, `force_flush_all`, `requeue`). It tracks in-memory thresholds and builds `FlushPayload`s. Storage is delegated to `DuckDbBuffer`.

- **`DuckDbBuffer` (`src/dq/duckdb_buffer.rs`)**  
  Owns `buffer.duckdb`. Ensures/creates staging tables `dq_<table>` with `dq_seq BIGINT` and payload columns. Enqueues batches, selects ordered slices for flushing, and acks deletions after a flush succeeds. Maintains metadata (`dq_meta`) with schema JSON and `last_flushed_seq` per table.

- **`QueueRuntime` (`src/dq/runtime.rs`)**  
  Background sweeper + flush workers (bounded by `max_parallel_flushes`). Workers receive `FlushPayload { table, schema, batches, handle }`, write to DuckLake, then ack the handle so buffered rows are deleted.

- **`Session` integration (`src/session/mod.rs`)**  
  Still detects `INSERT INTO duckling_queue.*`, rewrites to a SELECT, and hands Arrow batches to the coordinator. No per-session DuckDB files.

- **Schema helpers (`src/dq/schema.rs`)**  
  Map Arrow datatypes to DuckDB column definitions and help recreate staging tables when schemas change.

## Schema Changes (no data loss)

1. If incoming schema differs from the staging table, the buffer fetches the current SwanLake table schema.
2. If SwanLake schema matches the new incoming schema, the buffer first attempts to flush all buffered rows with the existing schema. Once drained (or if tables are empty), it recreates the staging table with the new schema and resets tracking. If flush fails (e.g., incompatible required columns), the old staging table is kept and an error is surfaced rather than truncating.
3. If schemas still disagree after refresh, the enqueue errors out; nothing is dropped.

## Configuration

| Config key | Purpose | Default |
| --- | --- | --- |
| `DUCKLING_QUEUE_ROOT` | Directory containing `buffer.duckdb` | `target/duckling_queue` |
| `DUCKLING_QUEUE_BUFFER_MAX_ROWS` | Flush once a table accumulates this many rows | `50_000` |
| `DUCKLING_QUEUE_ROTATE_SIZE_BYTES` | Flush when buffered bytes exceed this value | `100_000_000` |
| `DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS` | Maximum age of buffered data before a sweep flushes it | `300` |
| `DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS` | How often the sweeper runs | `60` |
| `DUCKLING_QUEUE_MAX_PARALLEL_FLUSHES` | Concurrent flush workers | `2` |
| `DUCKLING_QUEUE_TARGET_CATALOG` | Destination catalog for flushed tables | `swanlake` |
| `DUCKLING_QUEUE_FLUSH_CHUNK_ROWS` | Max rows per flush slice (per table) | `50_000` |
| `DUCKLING_QUEUE_MAX_DB_BYTES` | Optional size cap for `buffer.duckdb` (0 = no cap) | `0` |

All map to `ServerConfig`. DuckDB uses its default sync/WAL settings.

## Operational Notes

- `PRAGMA duckling_queue.flush`/`CALL duckling_queue_flush()` – force all buffers to flush immediately.
- Crash recovery: open `buffer.duckdb`, read `dq_meta` + `dq_%` tables, resume flushing rows newer than `last_flushed_seq`; no files to scan.
- At-least-once: if a flush fails, rows remain in the staging table and are retried; duplicates downstream are possible.
- Growth control: if `buffer.duckdb` exceeds `DUCKLING_QUEUE_MAX_DB_BYTES`, the coordinator can prioritize immediate flushes and (optionally) bypass the buffer to write directly to SwanLake with a warning.

This layout keeps the public DQ API stable, eliminates Parquet chunk management, and centralizes buffering/durability inside DuckDB for simpler maintenance.
