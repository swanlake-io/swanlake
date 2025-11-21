# Duckling Queue (DQ) – Server-Side Buffering

Duckling Queue prevents clients from flooding DuckLake/Postgres/S3 with many tiny files. Instead of attaching a DuckDB database per session, SwanLake now captures `INSERT INTO duckling_queue.*` statements, materializes the source query once, and buffers the resulting Arrow record batches per target table. Each enqueue is also written to disk under `DUCKLING_QUEUE_ROOT` so queued data survives process crashes. Background workers aggregate those batches and flush them to DuckLake when thresholds are met.

## Why the redesign?

The previous design attached a new DuckDB file for every session. That caused long connection times, thousands of useless `.db` files, and brittle table-creation logic that frequently raised “table not found” errors. The new pipeline solves those issues:

- **Zero per-session attachments** – Sessions simply execute the `INSERT` source query and hand the batches to the coordinator.
- **Table-scoped buffers** – We only keep one buffer per logical table, dramatically reducing file churn.
- **Deterministic schema handling** – The coordinator keeps the Arrow schema that DuckDB produced and (optionally) creates the DuckLake table before flushing.
- **Configurable aggregation** – Rows/bytes/age thresholds control how aggressively the runtime groups small batches into larger writes.

## Architecture Overview

```
client INSERT ... duckling_queue.table
             │
             ▼
Session executes the INSERT's source query (SELECT / VALUES)
             │
             ▼
DQ coordinator buffers RecordBatches per table
             │
             ├─ immediate flush if rows/bytes threshold exceeded
             └─ periodic sweeper flushes buffers that sat for too long
             ▼
Flush workers use dedicated DuckDB connections to insert the
coalesced data into {target_catalog}.{table}
```

### Key Components

- **`DqCoordinator` (`src/dq/coordinator.rs`)**  
  Keeps an in-memory `HashMap<table, BufferedTable>`, enqueues batches, and decides when a buffer should flush. Failures requeue the payload so nothing is lost.

- **`QueueRuntime` (`src/dq/runtime.rs`)**  
  Owns the background tasks:
  - flush workers (bounded by `max_parallel_flushes`) that write payloads via DuckDB’s appender API
  - age sweeper that asks the coordinator to flush stale buffers

- **Durable storage (`src/dq/storage.rs`)**  
  Persists every buffered chunk as an Arrow IPC file so crash recovery simply replays the surviving files. Flush workers delete chunk files after a successful write.

- **`Session` integration (`src/session/mod.rs`)**  
  Uses `sqlparser` to detect `INSERT INTO duckling_queue.*`, rewrites them into plain SELECT queries, and hands the resulting Arrow batches to the coordinator. `PRAGMA duckling_queue.flush` now just asks the coordinator to flush every buffer.

- **Schema helpers (`src/dq/schema.rs`)**  
  Map Arrow datatypes to DuckDB column specs so the runtime can optionally `CREATE TABLE IF NOT EXISTS` before inserting.

## Configuration

| Config key | Purpose | Default |
| --- | --- | --- |
| `DUCKLING_QUEUE_ROOT` | Directory where buffered batches are persisted between flushes | `target/duckling_queue` |
| `DUCKLING_QUEUE_BUFFER_MAX_ROWS` | Flush once a table accumulates this many rows | `50_000` |
| `DUCKLING_QUEUE_ROTATE_SIZE_BYTES` | Flush when buffered bytes exceed this value | `100_000_000` |
| `DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS` | Maximum age of buffered data before a sweep flushes it | `300` |
| `DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS` | How often the sweeper runs | `60` |
| `DUCKLING_QUEUE_MAX_PARALLEL_FLUSHES` | Concurrent flush workers | `2` |
| `DUCKLING_QUEUE_TARGET_CATALOG` | Destination catalog for flushed tables | `swanlake` |

All settings map directly to `ServerConfig` fields.

## Operational Notes

- `PRAGMA duckling_queue.flush`/`CALL duckling_queue_flush()` – force all buffers to flush immediately.
- Buffered batches are persisted under `DUCKLING_QUEUE_ROOT`, so pending data is replayed automatically after a crash. Flushes remain at-least-once: data that was already written to DuckLake may be retried after a failure.
- If DuckLake is unreachable, flush failures are logged and the payload is requeued, applying natural back-pressure on new inserts for that table. Optionally, set `SWANLAKE_DUCKLING_QUEUE_DLQ_TARGET` (e.g. `r2://bucket/path`) to copy failed chunks to remote storage instead of endlessly retrying that payload.

This new design keeps the user-facing SQL surface unchanged while drastically simplifying the implementation and avoiding the pathological file explosion observed previously.
