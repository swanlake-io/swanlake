# Duckling Queue Design

## Problem & Motivation
Directly inserting small batches into remote DuckLake storage causes many tiny Parquet fragments and high write latency. Duckling Queue introduces a persistent, local DuckDB staging layer per server so clients can keep writing quickly while SwanLake coalesces the data into larger batches before forwarding it to DuckLake.

## Goals
- Buffer client writes in a local DuckDB file (`duckling_queue_{uuid}.db`) that outlives SwanLake restarts.
- Rotate queue files by time and size so each flush produces sizable Parquet objects (ex: ≥100 MB).
- Flush queue files asynchronously into the upstream DuckLake database without client awareness.
- Survive crashes by replaying any queue DB that still contains unflushed data.
- Work in multi-host clusters by using portable locking so only one server flushes a given queue file at a time.

## Non-Goals
- Changing client SQL surfaces: clients keep using regular DuckDB tables.
- Fine-grained transactional guarantees across queue boundaries. Ordering is best-effort per file.

## High-Level Architecture
```
client ─▶ session/connection ─▶ duckling_queue (local DuckDB file)
                               └▶ flush worker ─▶ SwanLake DuckLake attachment (remote)
```

### Components
1. **Session-scoped DuckDB connection (existing code in `SessionRegistry`):** receives client statements.
2. **Queue attachment helper (`src/dq/manager.rs`):** creates/rotates `duckling_queue` files and injects the right `ATTACH` SQL into `EngineFactory` so every new connection sees both the remote DuckLake database (`swanlake`) and the local queue database (`duckling_queue`).
3. **Manifest & metadata tables:** small DuckDB table in each queue file that lists the logical tables staged inside and the matching destination schema in DuckLake.
4. **Flush worker (`src/dq/flush.rs`):** background task that scans the queue directory, locks one `.db` file, replays its contents into the remote DuckLake attachment, and removes the file once successful.
5. **Maintenance cron:** rotates the active queue file based on age/size, periodically rescans the `active/` directory for orphaned files, and reclaims stale locks.

## Data Lifecycle
1. **Attach & boot:** On startup SwanLake reads `ducklake_init_sql`, attaches DuckLake (`ATTACH ... AS swanlake`), then attaches/creates the active queue file: `ATTACH '{root}/duckling_queue_{server_uuid}_{seq}.db' AS duckling_queue;`. The queue file path is injected into the initialization SQL automatically when the feature is enabled.
2. **Client writes:** Clients issue `CREATE TABLE duckling_queue.my_table AS ...` or `INSERT INTO duckling_queue...`. All schema and data live inside the queue file.
3. **Rotation:** The queue manager tracks two thresholds: `rotate_bytes` and `rotate_interval`. When either is hit, it closes the ATTACH on the active connection, marks the file as `sealed`, and creates a fresh file/ATTACH for new writes. Sealed files stop accepting writes and are eligible for flushing.
4. **Flush:** The flush worker acquires a lock per sealed file (advisory file lock + best-effort `.lock` marker), attaches the file read-only, enumerates all user tables inside `duckling_queue`, and for each table runs `INSERT INTO swanlake.<dest_table> SELECT * FROM duckling_queue.<table>;`. After completely draining the file it drops the tables, detaches both DBs, and deletes the file.
5. **Recovery:** On restart the manager scans the queue directory, replays any `sealed` files, and recreates a single `active` file for new writes.

## File & Metadata Layout
```
{DUCKLING_QUEUE_ROOT}/
  active/duckling_queue_{server}_{ts}.db
  sealed/duckling_queue_{server}_{ts}.db
  flushed/  (optional archive for debugging)
```
- Active file path is embedded in `ATTACH ... AS duckling_queue` so every connection hits the same physical DB.
- When rotating, the file is moved from `active/` to `sealed/` and a `duckling_queue_{...}.lock` file is created by the worker holding it.
- Each queue DB contains helper tables:
  - `duckling_queue.__manifest(table_name TEXT, target_schema TEXT, last_append TIMESTAMP, row_count UINTEGER)`
  - `duckling_queue.__meta(key TEXT PRIMARY KEY, value TEXT)` storing the server UUID and schema version.

## Locking & Concurrency
- A single `active` Duckling Queue file is shared by every session on the same SwanLake process; sessions do **not** get per-session files.
- DuckDB allows concurrent connections to the same file and serializes writers internally, so multi-session inserts are safe (writers queue up briefly when they overlap).
- Advisory POSIX flock on the `.db` file prevents two SwanLake hosts from flushing it simultaneously.
- A `.lock` sidecar describes the owner hostname + pid + lease expiry; other hosts can steal the lock if the timestamp is stale.
- While a file is sealed, no session keeps it attached. New sessions only see the latest active file via `EngineFactory`.

## Configuration Additions
| Env / Config Field | Purpose | Default |
| --- | --- | --- |
| `DUCKLING_QUEUE_ENABLE` | Master toggle for the feature | `false` |
| `DUCKLING_QUEUE_ROOT` | Persistent directory (e.g. `/mnt/duckling`) | _required when enabled_ |
| `DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS` | Time-based rotation | `300` (5 min) |
| `DUCKLING_QUEUE_ROTATE_SIZE_BYTES` | Size-based rotation | `100_000_000` |
| `DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS` | How often the worker scans for sealed files | `60` |
| `DUCKLING_QUEUE_MAX_PARALLEL_FLUSHES` | Concurrency limit | `2` |
| `DUCKLING_QUEUE_LOCK_TTL_SECONDS` | Lease duration before another host can steal a flush lock | `600` |

`ServerConfig` gets the matching fields, and `EngineFactory::new` uses them to append the queue `ATTACH` statement immediately after the DuckLake attachment logic.

## Failure & Recovery Considerations
- **Crash before flush:** Sealed DB stays on disk; the next boot’s flush worker will pick it up.
- **Crash during flush:** Lease expires; another host will re-acquire the lock, attach the DB read-only, and replay from scratch (DuckDB writes are idempotent because we only insert into DuckLake).
- **Crash while a file is still active:** Every new server instance walks the `active/` directory during startup, treats every file as orphaned, and moves it into `sealed/` so it gets flushed like any other backlog. Because filenames embed the old server UUID/timestamp, the new host does not need prior state to identify them. The maintenance cron repeats this sweep on a schedule (e.g., every rotation tick) so long-lived servers also catch any active file that got stranded because rotation failed mid-way.
- **Out-of-disk:** Queue manager refuses to rotate and exposes a health warning; server can optionally drop to read-only mode.
- **Partial schema migrations:** Because each queue DB carries the manifest, we know which DuckLake destination table each staged table belongs to, even if clients rename tables later.

## Operational Controls
- `PRAGMA duckling_queue.flush;` — rotates the current `active` file immediately, flushes every sealed DB synchronously, and ensures the caller can read freshly flushed data without waiting for the async worker.

## Implementation TODOs (no code yet)
- [x] **Config plumbing**
  - [x] Extend `ServerConfig` with the Duckling Queue fields and document the env vars.
  - [x] Add startup validation ensuring `DUCKLING_QUEUE_ROOT` exists when the feature is enabled.
- [x] **Queue manager scaffolding (`src/dq/manager.rs`)**
  - [x] Create a struct storing server UUID, current active file metadata, and rotation thresholds.
  - [x] Implement helpers to generate file paths, write manifests, and emit the `ATTACH` SQL snippet.
- [x] **EngineFactory integration**
  - [x] When queueing is enabled, have `EngineFactory::new` request the manager’s `init_sql` fragment so every DuckDB connection automatically attaches the queue database alongside DuckLake.
  - [x] Ensure rotation events propagate to future connections (manager exposes `current_attach_sql()` and swaps it atomically).
- [x] **Rotation controller**
  - [x] Add a background task in `main.rs` that ticks every few seconds, checks the active file size + age, seals it via `manager.rotate()`, and sweeps `active/` for orphans.
- [x] **Flush worker implementation (`src/dq/runtime.rs`)**
  - [x] Periodically scan `sealed/`, take locks, attach the DB read-only, and stream each staged table into DuckLake via a pooled DuckDB connection.
  - [x] Delete or archive files after successful flush; if an error occurs, release the lock and leave the file for the next pass.
- [x] **Startup recovery**
  - [x] Before serving requests, invoke `manager.recover()` to move orphaned `.db` files into `sealed/` and schedule immediate flush attempts.
- [x] **Testing**
  - [x] Add a SQL logic test under `tests/sql/` (e.g., `duckling_queue_basic.test`) that writes into `duckling_queue`, triggers a scripted rotation, runs the flush worker once, and verifies the rows show up through the DuckLake attachment.
  - [x] Extend `tests/runner` to drive that SQL script so the end-to-end path is validated without unit tests.
- [x] **Documentation & observability**
  - [x] Expand `README.md`/`AGENT.md` with the new env vars and operational notes.
  - [x] Emit tracing spans for rotation and flush phases, and add metrics hooks (counters for queued rows, flush latency) for future scraping.
