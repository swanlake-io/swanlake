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
client ─▶ session/connection ─▶ session-specific duckling_queue file (local DuckDB)
                               └▶ flush worker ─▶ SwanLake DuckLake attachment (remote)
```

### Components
1. **Session-scoped DuckDB connection (`SessionRegistry`):** Each session receives client statements and manages its own queue file.
2. **Per-session queue manager (`src/dq/session_queue.rs`):** Each session owns exactly one active queue file. Handles creation (lazy), rotation (size/time), and sealing (on cleanup). Generates session-specific `ATTACH` SQL.
3. **Global queue manager (`src/dq/manager.rs`):** Manages directories, configuration, and sweeps orphaned active files (from crashed sessions).
4. **Flush worker (`src/dq/runtime.rs`):** Background task that:
   - Periodically checks all sessions for rotation thresholds (global ticker)
   - Scans sealed/ directory for files to flush
   - Locks and flushes sealed files into remote DuckLake
   - Sweeps active/ directory for orphaned files (not owned by any current session)
5. **Rotation triggers:** Time threshold, size threshold, session cleanup, or client force-flush command.

## Data Lifecycle
1. **Boot & initialization:** On startup, SwanLake reads `ducklake_init_sql` and attaches DuckLake (`ATTACH ... AS swanlake`). No global queue file is created. Sessions will create their own queue files lazily.
2. **Session queue creation:** Each session eagerly provisions and attaches its own queue file when the session starts (future optimization: make this lazy on first `duckling_queue.*` write). Files are named `duckling_queue_{uuid}_{timestamp}.db` under `{root}/active/`.
3. **Client writes:** With the queue already attached, clients issue `CREATE TABLE duckling_queue.my_table AS ...` or `INSERT INTO duckling_queue...`. All schema and data live inside the session's queue file.
4. **Rotation triggers:**
   - **Time-based:** Global ticker checks all sessions every `rotate_interval` seconds
   - **Size-based:** Session checks file size via `fs::metadata()` periodically
   - **Session cleanup:** Before removing idle session, its queue file is automatically sealed
   - **Force flush:** Client executes `PRAGMA duckling_queue.flush;`
5. **Rotation process:** Session detaches current queue, moves file from active/ to sealed/, creates new active file, and re-attaches.
6. **Orphan sweep:** Background worker periodically scans active/ directory and seals files not owned by any current session (from crashed sessions or server restarts).
7. **Flush:** Flush worker acquires lock on sealed file, attaches read-only, enumerates tables, inserts into DuckLake target schema, marks tables as flushed (renames with `__dq_flushed_` prefix), detaches, and moves file to flushed/.
8. **Cleanup:** Old flushed files are removed after retention period (default 3 days).

## File & Metadata Layout
```
{DUCKLING_QUEUE_ROOT}/
  active/duckling_queue_{uuid}_{timestamp}.db
  sealed/duckling_queue_{uuid}_{timestamp}.db
  flushed/duckling_queue_{uuid}_{timestamp}.db
```
- Each session owns exactly one active file at a time
- Timestamp is Unix epoch seconds
- Active file is attached per-session: `ATTACH '{path}' AS duckling_queue;`
- When rotating, file moves from active/ to sealed/
- Each queue DB contains helper table: `__session_queue_metadata` with creation timestamp
- PostgreSQL advisory locks keyed by the file path prevent concurrent flushers across hosts

`ServerConfig` gets the matching fields. Queue files are created and attached per-session, not globally in `EngineFactory`.

## Configuration Additions
| Env / Config Field | Purpose | Default |
| --- | --- | --- |
| `DUCKLING_QUEUE_ENABLE` | Master toggle for the feature | `false` |
| `DUCKLING_QUEUE_ROOT` | Persistent directory (e.g. `/mnt/duckling`) | _required when enabled_ |
| `DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS` | Time-based rotation | `300` (5 min) |
| `DUCKLING_QUEUE_ROTATE_SIZE_BYTES` | Size-based rotation | `100_000_000` |
| `DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS` | How often the worker scans for sealed files | `60` |
| `DUCKLING_QUEUE_MAX_PARALLEL_FLUSHES` | Concurrency limit | `2` |
| `DUCKLING_QUEUE_AUTO_CREATE_TABLES` | Automatically create missing output tables | `false` |

`ServerConfig` gets the matching fields, and `EngineFactory::new` uses them to append the queue `ATTACH` statement immediately after the DuckLake attachment logic.

## Failure & Recovery Considerations
- **Crash before flush:** Sealed files remain on disk; next server restart's flush worker picks them up
- **Crash during flush:** PostgreSQL releases the advisory lock when the flushing task crashes, letting another host retry with at-least-once semantics (duplicates are possible)
- **Crash while file is active:** Startup orphan sweep moves all active/ files to sealed/ (no current session owns them)
- **Session timeout:** Before removing idle session, queue file is automatically sealed
- **Orphan detection:** Rotation loop periodically sweeps `active/` and attempts to re-acquire PostgreSQL advisory locks; any lock that can be reacquired indicates an orphaned file which is then sealed
- **Out-of-disk:** Rotation fails; session continues writing to current file until space available
- **At-least-once inserts:** Flushes rename tables with `__dq_flushed_` once the copy succeeds. A crash between the insert and rename can replay the same batch, so downstream consumers must tolerate duplicate rows.

## Operational Controls
- `PRAGMA duckling_queue.flush;` — rotates the current `active` file immediately, flushes every sealed DB synchronously, and ensures the caller can read freshly flushed data without waiting for the async worker.

## Implementation Status
- [x] **Session-scoped architecture redesign**
  - [x] Create `QueueSession` abstraction (`src/dq/session.rs`) for per-session queue file management
  - [ ] Implement lazy queue creation on first write to `duckling_queue.*` schema (currently eager on session startup for simplicity)
  - [x] Add session-level rotation methods with size/time threshold checks
  - [x] Implement automatic sealing on session cleanup
- [x] **Session integration**
  - [x] Add `dq_queue` and `dq_manager` fields to `Session` struct
  - [x] Hook lazy queue creation into all execute methods
  - [x] Implement `maybe_rotate_queue()`, `force_rotate_queue()`, and `cleanup_queue()` methods
- [x] **Registry updates**
  - [x] Add `maybe_rotate_all_queues()` to check all sessions for rotation
  - [x] Add `get_all_session_ids()` for orphan detection
  - [x] Update `cleanup_idle_sessions()` to seal queue files before removal
- [x] **Manager simplification**
  - [x] Remove global active file tracking and rotation logic
  - [x] Add `sweep_orphaned_files()` that leverages PostgreSQL advisory locks to detect abandoned files
  - [x] Keep directory management and configuration
- [x] **Runtime workers**
  - [x] Update rotation loop to check all sessions + sweep orphans
  - [x] Update flush loop to process sealed files (unchanged)
  - [x] Update force flush to handle session rotation + orphan sweep
  - [x] Add cleanup loop for old flushed files (3-day retention)
- [x] **EngineFactory simplification**
  - [x] Remove global duckling queue path (sessions manage their own)
  - [x] Base connection no longer includes queue attachment
- [x] **Testing & validation**
  - [ ] Update E2E tests for session-scoped behavior
  - [ ] Test orphan sweep on restart
  - [ ] Test multi-session concurrent writes with isolation
  - [ ] Verify rotation triggers (size, time, cleanup, force-flush)
