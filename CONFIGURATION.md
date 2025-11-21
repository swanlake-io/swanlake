# SwanLake Configuration

SwanLake reads all settings from environment variables using the `SWANLAKE_` prefix.
`ServerConfig::load()` merges sources in the following order:

1. Built-in defaults (see `src/config.rs`)
2. Environment variables (`SWANLAKE_*`) – values from a local `.env` file are also picked up
   because the server calls `dotenvy::dotenv()` before loading the configuration.

Unset options fall back to their defaults. All numeric values are expressed in base-10,
and boolean flags accept `true/false` (case-insensitive).

## Core Server Options

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_HOST` | gRPC bind address | `0.0.0.0` |
| `SWANLAKE_PORT` | gRPC listening port | `4214` |
| `SWANLAKE_MAX_SESSIONS` | Maximum concurrent sessions | `100` |
| `SWANLAKE_SESSION_TIMEOUT_SECONDS` | Idle timeout before cleanup | `900` (15 min) |

## DuckDB UI Server

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_DUCKDB_UI_SERVER_ENABLED` | Start DuckDB's UI server (listens on port 4213 inside the process) | `false` |

## Logging

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_LOG_FORMAT` | `compact` or `json` | `compact` |
| `NO_COLOR` | Disable log color if set to `true` | _(unset)_ |

## DuckLake / DuckDB Initialization

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_DUCKLAKE_INIT_SQL` | SQL executed after DuckDB boots (attach remote catalogs, create schemas, etc.) | _(unset)_ |

The server always installs/loads the DuckLake, HTTPFS, AWS, and Postgres extensions before running
any user provided SQL so long as the binaries are available.

## External Connections

| Env Var | Description | Default |
| --- | --- | --- |
| `PGPASSWORD` | Password for Postgres connection | _(unset)_ |
| `PGHOST` | Host for Postgres connection | `127.0.0.1` |
| `PGUSER` | User for Postgres connection | `postgres` |

To enable DuckLake Postgres connection, see [DuckDB Postgres extension configuration](https://duckdb.org/docs/stable/core_extensions/postgres#configuring-via-environment-variables).

| Env Var | Description | Default |
| --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | Access key ID for S3 connection | _(unset)_ |
| `AWS_SECRET_ACCESS_KEY` | Secret access key for S3 connection | _(unset)_ |
| `AWS_SECRET_ACCOUNT_ID` | Account ID for S3 connection | _(unset)_ |

To enable DuckLake S3 connection, see [DuckDB HTTPFS S3 API configuration](https://duckdb.org/docs/stable/core_extensions/httpfs/s3api#platform-specific-secret-types).

## Distributed Locking

SwanLake uses PostgreSQL advisory locks for coordinating access to shared resources across multiple hosts. A single lightweight PostgreSQL session is shared for all locks on a host, so acquiring a lock never consumes an additional database connection.

| Env Var | Description | Default |
| --- | --- | --- |
| `PGHOST` | PostgreSQL host | `localhost` |
| `PGPORT` | PostgreSQL port | `5432` |
| `PGUSER` | PostgreSQL user | `postgres` |
| `PGDATABASE` | PostgreSQL database | `postgres` |
| `PGPASSWORD` | PostgreSQL password | _(unset)_ |
| `PGSSLMODE` | TLS mode. `disable` = plaintext, `prefer` = try TLS then fall back to plaintext, `require` = TLS without verification, `verify-ca` = TLS verifying CA only, `verify-full` = full TLS verification | `disable` |

Configuration is loaded once at startup and reused for the lifetime of the process. Locks are automatically released when the process terminates, so no extra TTL/lease controls are needed.

See `src/lock/README.md` for detailed documentation on the distributed lock implementation.

## Duckling Queue Buffering

SwanLake’s Duckling Queue buffers `INSERT INTO duckling_queue.*` statements in memory per target
table and flushes the accumulated batches into DuckLake/Postgres when thresholds are exceeded. These
settings control when flushes happen and how aggressive the runtime should be.

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_DUCKLING_QUEUE_BUFFER_MAX_ROWS` | Flush once a table buffers this many rows | `50_000` |
| `SWANLAKE_DUCKLING_QUEUE_ENABLED` | Start the Duckling Queue runtime and intercept `INSERT INTO duckling_queue.*` statements | `false` |
| `SWANLAKE_DUCKLING_QUEUE_ROTATE_SIZE_BYTES` | Flush when buffered bytes exceed this value | `100_000_000` |
| `SWANLAKE_DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS` | Maximum age (seconds) to hold buffered data before the sweeper flushes it | `300` |
| `SWANLAKE_DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS` | How often the sweeper checks for stale buffers | `60` |
| `SWANLAKE_DUCKLING_QUEUE_MAX_PARALLEL_FLUSHES` | Concurrent flush workers | `2` |
| `SWANLAKE_DUCKLING_QUEUE_TARGET_CATALOG` | Target catalog for flushed tables | `swanlake` |
| `SWANLAKE_DUCKLING_QUEUE_ROOT` | Directory used to persist buffered batches so they survive crashes | `target/duckling_queue` |
| `SWANLAKE_DUCKLING_QUEUE_DLQ_TARGET` | Optional destination (e.g. `r2://bucket/path`) to copy failed chunks instead of endlessly retrying | unset |

Admin commands:

- `PRAGMA duckling_queue.flush;` / `CALL duckling_queue_flush()` — force every buffer to flush
  immediately.

If the server logs that a flush failed, check DuckLake connectivity (the runtime retries and
requeues the payload). When clients receive “duckling queue runtime is disabled…”, set
`SWANLAKE_DUCKLING_QUEUE_ENABLED=true` and restart the service.

## Validation

`ServerConfig::validate()` currently performs only lightweight checks; the remaining options are
validated as they are consumed (e.g. parsing socket addresses or attaching schemas).
