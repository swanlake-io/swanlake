# SwanLake Configuration

SwanLake reads all settings from environment variables using the `SWANLAKE_` prefix.
`ServerConfig::load()` merges sources in the following order:

1. Built-in defaults (see `src/config.rs`)
2. Values from a configuration file passed via CLI (`--config`, if provided)
3. Environment variables (`SWANLAKE_*`)

Unset options fall back to their defaults. All numeric values are expressed in base-10,
and boolean flags accept `true/false` (case-insensitive).

## Core Server Options

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_HOST` | gRPC bind address | `0.0.0.0` |
| `SWANLAKE_PORT` | gRPC listening port | `4214` |
| `SWANLAKE_MAX_SESSIONS` | Maximum concurrent sessions | `100` |
| `SWANLAKE_SESSION_TIMEOUT_SECONDS` | Idle timeout before cleanup | `900` (15 min) |

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

## Queueing & Flush Runtime

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_DUCKLING_QUEUE_ROOT` | Persistent directory for queue files | `target/ducklake-tests/duckling_queue` |
| `SWANLAKE_DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS` | Time-based rotation threshold | `300` |
| `SWANLAKE_DUCKLING_QUEUE_ROTATE_SIZE_BYTES` | Size-based rotation threshold (bytes) | `100_000_000` |
| `SWANLAKE_DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS` | How often sealed files are scanned | `60` |
| `SWANLAKE_DUCKLING_QUEUE_MAX_PARALLEL_FLUSHES` | Concurrent flush workers | `2` |
| `SWANLAKE_DUCKLING_QUEUE_TARGET_SCHEMA` | Target schema for flushed tables | `swanlake` |

The root directory is created automatically if it does not exist. Within that root the manager
expects three child directories: `active/`, `sealed/`, and `flushed/`.

## Validation

During startup `ServerConfig::validate()` ensures `DUCKLING_QUEUE_ROOT` exists (creating it when
necessary) and that it is a directory. All other options are validated when they are consumed
(e.g. parsing socket addresses or attaching schemas).
