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

## Logging

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_LOG_FORMAT` | `compact` or `json` | `compact` |
| `NO_COLOR` | Disable log color if set to `true` | _(unset)_ |

## Status Page & Metrics

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_STATUS_ENABLED` | Enable the status HTTP server | `true` |
| `SWANLAKE_STATUS_HOST` | Status server bind address | `0.0.0.0` |
| `SWANLAKE_STATUS_PORT` | Status server port | `4215` |
| `SWANLAKE_METRICS_SLOW_QUERY_THRESHOLD_MS` | Slow query threshold (ms) used for tagging slow queries | `5000` |
| `SWANLAKE_METRICS_HISTORY_SIZE` | Number of latency/error/slow-query entries retained | `200` |

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

## DuckLake Maintenance (Checkpointing)

SwanLake can run background DuckLake checkpoints across multiple instances. Coordination and metadata are stored in PostgreSQL (`ducklake_checkpoints` table + advisory locks).

| Env Var | Description | Default |
| --- | --- | --- |
| `SWANLAKE_CHECKPOINT_DATABASES` | Comma-separated DuckLake database names to checkpoint (e.g. `db1,db2`) | _(unset)_ (disabled) |
| `SWANLAKE_CHECKPOINT_INTERVAL_HOURS` | Interval between checkpoints per database | `24` |
| `PGHOST` | PostgreSQL host | `localhost` |
| `PGPORT` | PostgreSQL port | `5432` |
| `PGUSER` | PostgreSQL user | `postgres` |
| `PGDATABASE` | PostgreSQL database | `postgres` |
| `PGPASSWORD` | PostgreSQL password | _(unset)_ |
| `PGSSLMODE` | TLS mode. `disable` = plaintext, `prefer` = try TLS then fall back to plaintext, `require` = TLS without verification, `verify-ca` = TLS verifying CA only, `verify-full` = full TLS verification | `disable` |

Notes:
- If `SWANLAKE_CHECKPOINT_DATABASES` is empty/unset, the checkpoint task is not started.
- Each configured database is checkpointed at most once per interval across all running instances.

## Validation

`ServerConfig::validate()` currently performs only lightweight checks; the remaining options are
validated as they are consumed (e.g. parsing socket addresses or attaching schemas).
