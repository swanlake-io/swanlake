# Distributed Lock Example

This example demonstrates how to use SwanLake's distributed lock module with PostgreSQL advisory locks.

## Prerequisites

- PostgreSQL server running (default: localhost:5432)
- Database created (default: postgres)
- PostgreSQL user with permissions (default: postgres)

## Setup

1. Start a PostgreSQL server (if not already running):
```bash
# Using Docker
docker run --name postgres-lock-test -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:16

# Or use an existing PostgreSQL installation
```

2. Configure environment variables:
```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=postgres
```

Alternatively, use a full connection string:
```bash
export SWANLAKE_LOCK_POSTGRES_CONNECTION="host=localhost port=5432 user=postgres password=postgres dbname=postgres"
```

## Running the Example

```bash
# Run a single instance
cargo run --example distributed-lock

# Run multiple instances in parallel to see lock coordination
cargo run --example distributed-lock &
cargo run --example distributed-lock &
wait
```

## What It Does

The example:
1. Connects to PostgreSQL
2. Attempts to acquire a lock on a test resource path
3. If successful, holds the lock for 5 seconds
4. Releases the lock
5. If lock is already held, waits and retries

This demonstrates how multiple processes can coordinate access to a shared resource using PostgreSQL advisory locks.

## Cleanup

```bash
# If using Docker
docker stop postgres-lock-test
docker rm postgres-lock-test
```
