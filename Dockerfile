# Build stage
FROM rust:slim AS builder

WORKDIR /app

# Install dependencies for DuckDB setup
RUN apt-get update && apt-get install -y wget unzip curl && rm -rf /var/lib/apt/lists/*

# Copy scripts and setup DuckDB
COPY scripts/setup_duckdb.sh scripts/
RUN bash scripts/setup_duckdb.sh

# Copy source
COPY . .

# Build the project
RUN bash -c "source .duckdb/env.sh && cargo build --release"

# Runtime stage
FROM debian:trixie-slim

WORKDIR /app

# Install runtime deps (if needed, e.g., for DuckDB)
RUN apt update && apt install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy DuckDB setup
COPY --from=builder /app/.duckdb .duckdb

# Copy built binary
COPY --from=builder /app/target/release/swanlake swanlake

# Copy scripts for tests
COPY scripts/run-all-sql-tests.sh scripts/
COPY scripts/run_ducklake_tests.sh scripts/

# Copy and set up entrypoint script
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose port
EXPOSE 4214

# Entrypoint and command
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["./swanlake"]
