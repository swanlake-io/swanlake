# Build stage
FROM rust:slim AS builder

WORKDIR /app

# Copy source
COPY . .

# Build the project
RUN cargo build --release

# Runtime stage
FROM debian:trixie-slim

WORKDIR /app

# Install runtime deps (if needed, e.g., for DuckDB)
RUN apt update && apt install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/*

# Install grpc-health-probe for health checks
RUN wget -qO /usr/local/bin/grpc-health-probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.41/grpc_health_probe-linux-amd64 && \
    chmod +x /usr/local/bin/grpc-health-probe

# Copy DuckDB setup
COPY --from=builder /app/.duckdb .duckdb

# Copy built binary
COPY --from=builder /app/target/release/swanlake swanlake

# Copy scripts for tests
COPY --from=builder /app/scripts scripts/

# Copy and set up entrypoint script
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose port
EXPOSE 4214

# Entrypoint and command
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["./swanlake"]
