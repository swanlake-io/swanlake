# Build stage
FROM rust:slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source
COPY . .

# Build the project
RUN cargo build --release

# Runtime stage
FROM debian:trixie-slim

WORKDIR /app

# Install runtime deps (if needed, e.g., for DuckDB)
RUN apt update && apt install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Install grpc-health-probe for health checks
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.41 /ko-app/grpc-health-probe /usr/local/bin/grpc-health-probe

# Copy DuckDB setup
COPY --from=builder /app/.duckdb .duckdb

# Copy built binary
COPY --from=builder /app/target/release/swanlake swanlake

# Copy scripts for tests
COPY --from=builder /app/scripts scripts/

# Copy and set up entrypoint script
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create non-root user and switch to it
RUN groupadd -r swanlake -g 4214 && useradd -r -u 4214 -g swanlake swanlake && \
    chown -R swanlake:swanlake /app
USER swanlake

# Expose port
EXPOSE 4214

# Entrypoint and command
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["./swanlake"]
