# syntax=docker/dockerfile:1.7

# Common base with toolchain deps and cargo-chef
FROM rust:slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install --locked cargo-chef

# Plan dependencies (only reruns when lockfiles/manifests change)
FROM base AS planner
COPY Cargo.toml Cargo.lock build.rs ./
RUN cargo chef prepare --recipe-path recipe.json

# Build stage
FROM base AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json --locked
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp target/release/swanlake /app/swanlake

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
COPY --from=builder /app/swanlake swanlake

# Copy scripts for tests
COPY --from=builder /app/scripts scripts/

# Copy and set up entrypoint script
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create non-root user and switch to it
RUN sed -i 's/SYS_UID_MAX 999/SYS_UID_MAX 9999/' /etc/login.defs && \
    groupadd -r swanlake -g 4214 && useradd -r -u 4214 -g swanlake -d /app swanlake && \
    chown -R swanlake:swanlake /app
USER swanlake

# Expose port
EXPOSE 4214
EXPOSE 4213

# Entrypoint and command
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["./swanlake"]
