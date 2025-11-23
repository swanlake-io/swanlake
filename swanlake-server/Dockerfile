# syntax=docker/dockerfile:1.7

# Common base with toolchain deps
FROM rust:1.91.1-slim AS toolchain
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /app

ENV CARGO_HOME=/usr/local/cargo \
    CARGO_TARGET_DIR=/app/target \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN --mount=type=cache,id=swanlake-apt-cache,target=/var/cache/apt \
    --mount=type=cache,id=swanlake-apt-lists,target=/var/lib/apt/lists \
    apt-get update && apt-get install -y --no-install-recommends \
        git \
        pkg-config \
        libssl-dev \
        binutils

# Install cargo-chef once so later edits don't invalidate the toolchain layer
FROM toolchain AS chef-installer
RUN --mount=type=cache,id=swanlake-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=swanlake-cargo-git,target=/usr/local/cargo/git \
    cargo install --locked cargo-chef

# Common base with cargo-chef available
FROM toolchain AS base
COPY --from=chef-installer /usr/local/cargo/bin/cargo-chef /usr/local/cargo/bin/

# Plan dependencies (only reruns when lockfiles/manifests change)
FROM base AS planner
COPY Cargo.toml Cargo.lock build.rs ./
RUN --mount=type=cache,id=swanlake-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=swanlake-cargo-git,target=/usr/local/cargo/git \
    cargo chef prepare --recipe-path recipe.json

# Build stage
FROM base AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,id=swanlake-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=swanlake-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=swanlake-target,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json --locked
COPY . .
RUN --mount=type=cache,id=swanlake-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=swanlake-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=swanlake-target,target=/app/target \
    cargo build --release --locked \
    && strip target/release/swanlake \
    && cp target/release/swanlake /app/swanlake

# Runtime stage
FROM debian:trixie-slim

WORKDIR /app

# Install runtime deps (if needed, e.g., for DuckDB)
RUN --mount=type=cache,id=swanlake-runtime-apt-cache,target=/var/cache/apt \
    --mount=type=cache,id=swanlake-runtime-apt-lists,target=/var/lib/apt/lists \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates

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
