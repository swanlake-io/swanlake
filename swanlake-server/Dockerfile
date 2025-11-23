# ========== builder ==========

FROM lukemathwalker/cargo-chef:latest-rust-1.91-bookworm AS chef

WORKDIR /src

FROM chef AS planner
WORKDIR /app
COPY . .
# RUN git rev-parse HEAD
RUN cargo chef prepare --recipe-path recipe.json --bin swanlake

FROM chef AS builder
WORKDIR /swanlake

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        pkg-config \
        libssl-dev \
        binutils \
        && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --bin swanlake
COPY . /swanlake

RUN cargo build --release --bin swanlake
RUN cp -r /swanlake/target/release /release \
    && cp /swanlake/scripts/docker-entrypoint.sh /release/docker-entrypoint.sh \
    && cp -r /swanlake/swanlake-core/.duckdb /release/.duckdb

# ========== base ==========

FROM debian:bookworm-slim AS base

RUN apt-get update && apt-get install -y ca-certificates openssl && rm -rf /var/lib/apt/lists/*

# ========== swanlake ==========

FROM base AS gateway

RUN useradd -m -s /bin/bash swanlake

COPY --from=builder /release/docker-entrypoint.sh docker-entrypoint.sh
RUN chmod +x docker-entrypoint.sh

USER swanlake

WORKDIR /app

COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.41 /ko-app/grpc-health-probe /usr/local/bin/grpc-health-probe
COPY --from=builder /release/.duckdb .duckdb
COPY --from=builder /release/swanlake /usr/local/bin/swanlake

EXPOSE 4214

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["swanlake"]
