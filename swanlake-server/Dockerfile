# ========== builder ==========

FROM rust:1.91-bookworm AS builder

WORKDIR /swanlake

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        pkg-config \
        libssl-dev \
        binutils \
        && rm -rf /var/lib/apt/lists/*

COPY . /swanlake

RUN --mount=type=cache,id=cargo_registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=swanlake_target,target=/swanlake/target \
    cargo build --release --bin swanlake && \
    mkdir -p /release && \
    cp /swanlake/target/release/swanlake /release/ && \
    cp /swanlake/scripts/docker-entrypoint.sh /release/docker-entrypoint.sh && \
    cp -r /swanlake/swanlake-core/.duckdb /release/.duckdb

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
