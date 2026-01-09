#!/bin/bash
set -e

TARGET_CACHE_ID="${TARGET_CACHE_ID:-swanlake_target}"
TARGET_TAR="${TARGET_TAR:-target.tar.zst}"
REGISTRY_TAR="${REGISTRY_TAR:-registry.tar.zst}"

# Extract Cargo Registry (compressed with zstd for faster transfer)
echo "Extracting cargo-registry..."
docker buildx build --progress=plain --output type=local,dest=. - <<EOF
FROM alpine:latest
RUN apk add --no-cache tar zstd
RUN --mount=type=cache,id=cargo_registry,target=/usr/local/cargo/registry \
    tar -cf - -C /usr/local/cargo/registry . | zstd -T0 -3 > /registry.tar.zst
FROM scratch
COPY --from=0 /registry.tar.zst /
EOF

# Extract Target (compressed with zstd for faster transfer)
echo "Extracting $TARGET_CACHE_ID..."
docker buildx build --progress=plain --output type=local,dest=. - <<EOF
FROM alpine:latest
RUN apk add --no-cache tar zstd
RUN --mount=type=cache,id=$TARGET_CACHE_ID,target=/app/target \
    tar -cf - -C /app/target . | zstd -T0 -3 > /$TARGET_TAR
FROM scratch
COPY --from=0 /$TARGET_TAR /
EOF
