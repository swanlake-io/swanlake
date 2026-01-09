#!/bin/bash
set -e

TARGET_CACHE_ID="${TARGET_CACHE_ID:-swanlake_target}"
TARGET_CACHE_DIR="${TARGET_CACHE_DIR:-swanlake-target}"
TARGET_TAR="${TARGET_TAR:-target.tar.zst}"
REGISTRY_TAR="${REGISTRY_TAR:-registry.tar.zst}"

# Helper function to detect compression and build appropriate dockerfile
inject_cache() {
  local cache_id="$1"
  local cache_target="$2"
  local tar_file="$3"
  local ctx_dir="$4"

  # Detect if file is zstd compressed
  local is_zstd=false
  if [[ "$tar_file" == *.zst ]] || file "$tar_file" 2>/dev/null | grep -q "Zstandard"; then
    is_zstd=true
  fi

  mkdir -p "$ctx_dir"
  mv "$tar_file" "$ctx_dir/"
  local tar_basename=$(basename "$tar_file")

  if [ "$is_zstd" = true ]; then
    docker buildx build --progress=plain --target inject_stage -f - "$ctx_dir" <<EOF
FROM alpine:latest AS inject_stage
COPY $tar_basename /
RUN apk add --no-cache tar zstd
RUN --mount=type=cache,id=$cache_id,target=$cache_target \
    zstd -d -c /$tar_basename | tar -xf - -C $cache_target && \
    echo "=== INJECT DEBUG: $cache_id (zstd) ===" && \
    ls -la $cache_target && \
    du -sh $cache_target
EOF
  else
    docker buildx build --progress=plain --target inject_stage -f - "$ctx_dir" <<EOF
FROM alpine:latest AS inject_stage
COPY $tar_basename /
RUN apk add --no-cache tar
RUN --mount=type=cache,id=$cache_id,target=$cache_target \
    tar -xf /$tar_basename -C $cache_target && \
    echo "=== INJECT DEBUG: $cache_id (uncompressed) ===" && \
    ls -la $cache_target && \
    du -sh $cache_target
EOF
  fi

  rm -rf "$ctx_dir"
  echo "Removed $tar_file (context) to save space."
}

# Inject Cargo Registry
if [ -d cargo-registry ]; then
  echo "Legacy cargo-registry folder found. Packing into $REGISTRY_TAR for injection..."
  tar -cf - -C cargo-registry . | zstd -T0 -3 > "$REGISTRY_TAR"
  rm -rf cargo-registry
  echo "Removed legacy cargo-registry folder to save space."
fi

# Try compressed first, then uncompressed
REGISTRY_FILE=""
if [ -f "$REGISTRY_TAR" ]; then
  REGISTRY_FILE="$REGISTRY_TAR"
elif [ -f "registry.tar.zst" ]; then
  REGISTRY_FILE="registry.tar.zst"
elif [ -f "registry.tar" ]; then
  REGISTRY_FILE="registry.tar"
fi

if [ -n "$REGISTRY_FILE" ]; then
  echo "Injecting cargo-registry from $REGISTRY_FILE..."
  inject_cache "cargo_registry" "/usr/local/cargo/registry" "$REGISTRY_FILE" ".inject_ctx_registry"
else
  echo "No registry cache found, skipping injection."
fi

# Inject Target
if [ -d "$TARGET_CACHE_DIR" ]; then
  echo "Legacy $TARGET_CACHE_DIR folder found. Packing into $TARGET_TAR for injection..."
  tar -cf - -C "$TARGET_CACHE_DIR" . | zstd -T0 -3 > "$TARGET_TAR"
  rm -rf "$TARGET_CACHE_DIR"
  echo "Removed legacy $TARGET_CACHE_DIR folder to save space."
fi

# Try compressed first, then uncompressed
TARGET_FILE=""
if [ -f "$TARGET_TAR" ]; then
  TARGET_FILE="$TARGET_TAR"
elif [ -f "target.tar.zst" ]; then
  TARGET_FILE="target.tar.zst"
elif [ -f "target.tar" ]; then
  TARGET_FILE="target.tar"
fi

if [ -n "$TARGET_FILE" ]; then
  echo "Injecting $TARGET_CACHE_ID from $TARGET_FILE..."
  inject_cache "$TARGET_CACHE_ID" "/app/target" "$TARGET_FILE" ".inject_ctx_target"
else
  echo "No target cache found, skipping injection."
fi
