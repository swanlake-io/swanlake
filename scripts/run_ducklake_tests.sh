#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENDPOINT="${ENDPOINT:-grpc://127.0.0.1:4214}"
TEST_FILES_CSV="${TEST_FILE:-$ROOT_DIR/tests/sql/ducklake_basic.test,$ROOT_DIR/tests/sql/duckling_queue_basic.test}"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.toml}"
SERVER_BIN="${SERVER_BIN:-cargo run --bin swanlake --}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"
TEST_DIR="${TEST_DIR:-$ROOT_DIR/target/ducklake-tests}"

if [[ -z "${DUCKDB_LIB_DIR:-}" && -f "$ROOT_DIR/.duckdb/env.sh" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT_DIR/.duckdb/env.sh"
fi

pushd "$ROOT_DIR" >/dev/null

read -r -a SERVER_CMD <<<"$SERVER_BIN"

if [[ -f "$CONFIG_FILE" ]]; then
  SERVER_CMD+=("--config" "$CONFIG_FILE")
fi

rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"
if [[ -z "${SWANLAKE_DUCKLING_QUEUE_ROOT:-}" ]]; then
  export SWANLAKE_DUCKLING_QUEUE_ROOT="$TEST_DIR/duckling_queue"
fi
mkdir -p "$SWANLAKE_DUCKLING_QUEUE_ROOT"
export SWANLAKE_DUCKLING_QUEUE_ENABLE="${SWANLAKE_DUCKLING_QUEUE_ENABLE:-true}"
export SWANLAKE_DUCKLING_QUEUE_ROTATE_SIZE_BYTES="${SWANLAKE_DUCKLING_QUEUE_ROTATE_SIZE_BYTES:-1}"
export SWANLAKE_DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS="${SWANLAKE_DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS:-1}"
export SWANLAKE_DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS="${SWANLAKE_DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS:-1}"
export SWANLAKE_DUCKLING_QUEUE_LOCK_TTL_SECONDS="${SWANLAKE_DUCKLING_QUEUE_LOCK_TTL_SECONDS:-600}"
# Prepare DuckLake attachment for flush worker when tests need it.

export RUST_LOG="${RUST_LOG:-info,swanlake::dq=debug}"

"${SERVER_CMD[@]}" &
SERVER_PID=$!

cleanup() {
  if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

python3 - "$ENDPOINT" "$WAIT_SECONDS" <<'PY'
import socket
import sys
import time
from urllib.parse import urlparse

endpoint = urlparse(sys.argv[1])
timeout = float(sys.argv[2])
deadline = time.time() + timeout
host = endpoint.hostname or "127.0.0.1"
port = endpoint.port or (443 if endpoint.scheme == "https" else 80)

while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=1):
            sys.exit(0)
    except OSError:
        time.sleep(0.2)

print(f"Timed out waiting for Flight SQL server at {host}:{port}", file=sys.stderr)
sys.exit(1)
PY

IFS=',' read -r -a TEST_FILES <<<"$TEST_FILES_CSV"
for TEST_FILE in "${TEST_FILES[@]}"; do
  if [[ ! -f "$TEST_FILE" ]]; then
    echo "Test file not found: $TEST_FILE" >&2
    exit 1
  fi
  cargo run --manifest-path "$ROOT_DIR/tests/runner/Cargo.toml" -- "$TEST_FILE" --endpoint "$ENDPOINT" --test-dir "$TEST_DIR"
done

cleanup
trap - EXIT

popd >/dev/null
