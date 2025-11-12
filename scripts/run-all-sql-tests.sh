#!/usr/bin/env bash
# Run all SQL tests in tests/sql directory
#
# Usage:
#   ./scripts/run-all-sql-tests.sh [TEST_FILTER]
#
# Examples:
#   ./scripts/run-all-sql-tests.sh              # Run all *.test files
#   ./scripts/run-all-sql-tests.sh "duck*.test" # Run only duckling/ducklake tests
#   ./scripts/run-all-sql-tests.sh "show*.test" # Run only show_describe tests
#
# Environment variables:
#   ENDPOINT - SwanLake endpoint (default: grpc://127.0.0.1:4214)
#   CONFIG_FILE - Config file path (default: config.toml)
#   SERVER_BIN - Server binary command (default: cargo run --bin swanlake --)
#   WAIT_SECONDS - Server startup timeout (default: 30)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENDPOINT="${ENDPOINT:-grpc://127.0.0.1:4214}"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.toml}"
SERVER_BIN="${SERVER_BIN:-cargo run --bin swanlake --}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"
TEST_DIR="$ROOT_DIR/target/tmp/all-sql-tests-$(date +%s)"
TEST_FILTER="${1:-*.test}"



# Collect all test files matching the filter
TEST_FILES=()
for test_file in "$ROOT_DIR/tests/sql"/$TEST_FILTER; do
  if [[ -f "$test_file" ]]; then
    TEST_FILES+=("$test_file")
  fi
done

if [[ ${#TEST_FILES[@]} -eq 0 ]]; then
  echo "No test files found matching: $ROOT_DIR/tests/sql/$TEST_FILTER"
  exit 0
fi

echo "Found ${#TEST_FILES[@]} test file(s) to run"

mkdir -p "$ROOT_DIR/target/tmp"

read -r -a SERVER_CMD <<<"$SERVER_BIN"

if [[ -f "$CONFIG_FILE" ]]; then
  SERVER_CMD+=("--config" "$CONFIG_FILE")
fi

rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"
export SWANLAKE_DUCKLING_QUEUE_ROOT="$TEST_DIR/duckling_queue"
mkdir -p "$SWANLAKE_DUCKLING_QUEUE_ROOT"
export SWANLAKE_DUCKLING_QUEUE_ENABLE="${SWANLAKE_DUCKLING_QUEUE_ENABLE:-true}"
export SWANLAKE_DUCKLING_QUEUE_ROTATE_SIZE_BYTES="${SWANLAKE_DUCKLING_QUEUE_ROTATE_SIZE_BYTES:-1}"
export SWANLAKE_DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS="${SWANLAKE_DUCKLING_QUEUE_ROTATE_INTERVAL_SECONDS:-1}"
export SWANLAKE_DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS="${SWANLAKE_DUCKLING_QUEUE_FLUSH_INTERVAL_SECONDS:-1}"
export SWANLAKE_DUCKLING_QUEUE_LOCK_TTL_SECONDS="${SWANLAKE_DUCKLING_QUEUE_LOCK_TTL_SECONDS:-600}"
export SWANLAKE_DUCKLING_QUEUE_AUTO_CREATE_TABLES=true

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

echo "Running SQL tests with filter '$TEST_FILTER' and TEST_DIR: $TEST_DIR"
if cargo run --manifest-path "$ROOT_DIR/tests/runner/Cargo.toml" -- "${TEST_FILES[@]}" --endpoint "$ENDPOINT" --test-dir "$TEST_DIR"; then
  rm -rf "$TEST_DIR"
else
  echo "Test failed, keeping TEST_DIR: $TEST_DIR"
  exit 1
fi

cleanup
trap - EXIT
