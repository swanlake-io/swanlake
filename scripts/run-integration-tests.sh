#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ============================================================================
# Environment Configuration - All exports at the top
# ============================================================================

# Test directory setup
TEST_DIR="$ROOT_DIR/target/tmp/all-sql-tests-$(date +%s)"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

export SWANLAKE_DUCKLING_QUEUE_ENABLED=true
export SWANLAKE_DUCKLING_QUEUE_ROOT="$TEST_DIR/duckling_queue"
mkdir -p "$SWANLAKE_DUCKLING_QUEUE_ROOT"
export SWANLAKE_DUCKLING_QUEUE_DLQ_TARGET="$TEST_DIR/duckling_dlq"
mkdir -p "$SWANLAKE_DUCKLING_QUEUE_DLQ_TARGET"

export SWANLAKE_DUCKLAKE_INIT_SQL="ATTACH 'ducklake:postgres:dbname=swanlake_test' AS swanlake (DATA_PATH '$TEST_DIR/swanlake_files', OVERRIDE_DATA_PATH true);"
export RUST_LOG="${RUST_LOG:-info,swanlake::dq=debug}"

# Coverage configuration
export CARGO_TARGET_DIR="$ROOT_DIR/target/llvm-cov-target"
export CARGO_LLVM_COV_TARGET_DIR="$ROOT_DIR/target/llvm-cov-target"
export LLVM_PROFILE_FILE="$ROOT_DIR/target/llvm-cov-target/swanlake-%p-%m.profraw"

# Server configuration
export SERVER_BIN="$CARGO_TARGET_DIR/debug/swanlake"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.toml}"
ENDPOINT="${ENDPOINT:-grpc://127.0.0.1:4214}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"
TEST_FILTER="${TEST_FILTER:-*.test}"

# ============================================================================
# Reusable Functions
# ============================================================================

wait_for_server() {
  local endpoint="$1"
  local timeout="$2"

  python3 - "$endpoint" "$timeout" <<'PY'
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
}

cleanup_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}

# ============================================================================
# Coverage Cleanup
# ============================================================================

if [[ "${SWANLAKE_FORCE_LLVM_COV_CLEAN:-0}" == "1" ]]; then
  cargo llvm-cov clean --workspace
else
  COV_TARGET="$ROOT_DIR/target/llvm-cov-target"
  if [[ -d "$COV_TARGET" ]]; then
    find "$COV_TARGET" -name "*.profraw" -delete || true
    find "$COV_TARGET" -name "*.profdata" -delete || true
  fi
fi

# Export llvm-cov environment for coverage instrumentation
source <(cargo llvm-cov show-env --export-prefix)

# ============================================================================
# Build and Test
# ============================================================================

# Build with coverage
cargo build --package swanlake-server --package swanlake-core --target-dir "$CARGO_TARGET_DIR"

# Source DuckDB environment to set library paths
source "$ROOT_DIR/swanlake-core/.duckdb/env.sh"

# Run Rust unit tests under coverage instrumentation
cargo test --package swanlake-server --package swanlake-core --target-dir "$CARGO_TARGET_DIR"

# ============================================================================
# SQL Tests
# ============================================================================

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

# Start server once for all integration tests
read -r -a SERVER_CMD <<<"$SERVER_BIN"
if [[ -f "$CONFIG_FILE" ]]; then
  SERVER_CMD+=("--config" "$CONFIG_FILE")
fi

"${SERVER_CMD[@]}" &
SERVER_PID=$!

trap cleanup_server EXIT

wait_for_server "$ENDPOINT" "$WAIT_SECONDS"

# Run SQL tests
echo "Running SQL tests with filter '$TEST_FILTER' and TEST_DIR: $TEST_DIR"
if cargo run --manifest-path "$ROOT_DIR/tests/runner/Cargo.toml" -- "${TEST_FILES[@]}" --endpoint "$ENDPOINT" --test-dir "$TEST_DIR"; then
  rm -rf "$TEST_DIR"
else
  echo "Test failed, keeping TEST_DIR: $TEST_DIR"
  exit 1
fi

# ============================================================================
# Example Tests (reusing the same server)
# ============================================================================

# Run Go examples
for dir in "$ROOT_DIR/examples/go-"*; do
  if [[ -d "$dir" ]]; then
    cd "$dir"
    go run main.go
    cd -
  fi
done

# Run Rust example
cd "$ROOT_DIR/examples/rust-adbc"
cargo run
cd -

# Run Python example
cd "$ROOT_DIR/examples/python-adbc"
uv run main.py
cd -

# Run Python marimo example
cd "$ROOT_DIR/examples/python-marimo"
uv run swanlake.py
cd -

# Give a moment for profraw files to be fully written
sleep 2

cleanup_server

echo "Integration tests completed. Coverage data collected in target/llvm-cov-target/"

# ============================================================================
# Generate Coverage Report
# ============================================================================

cargo llvm-cov report --lcov --output-path "$ROOT_DIR/lcov.info" --package swanlake-server --package swanlake-core
