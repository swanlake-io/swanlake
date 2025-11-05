#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENDPOINT="${ENDPOINT:-grpc://127.0.0.1:4214}"
TEST_FILE="${TEST_FILE:-$ROOT_DIR/tests/sql/ducklake_basic.test}"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.toml}"
SERVER_BIN="${SERVER_BIN:-cargo run --bin swanlake --}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"

if [[ -z "${DUCKDB_LIB_DIR:-}" && -f "$ROOT_DIR/.duckdb/env.sh" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT_DIR/.duckdb/env.sh"
fi

if [[ ! -f "$TEST_FILE" ]]; then
  echo "Test file not found: $TEST_FILE" >&2
  exit 1
fi

pushd "$ROOT_DIR" >/dev/null

read -r -a SERVER_CMD <<<"$SERVER_BIN"

if [[ -f "$CONFIG_FILE" ]]; then
  SERVER_CMD+=("--config" "$CONFIG_FILE")
fi

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

if [[ -n "${TEST_DIR:-}" ]]; then
  ARGS=(--test-dir "$TEST_DIR")
else
  ARGS=()
fi
cargo test-runner -- "$TEST_FILE" --endpoint "$ENDPOINT" "${ARGS[@]}"

cleanup
trap - EXIT

popd >/dev/null
