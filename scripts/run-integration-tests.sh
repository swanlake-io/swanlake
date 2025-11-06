#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Setup DuckDB if not already done
if [[ ! -f "$ROOT_DIR/.duckdb/env.sh" ]]; then
  bash "$ROOT_DIR/scripts/setup_duckdb.sh"
fi

# Source the environment
source "$ROOT_DIR/.duckdb/env.sh"

# Build with coverage
source <(cargo llvm-cov show-env --export-prefix)
cargo build

# Run SQL tests with SERVER_BIN set to built binary for coverage
export SERVER_BIN="./target/debug/swanlake"
bash "$ROOT_DIR/scripts/run-all-sql-tests.sh"

# Start server for examples
./target/debug/swanlake &
SERVER_PID=$!

# Wait for server to be ready
ENDPOINT="${ENDPOINT:-grpc://127.0.0.1:4214}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"

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

# Kill server
kill "$SERVER_PID" >/dev/null 2>&1 || true
wait "$SERVER_PID" 2>/dev/null || true
