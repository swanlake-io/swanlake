#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Clean old coverage data
cargo llvm-cov clean --workspace

# Export llvm-cov environment for coverage instrumentation
source <(cargo llvm-cov show-env --export-prefix)

# Override target directories to use llvm-cov-target for consistency with report
export CARGO_TARGET_DIR="$ROOT_DIR/target/llvm-cov-target"
export CARGO_LLVM_COV_TARGET_DIR="$ROOT_DIR/target/llvm-cov-target"
export LLVM_PROFILE_FILE="$ROOT_DIR/target/llvm-cov-target/swanlake-%p-%m.profraw"

# Build with coverage using explicit target directory
cargo build --target-dir "$CARGO_TARGET_DIR"

# Run Rust unit tests under coverage instrumentation
cargo test --all --target-dir "$CARGO_TARGET_DIR"

# Run SQL tests with SERVER_BIN set to built binary for coverage
export SERVER_BIN="$CARGO_TARGET_DIR/debug/swanlake"
bash "$ROOT_DIR/scripts/run-all-sql-tests.sh"

# Start server for examples
"$CARGO_TARGET_DIR/debug/swanlake" &
SERVER_PID=$!

trap "kill -TERM $SERVER_PID 2>/dev/null || true; sleep 5; kill -9 $SERVER_PID 2>/dev/null || true" EXIT

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

# Run Python example
cd "$ROOT_DIR/examples/python-adbc"
uv run main.py
cd -

# Give a moment for profraw files to be fully written
sleep 2

echo "Integration tests completed. Coverage data collected in target/llvm-cov-target/"
