#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BENCHBASE_REF="${BENCHBASE_REF:-main}"
ARROW_VERSION="${ARROW_VERSION:-18.3.0}"

WORK_DIR="${WORK_DIR:-$ROOT_DIR/target/benchbase-ycsb}"
BENCHBASE_SRC="$WORK_DIR/benchbase-src"
BENCHBASE_DIST="$WORK_DIR/benchbase-dist"
BENCHBASE_TARBALL="$WORK_DIR/benchbase-${BENCHBASE_REF}.tar.gz"

CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/tests/benchbase/ycsb-flight-sql.xml}"
DDL_PATH="${DDL_PATH:-$ROOT_DIR/tests/benchbase/ycsb-ddl-ducklake.sql}"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.toml}"
ENDPOINT="${ENDPOINT:-grpc://127.0.0.1:4214}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"
SCALE_FACTOR="${SCALE_FACTOR:-}"
TERMINALS="${TERMINALS:-}"
LOG_FILE="${LOG_FILE:-$WORK_DIR/benchbase-$(date +%Y%m%d-%H%M%S).log}"
SWANLAKE_SESSION_ID_MODE="${SWANLAKE_SESSION_ID_MODE:-peer_ip}"

# For shared state across JDBC connections, set SWANLAKE_DUCKLAKE_INIT_SQL to
# attach and USE a DuckLake database before running this script.

log() {
  printf '[benchbase-ycsb] %s\n' "$*"
}

mkdir -p "$WORK_DIR"

fetch_benchbase() {
  if [[ -d "$BENCHBASE_SRC" ]]; then
    log "BenchBase source already present at $BENCHBASE_SRC"
    return 0
  fi

  log "Fetching BenchBase (${BENCHBASE_REF})..."
  local tar_url="https://github.com/cmu-db/benchbase/archive/refs/heads/${BENCHBASE_REF}.tar.gz"
  curl -L --retry 3 --retry-delay 2 "$tar_url" -o "$BENCHBASE_TARBALL"
  tar -xzf "$BENCHBASE_TARBALL" -C "$WORK_DIR"
  mv "$WORK_DIR/benchbase-${BENCHBASE_REF}" "$BENCHBASE_SRC"
}

build_benchbase() {
  if [[ -f "$BENCHBASE_DIST/benchbase.jar" ]]; then
    log "BenchBase distribution already present at $BENCHBASE_DIST"
    return 0
  fi

  log "Building BenchBase with postgres profile..."
  pushd "$BENCHBASE_SRC" >/dev/null
  ./mvnw -q -DskipTests clean package -P postgres
  popd >/dev/null

  log "Extracting BenchBase distribution..."
  rm -rf "$BENCHBASE_DIST"
  tar -xzf "$BENCHBASE_SRC/target/benchbase-postgres.tgz" -C "$WORK_DIR"
  mv "$WORK_DIR/benchbase-postgres" "$BENCHBASE_DIST"
}

install_arrow_driver() {
  if ls "$BENCHBASE_DIST/lib"/flight-sql-jdbc-driver-*.jar >/dev/null 2>&1; then
    log "Arrow Flight SQL JDBC driver already present in $BENCHBASE_DIST/lib"
    return 0
  fi

  log "Resolving Arrow Flight SQL JDBC driver (arrow ${ARROW_VERSION})..."
  local deps_dir="$WORK_DIR/arrow-jdbc"
  mkdir -p "$deps_dir"

  cat > "$deps_dir/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.swanlake</groupId>
  <artifactId>benchbase-arrow-driver</artifactId>
  <version>1.0.0</version>
  <dependencies>
    <dependency>
      <groupId>org.apache.arrow</groupId>
      <artifactId>flight-sql-jdbc-driver</artifactId>
      <version>${ARROW_VERSION}</version>
    </dependency>
  </dependencies>
</project>
POM

  pushd "$BENCHBASE_SRC" >/dev/null
  ./mvnw -q -f "$deps_dir/pom.xml" dependency:copy-dependencies \
    -DoutputDirectory="$deps_dir/lib" \
    -DincludeScope=runtime
  popd >/dev/null

  cp -f "$deps_dir/lib"/*.jar "$BENCHBASE_DIST/lib/"
  log "Installed Arrow JDBC driver jars into $BENCHBASE_DIST/lib"
}

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

fetch_benchbase
build_benchbase
install_arrow_driver

SERVER_BIN="${SERVER_BIN:-$ROOT_DIR/target/debug/swanlake}"
SERVER_MODE="${SERVER_MODE:-cargo}"

# Ensure DuckDB environment variables are available for the server.
log "Loading DuckDB environment..."
source "$ROOT_DIR/swanlake-core/.duckdb/env.sh"

if [[ "$SERVER_MODE" == "cargo" ]]; then
  SERVER_CMD=(cargo run --quiet --package swanlake-server --bin swanlake)
  if [[ -f "$CONFIG_FILE" ]]; then
    SERVER_CMD+=(-- --config "$CONFIG_FILE")
  fi
else
  if [[ ! -x "$SERVER_BIN" ]]; then
    log "Building swanlake-server binary..."
    cargo build --package swanlake-server
  fi
  read -r -a SERVER_CMD <<<"$SERVER_BIN"
  if [[ -f "$CONFIG_FILE" ]]; then
    SERVER_CMD+=("--config" "$CONFIG_FILE")
  fi
fi

log "Starting SwanLake server (mode: $SERVER_MODE)..."
log "Using session id mode: $SWANLAKE_SESSION_ID_MODE"
export SWANLAKE_SESSION_ID_MODE
"${SERVER_CMD[@]}" &
SERVER_PID=$!
trap cleanup_server EXIT

log "Waiting for Flight SQL server at $ENDPOINT (timeout ${WAIT_SECONDS}s)..."
wait_for_server "$ENDPOINT" "$WAIT_SECONDS"

JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} --add-opens=java.base/java.nio=ALL-UNNAMED"
export JAVA_TOOL_OPTIONS

RESOLVED_CONFIG="$WORK_DIR/ycsb-flight-sql.resolved.xml"
if [[ -f "$CONFIG_PATH" ]]; then
  log "Rendering BenchBase config with ddlpath $DDL_PATH"
  sed "s|__DDL_PATH__|$DDL_PATH|g" "$CONFIG_PATH" > "$RESOLVED_CONFIG"
  if [[ -n "$SCALE_FACTOR" ]]; then
    log "Overriding YCSB scalefactor to $SCALE_FACTOR"
  fi
  if [[ -n "$TERMINALS" ]]; then
    log "Overriding YCSB terminals to $TERMINALS"
  fi
  python3 - "$RESOLVED_CONFIG" "$SCALE_FACTOR" "$TERMINALS" <<'PY'
import re
import sys

path = sys.argv[1]
scale = sys.argv[2]
terminals = sys.argv[3]

with open(path, "r", encoding="utf-8") as fh:
    text = fh.read()

def replace_tag(text: str, tag: str, value: str) -> str:
    if not value:
        return text
    pattern = re.compile(rf"<{tag}>.*?</{tag}>", re.DOTALL)
    return pattern.sub(f"<{tag}>{value}</{tag}>", text, count=1)

text = replace_tag(text, "scalefactor", scale)
text = replace_tag(text, "terminals", terminals)

with open(path, "w", encoding="utf-8") as fh:
    fh.write(text)
PY
else
  log "Config file not found at $CONFIG_PATH"
  exit 1
fi

log "Running BenchBase YCSB with config $RESOLVED_CONFIG"
log "Writing BenchBase output to $LOG_FILE"
pushd "$BENCHBASE_DIST" >/dev/null
java -cp "benchbase.jar:lib/*" com.oltpbenchmark.DBWorkload \
  -b ycsb \
  -c "$RESOLVED_CONFIG" \
  --create=true \
  --load=true \
  --execute=true \
  2>&1 | tee "$LOG_FILE"
popd >/dev/null

SUMMARY_FILE="$(ls -t "$BENCHBASE_DIST"/results/*.summary.json 2>/dev/null | head -n1 || true)"
if [[ -z "$SUMMARY_FILE" ]]; then
  log "BenchBase summary file not found in $BENCHBASE_DIST/results"
  exit 1
fi

python3 - "$SUMMARY_FILE" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)

goodput = float(data.get("Goodput (requests/second)", 0.0) or 0.0)
measured = int(data.get("Measured Requests", 0) or 0)

if goodput <= 0 or measured <= 0:
    print(
        f"BenchBase YCSB failed: measured_requests={measured}, goodput={goodput}",
        file=sys.stderr,
    )
    sys.exit(1)

print(f"BenchBase YCSB OK: measured_requests={measured}, goodput={goodput}")
PY

log "BenchBase YCSB completed successfully"
