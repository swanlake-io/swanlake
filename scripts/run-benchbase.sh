#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BENCHBASE_REF="${BENCHBASE_REF:-main}"
ARROW_VERSION="${ARROW_VERSION:-18.3.0}"
# Defaults to YCSB for backward compatibility; set BENCHMARK=tpch (etc.) to run others.
BENCHMARK="${BENCHMARK:-ycsb}"

WORK_DIR="${WORK_DIR:-$ROOT_DIR/target/benchbase-${BENCHMARK}}"
BENCHBASE_SRC="$WORK_DIR/benchbase-src"
BENCHBASE_DIST="$WORK_DIR/benchbase-dist"
BENCHBASE_TARBALL="$WORK_DIR/benchbase-${BENCHBASE_REF}.tar.gz"
TPCH_DIALECT_PATCH_VERSION="${TPCH_DIALECT_PATCH_VERSION:-duckdb-tpch-v7}"
TPCH_DIALECT_PATCH_MARKER="$WORK_DIR/.${TPCH_DIALECT_PATCH_VERSION}.applied"
DEFAULT_BENCHBASE_JUL_CONFIG="$BENCHBASE_SRC/src/main/resources/logging.properties"
DEFAULT_BENCHBASE_LOG4J_CONFIG="$BENCHBASE_SRC/src/main/resources/log4j.properties"
BENCHBASE_JUL_CONFIG="${BENCHBASE_JUL_CONFIG:-$DEFAULT_BENCHBASE_JUL_CONFIG}"
BENCHBASE_LOG4J_CONFIG="${BENCHBASE_LOG4J_CONFIG:-$DEFAULT_BENCHBASE_LOG4J_CONFIG}"
BENCHBASE_ENABLE_JDBC_LOGGING="${BENCHBASE_ENABLE_JDBC_LOGGING:-false}"
BENCHBASE_JAVA_OPTS="${BENCHBASE_JAVA_OPTS:-}"
BENCHBASE_DEBUG_LOGGING="${BENCHBASE_DEBUG_LOGGING:-false}"

CONFIG_PATH="${CONFIG_PATH:-}"
DDL_PATH="${DDL_PATH:-}"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config.toml}"
ENDPOINT="${ENDPOINT:-grpc://127.0.0.1:4214}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
SCALE_FACTOR="${SCALE_FACTOR:-}"
TERMINALS="${TERMINALS:-}"
WARMUP_SECONDS="${WARMUP_SECONDS:-}"
BENCHMARK_TIME_SECONDS="${BENCHMARK_TIME_SECONDS:-}"
LOG_FILE="${LOG_FILE:-$WORK_DIR/benchbase-$(date +%Y%m%d-%H%M%S).log}"
SERVER_LOG_FILE="${SERVER_LOG_FILE:-$WORK_DIR/swanlake-server-$(date +%Y%m%d-%H%M%S).log}"
SERVER_LOG_LINES="${SERVER_LOG_LINES:-200}"
BENCHBASE_LOG_LINES="${BENCHBASE_LOG_LINES:-200}"
SWANLAKE_SESSION_ID_MODE="${SWANLAKE_SESSION_ID_MODE:-peer_ip}"

# For shared state across JDBC connections, set SWANLAKE_DUCKLAKE_INIT_SQL to
# attach and USE a DuckLake database before running this script.

log() {
  printf '[benchbase-%s] %s\n' "$BENCHMARK" "$*"
}

if [[ "$BENCHBASE_DEBUG_LOGGING" == "true" ]]; then
  if [[ -z "$BENCHBASE_JUL_CONFIG" || "$BENCHBASE_JUL_CONFIG" == "$DEFAULT_BENCHBASE_JUL_CONFIG" ]]; then
    BENCHBASE_JUL_CONFIG="$ROOT_DIR/tests/benchbase/logging-debug.properties"
  fi
  if [[ -z "$BENCHBASE_LOG4J_CONFIG" || "$BENCHBASE_LOG4J_CONFIG" == "$DEFAULT_BENCHBASE_LOG4J_CONFIG" ]]; then
    BENCHBASE_LOG4J_CONFIG="$ROOT_DIR/tests/benchbase/log4j-debug.properties"
  fi
  BENCHBASE_ENABLE_JDBC_LOGGING=true
  if [[ "$BENCHBASE_JAVA_OPTS" != *"-Darrow.memory.debug.allocator=true"* ]]; then
    BENCHBASE_JAVA_OPTS="${BENCHBASE_JAVA_OPTS:+$BENCHBASE_JAVA_OPTS }-Darrow.memory.debug.allocator=true"
  fi
fi

if [[ -z "$CONFIG_PATH" ]]; then
  case "$BENCHMARK" in
    tpch)
      CONFIG_PATH="$ROOT_DIR/tests/benchbase/tpch-flight-sql.xml"
      ;;
    *)
      CONFIG_PATH="$ROOT_DIR/tests/benchbase/ycsb-flight-sql.xml"
      ;;
  esac
fi

if [[ -z "$DDL_PATH" ]]; then
  case "$BENCHMARK" in
    tpch)
      DDL_PATH="$ROOT_DIR/tests/benchbase/tpch-ddl-ducklake.sql"
      ;;
    *)
      DDL_PATH="$ROOT_DIR/tests/benchbase/ycsb-ddl-ducklake.sql"
      ;;
  esac
fi

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
  local force_rebuild=0
  if [[ "$BENCHMARK" == "tpch" && ! -f "$TPCH_DIALECT_PATCH_MARKER" ]]; then
    force_rebuild=1
  fi

  if [[ -f "$BENCHBASE_DIST/benchbase.jar" && "$force_rebuild" -eq 0 ]]; then
    log "BenchBase distribution already present at $BENCHBASE_DIST"
    return 0
  fi

  if [[ "$force_rebuild" -eq 1 ]]; then
    log "Rebuilding BenchBase to apply TPCH DuckDB dialect patch..."
  fi

  log "Building BenchBase with postgres profile..."
  pushd "$BENCHBASE_SRC" >/dev/null
  ./mvnw -q -DskipTests clean package -P postgres
  popd >/dev/null

  log "Extracting BenchBase distribution..."
  rm -rf "$BENCHBASE_DIST"
  tar -xzf "$BENCHBASE_SRC/target/benchbase-postgres.tgz" -C "$WORK_DIR"
  mv "$WORK_DIR/benchbase-postgres" "$BENCHBASE_DIST"

  if [[ "$BENCHMARK" == "tpch" ]]; then
    touch "$TPCH_DIALECT_PATCH_MARKER"
  fi
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
  local pid="${3:-}"

  python3 - "$endpoint" "$timeout" "$pid" <<'PY'
import os
import socket
import sys
import time
from urllib.parse import urlparse

endpoint = urlparse(sys.argv[1])
timeout = float(sys.argv[2])
pid_arg = sys.argv[3]
pid = int(pid_arg) if pid_arg else None
deadline = time.time() + timeout
host = endpoint.hostname or "127.0.0.1"
port = endpoint.port or (443 if endpoint.scheme == "https" else 80)

while time.time() < deadline:
    if pid is not None:
        try:
            os.kill(pid, 0)
        except OSError:
            print(
                f"SwanLake server process (pid={pid}) exited before binding {host}:{port}",
                file=sys.stderr,
            )
            sys.exit(2)
    try:
        with socket.create_connection((host, port), timeout=1):
            sys.exit(0)
    except OSError:
        time.sleep(0.2)

if pid is not None:
    try:
        os.kill(pid, 0)
        print(
            f"Timed out waiting for Flight SQL server at {host}:{port}; "
            f"server process (pid={pid}) is still running",
            file=sys.stderr,
        )
    except OSError:
        print(
            f"Timed out waiting for Flight SQL server at {host}:{port}; "
            f"server process (pid={pid}) already exited",
            file=sys.stderr,
        )
else:
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

print_server_log() {
  if [[ -f "$SERVER_LOG_FILE" ]]; then
    log "SwanLake server log: $SERVER_LOG_FILE"
    cat "$SERVER_LOG_FILE" || true
  else
    log "SwanLake server log file not found at $SERVER_LOG_FILE"
  fi
}

print_benchbase_log_tail() {
  if [[ -f "$LOG_FILE" ]]; then
    log "BenchBase log (last ${BENCHBASE_LOG_LINES} lines): $LOG_FILE"
    tail -n "$BENCHBASE_LOG_LINES" "$LOG_FILE" || true
  else
    log "BenchBase log file not found at $LOG_FILE"
  fi
}

summarize_benchbase_sql_errors() {
  local logfile="$1"
  if [[ ! -f "$logfile" ]]; then
    log "BenchBase log file not found for SQL error summary: $logfile"
    BENCHBASE_UNEXPECTED_SQL_ERRORS=0
    return 0
  fi

  local analysis
  analysis="$(python3 - "$logfile" <<'PY'
import re
import sys

ansi = re.compile(r"\x1b\[[0-9;]*m")
warn_re = re.compile(r"SQLException occurred during \[([^\]]+)\]")
row_re = re.compile(r"(com\.oltpbenchmark\.[^\s]+)\s+\[\s*(\d+)\]")

path = sys.argv[1]
with open(path, "r", encoding="utf-8", errors="replace") as fh:
    lines = [ansi.sub("", line.rstrip("\n")) for line in fh]

unexpected = {}
in_unexpected = False
for line in lines:
    stripped = line.strip()
    if "Unexpected SQL Errors:" in stripped:
        in_unexpected = True
        continue
    if not in_unexpected:
        continue
    if not stripped or stripped == "<EMPTY>":
        break
    if stripped.endswith(":"):
        break
    match = row_re.search(stripped)
    if match:
        unexpected[match.group(1)] = int(match.group(2))

causes = {}
for i, line in enumerate(lines):
    warn = warn_re.search(line)
    if not warn:
        continue
    proc = warn.group(1)
    fallback = None
    selected = None
    for j in range(i + 1, min(i + 60, len(lines))):
        probe = lines[j]
        if warn_re.search(probe):
            break
        if "Caused by:" not in probe:
            continue
        cause = probe.split("Caused by:", 1)[1].strip()
        if not cause:
            continue
        if fallback is None:
            fallback = cause
        if "Binding value of type" in cause:
            selected = cause
            break
    best = selected or fallback
    if best and proc not in causes:
        causes[proc] = best

total = sum(unexpected.values())
print(f"__UNEXPECTED_SQL_ERRORS__={total}")

if not unexpected:
    print("No unexpected SQL errors were reported by BenchBase.")
    sys.exit(0)

print("Unexpected SQL errors by procedure:")
for proc in sorted(unexpected):
    count = unexpected[proc]
    cause = causes.get(proc, "No root cause captured nearby; inspect full log.")
    print(f"- {proc}: count={count}; cause={cause}")
PY
)"

  BENCHBASE_UNEXPECTED_SQL_ERRORS="$(printf '%s\n' "$analysis" | awk -F= '/^__UNEXPECTED_SQL_ERRORS__=/{print $2}' | tail -n1)"
  BENCHBASE_UNEXPECTED_SQL_ERRORS="${BENCHBASE_UNEXPECTED_SQL_ERRORS:-0}"

  local human_readable
  human_readable="$(printf '%s\n' "$analysis" | sed '/^__UNEXPECTED_SQL_ERRORS__=/d')"
  if [[ -n "$human_readable" ]]; then
    log "BenchBase SQL error summary:"
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      log "$line"
    done <<<"$human_readable"
  fi
}

on_error() {
  local exit_code="$?"
  if [[ "$exit_code" -ne 0 ]]; then
    log "Encountered error (exit code $exit_code)"
    print_benchbase_log_tail
    summarize_benchbase_sql_errors "$LOG_FILE"
    print_server_log
  fi
}

patch_tpch_dialect_for_duckdb() {
  if [[ "$BENCHMARK" != "tpch" ]]; then
    return 0
  fi

  local tpch_root="$BENCHBASE_SRC/src/main"
  if [[ ! -d "$tpch_root" ]]; then
    log "TPCH source tree not found under $tpch_root"
    exit 1
  fi

  log "Patching BenchBase TPCH sources for DuckDB + Flight SQL compatibility..."
  python3 - "$BENCHBASE_SRC" <<'PY'
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])


def update_file(path: Path, replacements: list[tuple[str, str]]) -> None:
    if not path.exists():
        raise FileNotFoundError(f"required file not found: {path}")
    text = path.read_text(encoding="utf-8")
    original = text
    for old, new in replacements:
        if old in text:
            text = text.replace(old, new)
        elif new in text:
            continue
        else:
            raise RuntimeError(
                f"patch snippet not found in {path}: {old!r}"
            )
    if text != original:
        path.write_text(text, encoding="utf-8")


dialect_file = root / "src/main/resources/benchmarks/tpch/dialect-postgres.xml"
if not dialect_file.exists():
    raise FileNotFoundError(f"required file not found: {dialect_file}")

dialect = dialect_file.read_text(encoding="utf-8")

# Keep DuckDB interval syntax and cast compatibility.
dialect = dialect.replace("concat(?,' day')::interval", "CAST(? AS INTEGER)")
dialect = dialect.replace("(? * interval '1 day')", "CAST(? AS INTEGER)")
dialect = re.sub(r"\?::date\b", "CAST(? AS DATE)", dialect)
dialect = re.sub(r"\?::decimal\b", "CAST(? AS DOUBLE)", dialect)
dialect = re.sub(r"interval\s+'([0-9]+)'\s+(month|year)\b", r"interval '\1 \2'", dialect)

# Q1: bind an absolute cutoff date instead of interval arithmetic placeholder.
dialect = re.sub(
    r"date '1998-12-01'\s*-\s*CAST\(\?\s+AS\s+INTEGER\)",
    "?",
    dialect,
    flags=re.IGNORECASE,
)
dialect = re.sub(
    r"date '1998-12-01'\s*-\s*interval\s+\?\s+day",
    "?",
    dialect,
    flags=re.IGNORECASE,
)

dialect_file.write_text(dialect, encoding="utf-8")

proc_dir = root / "src/main/java/com/oltpbenchmark/benchmarks/tpch/procedures"

update_file(
    proc_dir / "Q1.java",
    [
        ("String delta = String.valueOf(rand.number(60, 120));", "int delta = rand.number(60, 120);"),
        (
            "stmt.setString(1, delta);",
            "stmt.setDate(1, java.sql.Date.valueOf(java.time.LocalDate.of(1998, 12, 1).minusDays(delta)));",
        ),
    ],
)
update_file(
    proc_dir / "Q2.java",
    [("stmt.setInt(1, size);", "stmt.setString(1, Integer.toString(size));")],
)
update_file(
    proc_dir / "Q3.java",
    [
        ("stmt.setDate(2, Date.valueOf(date));", "stmt.setString(2, date);"),
        ("stmt.setDate(3, Date.valueOf(date));", "stmt.setString(3, date);"),
    ],
)
update_file(
    proc_dir / "Q5.java",
    [
        ("stmt.setDate(2, Date.valueOf(date));", "stmt.setString(2, date);"),
        ("stmt.setDate(3, Date.valueOf(date));", "stmt.setString(3, date);"),
    ],
)
update_file(
    proc_dir / "Q6.java",
    [
        ("stmt.setString(3, discount);", "stmt.setDouble(3, Double.parseDouble(discount));"),
        ("stmt.setString(4, discount);", "stmt.setDouble(4, Double.parseDouble(discount));"),
    ],
)
update_file(
    proc_dir / "Q10.java",
    [
        ("stmt.setDate(1, Date.valueOf(date));", "stmt.setString(1, date);"),
        ("stmt.setDate(2, Date.valueOf(date));", "stmt.setString(2, date);"),
    ],
)
update_file(
    proc_dir / "Q11.java",
    [
        (
            "SUM(ps_supplycost * ps_availqty) * ?",
            "SUM(ps_supplycost * ps_availqty) * CAST(? AS DOUBLE)",
        ),
        ("stmt.setDouble(2, fraction);", "stmt.setString(2, Double.toString(fraction));"),
    ],
)
update_file(
    proc_dir / "Q12.java",
    [
        ("stmt.setDate(3, Date.valueOf(date));", "stmt.setString(3, date);"),
        ("stmt.setDate(4, Date.valueOf(date));", "stmt.setString(4, date);"),
    ],
)
update_file(
    proc_dir / "Q14.java",
    [
        ("stmt.setDate(1, Date.valueOf(date));", "stmt.setString(1, date);"),
        ("stmt.setDate(2, Date.valueOf(date));", "stmt.setString(2, date);"),
    ],
)
update_file(
    proc_dir / "Q15.java",
    [
        ("CREATE view revenue0 (supplier_no, total_revenue) AS", "CREATE TEMP VIEW revenue0 (supplier_no, total_revenue) AS"),
        ("DROP VIEW revenue0", "DROP VIEW IF EXISTS revenue0"),
    ],
)
update_file(
    proc_dir / "Q16.java",
    [("stmt.setInt(3 + i, sizes[i]);", "stmt.setString(3 + i, Integer.toString(sizes[i]));")],
)
update_file(
    proc_dir / "Q18.java",
    [
        ("SUM(l_quantity) > ?", "SUM(l_quantity) > CAST(? AS INTEGER)"),
        ("stmt.setInt(1, quantity);", "stmt.setString(1, Integer.toString(quantity));"),
    ],
)
update_file(
    proc_dir / "Q19.java",
    [
        ("AND l_quantity >= ?", "AND l_quantity >= CAST(? AS INTEGER)"),
        ("AND l_quantity <= ? + 10", "AND l_quantity <= CAST(? AS INTEGER) + 10"),
        ("stmt.setInt(2, quantity1);", "stmt.setString(2, Integer.toString(quantity1));"),
        ("stmt.setInt(3, quantity1);", "stmt.setString(3, Integer.toString(quantity1));"),
        ("stmt.setInt(5, quantity2);", "stmt.setString(5, Integer.toString(quantity2));"),
        ("stmt.setInt(6, quantity2);", "stmt.setString(6, Integer.toString(quantity2));"),
        ("stmt.setInt(8, quantity3);", "stmt.setString(8, Integer.toString(quantity3));"),
        ("stmt.setInt(9, quantity3);", "stmt.setString(9, Integer.toString(quantity3));"),
    ],
)
update_file(
    proc_dir / "Q20.java",
    [
        ("stmt.setDate(2, Date.valueOf(date));", "stmt.setString(2, date);"),
        ("stmt.setDate(3, Date.valueOf(date));", "stmt.setString(3, date);"),
    ],
)
PY
}

fetch_benchbase
patch_tpch_dialect_for_duckdb
build_benchbase
install_arrow_driver

SERVER_BIN="${SERVER_BIN:-$ROOT_DIR/target/debug/swanlake}"
SERVER_MODE="${SERVER_MODE:-cargo}"

# Ensure DuckDB environment variables are available for the server.
log "Loading DuckDB environment..."
DUCKDB_ENV_FILE="$ROOT_DIR/swanlake-core/.duckdb/env.sh"
if [[ -f "$DUCKDB_ENV_FILE" ]]; then
  source "$DUCKDB_ENV_FILE"
else
  log "DuckDB environment file not found at $DUCKDB_ENV_FILE; continuing without it"
fi

if [[ "$SERVER_MODE" == "cargo" ]]; then
  # Keep startup timeout focused on server bootstrap, not first-time compile.
  log "Building swanlake-server binary..."
  cargo build --package swanlake-server --bin swanlake
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
log "Writing SwanLake server output to $SERVER_LOG_FILE"
export SWANLAKE_SESSION_ID_MODE
("${SERVER_CMD[@]}") >"$SERVER_LOG_FILE" 2>&1 &
SERVER_PID=$!
trap on_error ERR
trap cleanup_server EXIT

log "Waiting for Flight SQL server at $ENDPOINT (timeout ${WAIT_SECONDS}s)..."
wait_for_server "$ENDPOINT" "$WAIT_SECONDS" "$SERVER_PID"

JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} --add-opens=java.base/java.nio=ALL-UNNAMED"
export JAVA_TOOL_OPTIONS

RESOLVED_CONFIG="$WORK_DIR/${BENCHMARK}-flight-sql.resolved.xml"
if [[ -f "$CONFIG_PATH" ]]; then
  log "Rendering BenchBase config with ddlpath $DDL_PATH"
  sed "s|__DDL_PATH__|$DDL_PATH|g" "$CONFIG_PATH" > "$RESOLVED_CONFIG"
  if [[ -n "$SCALE_FACTOR" ]]; then
    log "Overriding ${BENCHMARK} scalefactor to $SCALE_FACTOR"
  fi
  if [[ -n "$TERMINALS" ]]; then
    log "Overriding ${BENCHMARK} terminals to $TERMINALS"
  fi
  if [[ -n "$WARMUP_SECONDS" ]]; then
    log "Overriding ${BENCHMARK} warmup to ${WARMUP_SECONDS}s"
  fi
  if [[ -n "$BENCHMARK_TIME_SECONDS" ]]; then
    log "Overriding ${BENCHMARK} benchmark time to ${BENCHMARK_TIME_SECONDS}s"
  fi
  python3 - "$RESOLVED_CONFIG" "$SCALE_FACTOR" "$TERMINALS" "$WARMUP_SECONDS" "$BENCHMARK_TIME_SECONDS" <<'PY'
import re
import sys

path = sys.argv[1]
scale = sys.argv[2]
terminals = sys.argv[3]
warmup = sys.argv[4]
run_time = sys.argv[5]

with open(path, "r", encoding="utf-8") as fh:
    text = fh.read()

def replace_tag(text: str, tag: str, value: str) -> str:
    if not value:
        return text
    pattern = re.compile(rf"<{tag}>.*?</{tag}>", re.DOTALL)
    return pattern.sub(f"<{tag}>{value}</{tag}>", text, count=1)

text = replace_tag(text, "scalefactor", scale)
text = replace_tag(text, "terminals", terminals)
text = replace_tag(text, "warmup", warmup)
text = replace_tag(text, "time", run_time)

with open(path, "w", encoding="utf-8") as fh:
    fh.write(text)
PY
else
  log "Config file not found at $CONFIG_PATH"
  exit 1
fi

log "Running BenchBase $BENCHMARK with config $RESOLVED_CONFIG"
log "Writing BenchBase output to $LOG_FILE"
if [[ "$BENCHBASE_DEBUG_LOGGING" == "true" ]]; then
  log "Debug logging enabled (log4j=$BENCHBASE_LOG4J_CONFIG, jul=$BENCHBASE_JUL_CONFIG)"
fi

JAVA_FLAGS=()
if [[ -f "$BENCHBASE_LOG4J_CONFIG" ]]; then
  JAVA_FLAGS+=("-Dlog4j.configuration=file:$BENCHBASE_LOG4J_CONFIG")
fi
if [[ "$BENCHBASE_ENABLE_JDBC_LOGGING" == "true" ]]; then
  if [[ -f "$BENCHBASE_JUL_CONFIG" ]]; then
    JAVA_FLAGS+=("-Djava.util.logging.config.file=$BENCHBASE_JUL_CONFIG")
    log "Enabled java.util.logging config: $BENCHBASE_JUL_CONFIG"
  else
    log "JDBC logging requested but JUL config not found at $BENCHBASE_JUL_CONFIG"
  fi
fi
if [[ -n "$BENCHBASE_JAVA_OPTS" ]]; then
  read -r -a EXTRA_JAVA_FLAGS <<<"$BENCHBASE_JAVA_OPTS"
  JAVA_FLAGS+=("${EXTRA_JAVA_FLAGS[@]}")
fi

pushd "$BENCHBASE_DIST" >/dev/null
java "${JAVA_FLAGS[@]}" -cp "benchbase.jar:lib/*" com.oltpbenchmark.DBWorkload \
  -b "$BENCHMARK" \
  -c "$RESOLVED_CONFIG" \
  --create=true \
  --load=true \
  --execute=true \
  2>&1 | tee "$LOG_FILE"
popd >/dev/null

BENCHBASE_UNEXPECTED_SQL_ERRORS=0
summarize_benchbase_sql_errors "$LOG_FILE"

SUMMARY_FILE="$(ls -t "$BENCHBASE_DIST"/results/"${BENCHMARK}"_*.summary.json 2>/dev/null | head -n1 || true)"
if [[ -z "$SUMMARY_FILE" ]]; then
  log "BenchBase summary file for benchmark '$BENCHMARK' not found in $BENCHBASE_DIST/results"
  exit 1
fi

python3 - "$SUMMARY_FILE" "$BENCHMARK" <<'PY'
import json
import sys

path = sys.argv[1]
benchmark = sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)

final_state = str(data.get("Final State", "") or "")
goodput = float(data.get("Goodput (requests/second)", 0.0) or 0.0)
throughput = float(data.get("Throughput (requests/second)", 0.0) or 0.0)
measured = int(data.get("Measured Requests", 0) or 0)

if final_state.upper() != "EXIT":
    print(
        f"BenchBase {benchmark} failed: final_state={final_state!r}",
        file=sys.stderr,
    )
    sys.exit(1)

if measured <= 0:
    print(
        f"BenchBase {benchmark} failed: measured_requests={measured}",
        file=sys.stderr,
    )
    sys.exit(1)

if benchmark.lower() == "ycsb":
    metric = goodput if goodput > 0 else throughput
    metric_name = "goodput" if goodput > 0 else "throughput"
else:
    metric = throughput
    metric_name = "throughput"

if metric <= 0:
    print(
        f"BenchBase {benchmark} failed: measured_requests={measured}, "
        f"goodput={goodput}, throughput={throughput}",
        file=sys.stderr,
    )
    sys.exit(1)

print(
    f"BenchBase {benchmark} OK: measured_requests={measured}, "
    f"{metric_name}={metric}"
)
PY

if [[ "$BENCHBASE_UNEXPECTED_SQL_ERRORS" -gt 0 ]]; then
  log "BenchBase $BENCHMARK failed: unexpected_sql_errors=$BENCHBASE_UNEXPECTED_SQL_ERRORS"
  exit 1
fi

log "BenchBase $BENCHMARK completed successfully"
