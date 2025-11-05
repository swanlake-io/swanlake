#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Setup DuckDB if not already done
if [[ ! -f "$ROOT_DIR/.duckdb/env.sh" ]]; then
  bash "$ROOT_DIR/scripts/setup_duckdb.sh"
fi

# Source the environment
if [[ -f "$ROOT_DIR/.duckdb/env.sh" ]]; then
  source "$ROOT_DIR/.duckdb/env.sh"
fi

mkdir -p "$ROOT_DIR/target/tmp"

for test_file in "$ROOT_DIR/tests/sql"/*.test; do
  if [[ ! -f "$test_file" ]]; then
    continue
  fi
  TEST_DIR="$ROOT_DIR/target/tmp/$(basename "$test_file" .test)-$(date +%s)"
  mkdir "$TEST_DIR"
  export TEST_FILE="$test_file"
  export TEST_DIR
  echo "Running test: $test_file with TEST_DIR: $TEST_DIR"
  if bash "$ROOT_DIR/scripts/run_ducklake_tests.sh"; then
    rm -rf "$TEST_DIR"
  else
    echo "Test failed, keeping TEST_DIR: $TEST_DIR"
    exit 1
  fi
done
