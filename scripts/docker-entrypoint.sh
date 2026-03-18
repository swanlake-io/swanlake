#!/bin/sh
set -e

DUCKDB_LIB_DIR="${DUCKDB_LIB_DIR:-/app/duckdb}"
if [ -d "${DUCKDB_LIB_DIR}" ]; then
  export LD_LIBRARY_PATH="${DUCKDB_LIB_DIR}:${LD_LIBRARY_PATH:-}"
fi
exec "$@"
