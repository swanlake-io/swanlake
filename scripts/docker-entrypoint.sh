#!/bin/sh
set -e
DUCKDB_PREFIX="${DUCKDB_PREFIX:-/app/.duckdb}"
if [ -f "${DUCKDB_PREFIX}/env.sh" ]; then
  . "${DUCKDB_PREFIX}/env.sh"
else
  echo "Warning: ${DUCKDB_PREFIX}/env.sh missing; DuckDB may fail to load" >&2
fi
exec "$@"
