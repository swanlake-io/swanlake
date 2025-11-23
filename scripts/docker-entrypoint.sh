#!/bin/sh
set -e
export LD_LIBRARY_PATH="/app/.duckdb/1.4.1:${LD_LIBRARY_PATH:-}"
exec "$@"
