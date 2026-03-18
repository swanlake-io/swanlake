#!/usr/bin/env bash
# Sets LD_LIBRARY_PATH / DYLD_FALLBACK_LIBRARY_PATH so the swanlake binary
# can find the libduckdb shared library downloaded by libduckdb-sys.
#
# Usage:  source scripts/duckdb-lib-env.sh
#
# The script searches target/duckdb-download for the library and exports the
# appropriate environment variable for the current OS.

set -euo pipefail

_DUCKDB_LIB_ENV_ROOT="${CARGO_TARGET_DIR:-${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/target}}"

_duckdb_lib_path=""
if [[ "$(uname -s)" == "Darwin" ]]; then
  _duckdb_lib_path="$(find "$_DUCKDB_LIB_ENV_ROOT" -path '*/duckdb-download/*/libduckdb.dylib' 2>/dev/null | head -1)"
else
  _duckdb_lib_path="$(find "$_DUCKDB_LIB_ENV_ROOT" -path '*/duckdb-download/*/libduckdb.so' 2>/dev/null | head -1)"
fi

if [[ -z "$_duckdb_lib_path" ]]; then
  echo "Warning: libduckdb shared library not found under $_DUCKDB_LIB_ENV_ROOT/duckdb-download" >&2
else
  _duckdb_lib_dir="$(dirname "$_duckdb_lib_path")"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    export DYLD_FALLBACK_LIBRARY_PATH="${_duckdb_lib_dir}:${DYLD_FALLBACK_LIBRARY_PATH:-}"
  else
    export LD_LIBRARY_PATH="${_duckdb_lib_dir}:${LD_LIBRARY_PATH:-}"
  fi
  echo "DuckDB library path: $_duckdb_lib_dir"
fi

unset _duckdb_lib_path _duckdb_lib_dir _DUCKDB_LIB_ENV_ROOT
