#!/bin/sh
set -e
. .duckdb/env.sh
exec "$@"
