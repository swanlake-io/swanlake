#!/bin/sh
set -e
source .duckdb/env.sh
exec "$@"
