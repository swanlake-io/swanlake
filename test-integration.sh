#!/usr/bin/env bash

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}SwanDB Integration Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if DuckDB environment is set up
if [ ! -f .duckdb/env.sh ]; then
    echo -e "${RED}Error: DuckDB environment not found${NC}"
    echo "Please run: scripts/setup_duckdb.sh"
    exit 1
fi

# Source DuckDB environment
echo -e "${YELLOW}Loading DuckDB environment...${NC}"
source .duckdb/env.sh
echo -e "${GREEN}✓${NC} DuckDB environment loaded"
echo ""

# Build the server
echo -e "${YELLOW}Building SwanDB server...${NC}"
if cargo build --release 2>&1 | grep -q "error:"; then
    echo -e "${RED}✗ Build failed${NC}"
    cargo build --release
    exit 1
fi
echo -e "${GREEN}✓${NC} Server built successfully"
echo ""

# Kill any existing server on port 50051
echo -e "${YELLOW}Checking for existing server...${NC}"
if lsof -ti:50051 > /dev/null 2>&1; then
    echo "Killing existing server on port 50051..."
    kill -9 $(lsof -ti:50051) 2>/dev/null || true
    sleep 1
fi
echo -e "${GREEN}✓${NC} Port 50051 is available"
echo ""

# Start the server in background
echo -e "${YELLOW}Starting SwanDB server...${NC}"
SWANDB_PORT=50051 ./target/release/swandb > /tmp/swandb.log 2>&1 &
SERVER_PID=$!

# Cleanup function
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    if [ ! -z "$SERVER_PID" ]; then
        kill -9 $SERVER_PID 2>/dev/null || true
    fi
    # Also kill by port just in case
    lsof -ti:50051 | xargs kill -9 2>/dev/null || true
}

trap cleanup EXIT INT TERM

# Wait for server to start
echo "Waiting for server to start..."
MAX_WAIT=10
WAITED=0
while ! nc -z 127.0.0.1 50051 2>/dev/null; do
    sleep 1
    WAITED=$((WAITED + 1))
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo -e "${RED}✗ Server failed to start within ${MAX_WAIT}s${NC}"
        echo ""
        echo "Server logs:"
        cat /tmp/swandb.log
        exit 1
    fi
done
echo -e "${GREEN}✓${NC} Server started (PID: $SERVER_PID)"
echo ""

# Run the Go client tests
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Running Go Client Tests${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

cd go-client

if SWANDB_PORT=50051 go run main.go; then
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo -e "${BLUE}========================================${NC}"
    EXIT_CODE=0
else
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${RED}✗ Tests failed${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo -e "${YELLOW}Server logs:${NC}"
    cat /tmp/swandb.log
    EXIT_CODE=1
fi

exit $EXIT_CODE
