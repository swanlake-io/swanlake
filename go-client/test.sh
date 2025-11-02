#!/usr/bin/env bash

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}SwanDB Go Client Test Runner${NC}"
echo "=================================="
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo -e "${RED}Error: Go is not installed${NC}"
    echo "Please install Go 1.21 or later from https://golang.org/dl/"
    exit 1
fi

echo -e "${GREEN}✓${NC} Go version: $(go version)"
echo ""

# Get connection parameters
SWANDB_HOST=${SWANDB_HOST:-127.0.0.1}
SWANDB_PORT=${SWANDB_PORT:-4214}

echo "Connection settings:"
echo "  Host: $SWANDB_HOST"
echo "  Port: $SWANDB_PORT"
echo ""

# Check if server is reachable
echo "Checking if SwanDB server is running..."
if ! nc -z "$SWANDB_HOST" "$SWANDB_PORT" 2>/dev/null; then
    echo -e "${YELLOW}Warning: Cannot connect to $SWANDB_HOST:$SWANDB_PORT${NC}"
    echo "Make sure SwanDB server is running:"
    echo "  cd .. && cargo run"
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo -e "${GREEN}✓${NC} Server is reachable"
fi
echo ""

# Ensure dependencies are up to date
echo "Installing dependencies..."
go mod download
go mod tidy
echo -e "${GREEN}✓${NC} Dependencies installed"
echo ""

# Run the test client
echo "Running Flight SQL tests..."
echo "=================================="
echo ""

if go run main.go; then
    echo ""
    echo "=================================="
    echo -e "${GREEN}✓ All tests completed successfully!${NC}"
    exit 0
else
    echo ""
    echo "=================================="
    echo -e "${RED}✗ Tests failed${NC}"
    exit 1
fi
