.PHONY: help run build clean test deps

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

deps: ## Download Go dependencies
	go mod download
	go mod tidy

build: ## Build the Go client binary
	go build -o bin/swandb-client main.go

run: ## Run the client (requires SwanDB server running)
	go run main.go

test: ## Run Go tests
	go test -v ./...

clean: ## Clean build artifacts
	rm -rf bin/
	go clean

fmt: ## Format Go code
	go fmt ./...

vet: ## Run go vet
	go vet ./...

mod-update: ## Update Go dependencies
	go get -u ./...
	go mod tidy
