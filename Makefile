.PHONY: help build build-all clean test proto install-tools \
        build-linux build-mac-intel build-mac-arm \
        build-router build-storage \
        run-router run-storage \
        docker-build docker-up docker-down

# Variables
VERSION := $(shell cat VERSION 2>/dev/null || echo "dev")
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT) -X main.BuildTime=$(BUILD_TIME)

# Binary names
ROUTER_BIN := bin/router
STORAGE_BIN := bin/storage

# Go commands
GOCMD := go
GOBUILD := $(GOCMD) build
GOTEST := $(GOCMD) test
GOMOD := $(GOCMD) mod
GOCLEAN := $(GOCMD) clean

# Default target
.DEFAULT_GOAL := help

## help: Show this help message
help:
	@echo 'Usage:'
	@echo '  make <target>'
	@echo ''
	@echo 'Targets:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

## build: Build binaries for current platform
build: build-router build-storage
	@echo "Build complete for current platform!"

## build-all: Build binaries for all platforms
build-all: build-linux build-mac-intel build-mac-arm
	@echo "Build complete for all platforms!"

## build-router: Build router service for current platform
build-router:
	@echo "Building router service for current platform..."
	@$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(ROUTER_BIN) ./cmd/services/router
	@echo "Router binary: $(ROUTER_BIN)"

## build-storage: Build storage service for current platform
build-storage:
	@echo "Building storage service for current platform..."
	@$(GOBUILD) -ldflags "$(LDFLAGS)" -o $(STORAGE_BIN) ./cmd/services/storage
	@echo "Storage binary: $(STORAGE_BIN)"

## build-linux: Build binaries for Linux (EC2)
build-linux:
	@echo "Building for Linux (EC2)..."
	@GOOS=linux GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/router-linux-amd64 ./cmd/services/router
	@GOOS=linux GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/storage-linux-amd64 ./cmd/services/storage
	@echo "Linux binaries built:"
	@echo "  - bin/router-linux-amd64"
	@echo "  - bin/storage-linux-amd64"

## build-mac-intel: Build binaries for Mac Intel
build-mac-intel:
	@echo "Building for Mac Intel..."
	@GOOS=darwin GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/router-darwin-amd64 ./cmd/services/router
	@GOOS=darwin GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/storage-darwin-amd64 ./cmd/services/storage
	@echo "Mac Intel binaries built:"
	@echo "  - bin/router-darwin-amd64"
	@echo "  - bin/storage-darwin-amd64"

## build-mac-arm: Build binaries for Mac Apple Silicon (M1/M2/M3)
build-mac-arm:
	@echo "Building for Mac Apple Silicon..."
	@GOOS=darwin GOARCH=arm64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/router-darwin-arm64 ./cmd/services/router
	@GOOS=darwin GOARCH=arm64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/storage-darwin-arm64 ./cmd/services/storage
	@echo "Mac ARM binaries built:"
	@echo "  - bin/router-darwin-arm64"
	@echo "  - bin/storage-darwin-arm64"

## run-router: Run router service
run-router:
	@./$(ROUTER_BIN) -config configs/config.yaml

## run-storage: Run storage service
run-storage:
	@./$(STORAGE_BIN) -config configs/config.yaml

## test: Run all tests
test:
	@echo "Running tests..."
	@$(GOTEST) -v -race -coverprofile=coverage.out -covermode=atomic ./...
	@echo "Coverage report: coverage.out"

## test-short: Run tests without race detection
test-short:
	@echo "Running short tests..."
	@$(GOTEST) -v ./...

## test-coverage: Show test coverage in browser
test-coverage: test
	@$(GOCMD) tool cover -html=coverage.out

## proto: Generate protobuf files
proto:
	@echo "Generating protobuf files..."
	@bash scripts/generate_proto.sh

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@$(GOCLEAN)
	@rm -rf bin/router* bin/storage*
	@rm -f coverage.out
	@echo "Clean complete!"

## tidy: Tidy go modules
tidy:
	@echo "Tidying go modules..."
	@$(GOMOD) tidy

## vendor: Vendor dependencies
vendor:
	@echo "Vendoring dependencies..."
	@$(GOMOD) vendor

## install-tools: Install required tools
install-tools:
	@echo "Installing tools..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Tools installed!"

## docker-build: Build docker images
docker-build:
	@echo "Building docker images..."
	@docker-compose build

## docker-up: Start docker containers
docker-up:
	@echo "Starting docker containers..."
	@docker-compose up -d

## docker-down: Stop docker containers
docker-down:
	@echo "Stopping docker containers..."
	@docker-compose down

## docker-logs: Show docker logs
docker-logs:
	@docker-compose logs -f

## fmt: Format code
fmt:
	@echo "Formatting code..."
	@$(GOCMD) fmt ./...

## lint: Run linter (requires golangci-lint)
lint:
	@echo "Running linter..."
	@golangci-lint run ./...

## version: Show version information
version:
	@echo "Version: $(VERSION)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	@$(GOMOD) download
