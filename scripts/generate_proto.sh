#!/bin/bash

# Proto generation script for Soltix

set -e

# Ensure Go bin is on PATH for installed protoc plugins
GO_BIN="$(go env GOBIN)"
if [ -z "$GO_BIN" ]; then
    GO_BIN="$(go env GOPATH)/bin"
fi
export PATH="$GO_BIN:$PATH"

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc is not installed"
    echo "Install with: brew install protobuf (macOS) or apt-get install protobuf-compiler (Linux)"
    exit 1
fi

# Check if protoc-gen-go is installed
if ! command -v protoc-gen-go &> /dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

# Check if protoc-gen-go-grpc is installed
if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Installing protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

echo "Generating Go code from protobuf..."

# Create output directories
mkdir -p proto/storage/v1
mkdir -p proto/wal/v1

# Generate Go code
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/storage/v1/storage.proto

protoc \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    proto/wal/v1/wal.proto

echo "Proto generation completed successfully!"
echo "Generated files:"
echo "  - proto/storage/v1/storage.pb.go"
echo "  - proto/storage/v1/storage_grpc.pb.go"
echo "  - proto/wal/v1/wal.pb.go"
echo "  - proto/wal/v1/wal_grpc.pb.go"
