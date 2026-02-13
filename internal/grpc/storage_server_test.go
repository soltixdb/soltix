package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
)

func TestNewStorageServer(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	server := NewStorageServer(
		"localhost:50051",
		"/tmp/test",
		"node1",
		logger,
		memStore,
		columnarStorage,
		aggStorage,
	)

	if server == nil {
		t.Fatal("Expected non-nil StorageServer")
		return
	}

	if server.address != "localhost:50051" {
		t.Errorf("Expected address 'localhost:50051', got '%s'", server.address)
	}

	if server.logger == nil {
		t.Error("Expected non-nil logger")
	}

	if server.storageHandler == nil {
		t.Error("Expected non-nil storageHandler")
	}

	if server.grpcServer != nil {
		t.Error("Expected nil grpcServer before Start()")
	}
}

func TestStorageServer_Stop(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	t.Run("stop_without_start", func(t *testing.T) {
		server := NewStorageServer(
			"localhost:50051",
			"/tmp/test",
			"node1",
			logger,
			memStore,
			columnarStorage,
			aggStorage,
		)

		// Stop should not panic even if server is not started
		server.Stop()
		// Test passes if no panic occurs
	})

	t.Run("stop_twice", func(t *testing.T) {
		server := NewStorageServer(
			"localhost:50051",
			"/tmp/test",
			"node1",
			logger,
			memStore,
			columnarStorage,
			aggStorage,
		)

		// Stop twice should not panic
		server.Stop()
		server.Stop()
		// Test passes if no panic occurs
	})

	t.Run("stop_with_nil_grpc_server", func(t *testing.T) {
		server := &StorageServer{
			address:    "localhost:50051",
			logger:     logger,
			grpcServer: nil,
		}

		// Should handle nil grpcServer gracefully
		server.Stop()
		// Test passes if no panic occurs
	})
}

func TestStorageServer_Start(t *testing.T) {
	logger := logging.NewDevelopment()
	memStore := storage.NewMemoryStore(2*time.Hour, 1024, logger)
	columnarStorage := storage.NewTieredStorage(storage.DefaultGroupStorageConfig("/tmp/test"), logger)
	var aggStorage aggregation.AggregationStorage

	t.Run("start_and_cancel", func(t *testing.T) {
		server := NewStorageServer(
			"localhost:0", // Use port 0 to get a random available port
			"/tmp/test",
			"node1",
			logger,
			memStore,
			columnarStorage,
			aggStorage,
		)

		ctx, cancel := context.WithCancel(context.Background())

		// Start server in goroutine
		startErr := make(chan error, 1)
		go func() {
			startErr <- server.Start(ctx)
		}()

		// Wait a bit for server to start
		time.Sleep(100 * time.Millisecond)

		// Cancel context to trigger shutdown
		cancel()

		// Wait for Start to return
		select {
		case err := <-startErr:
			if err != nil {
				t.Errorf("Start returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Start did not return after context cancellation")
		}
	})

	t.Run("start_with_immediate_cancel", func(t *testing.T) {
		server := NewStorageServer(
			"localhost:0",
			"/tmp/test",
			"node1",
			logger,
			memStore,
			columnarStorage,
			aggStorage,
		)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := server.Start(ctx)
		if err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	})

	t.Run("start_with_invalid_address", func(t *testing.T) {
		server := NewStorageServer(
			"invalid:999999", // Invalid port
			"/tmp/test",
			"node1",
			logger,
			memStore,
			columnarStorage,
			aggStorage,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := server.Start(ctx)
		// Should return an error for invalid address
		if err == nil {
			t.Error("Expected error for invalid address but got nil")
		}
	})

	t.Run("start_with_timeout", func(t *testing.T) {
		server := NewStorageServer(
			"localhost:0",
			"/tmp/test",
			"node1",
			logger,
			memStore,
			columnarStorage,
			aggStorage,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		startErr := make(chan error, 1)
		go func() {
			startErr <- server.Start(ctx)
		}()

		// Wait for timeout
		select {
		case err := <-startErr:
			if err != nil {
				t.Errorf("Start returned error: %v", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("Start did not return after timeout")
		}
	})
}
