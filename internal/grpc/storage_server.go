package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// StorageServer represents the storage gRPC server
type StorageServer struct {
	address    string
	grpcServer *grpc.Server
	logger     *logging.Logger

	// Service handlers
	storageHandler *StorageServiceHandler
}

// NewStorageServer creates a new storage gRPC server instance with V3 Columnar storage
func NewStorageServer(
	address string,
	dataDir string,
	nodeID string,
	logger *logging.Logger,
	memoryStore *storage.MemoryStore,
	columnarStorage *storage.TieredStorage,
	aggregateStorage aggregation.AggregationStorage,
) *StorageServer {
	// Create storage service handler
	storageHandler := NewStorageServiceHandler(dataDir, nodeID, logger, memoryStore, columnarStorage, aggregateStorage)

	return &StorageServer{
		address:        address,
		logger:         logger,
		storageHandler: storageHandler,
	}
}

// Start starts the gRPC server
func (s *StorageServer) Start(ctx context.Context) error {
	// Create gRPC server with options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(1024 * 1024 * 10), // 10MB
		grpc.MaxSendMsgSize(1024 * 1024 * 10), // 10MB
	}

	s.grpcServer = grpc.NewServer(opts...)

	// Register storage service
	pb.RegisterStorageServiceServer(s.grpcServer, s.storageHandler)

	// Register reflection service (for debugging with grpcurl)
	reflection.Register(s.grpcServer)

	s.logger.Info("Registered StorageService with gRPC server")

	// Listen on the specified address
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}

	s.logger.Info("gRPC server starting", "address", s.address)

	// Start serving in a goroutine
	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			s.logger.Error("gRPC server error", "error", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	s.logger.Info("Shutting down gRPC server")
	s.Stop()

	return nil
}

// Stop stops the gRPC server gracefully
func (s *StorageServer) Stop() {
	if s.grpcServer != nil {
		s.logger.Info("Stopping gRPC server")
		s.grpcServer.GracefulStop()
	}
}
