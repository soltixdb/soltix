package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/utils"
)

// ConnectionPool manages gRPC connections to storage nodes
type ConnectionPool struct {
	mu     sync.RWMutex
	conns  map[string]*grpc.ClientConn
	logger *logging.Logger

	// Health check configuration
	healthCheckInterval time.Duration
	stopCh              chan struct{}
	wg                  sync.WaitGroup
	closed              bool
	closeMu             sync.Mutex
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(logger *logging.Logger) *ConnectionPool {
	pool := &ConnectionPool{
		conns:               make(map[string]*grpc.ClientConn),
		logger:              logger,
		healthCheckInterval: utils.GRPCHealthCheckInterval,
		stopCh:              make(chan struct{}),
	}

	// Start background health checker
	pool.wg.Add(1)
	go pool.healthCheckLoop()

	return pool
}

// GetConnection gets or creates a gRPC connection
func (p *ConnectionPool) GetConnection(address string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, exists := p.conns[address]
	p.mu.RUnlock()

	if exists {
		// Check if connection is still healthy
		state := conn.GetState()
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			// Connection is unhealthy, remove and recreate
			p.removeConnection(address)
		} else {
			return conn, nil
		}
	}

	// Create new connection
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := p.conns[address]; exists {
		state := conn.GetState()
		if state != connectivity.TransientFailure && state != connectivity.Shutdown {
			return conn, nil
		}
		// Close unhealthy connection
		_ = conn.Close()
		delete(p.conns, address)
	}

	// Create connection with options
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1024*1024*10), // 10MB
			grpc.MaxCallSendMsgSize(1024*1024*10),
		),
	}

	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	p.conns[address] = conn
	p.logger.Debug("Created new gRPC connection", "address", address)

	return conn, nil
}

// removeConnection removes a connection from the pool
func (p *ConnectionPool) removeConnection(address string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, exists := p.conns[address]; exists {
		_ = conn.Close()
		delete(p.conns, address)
		p.logger.Debug("Removed unhealthy gRPC connection", "address", address)
	}
}

// healthCheckLoop periodically checks connection health
func (p *ConnectionPool) healthCheckLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.checkConnections()
		}
	}
}

// checkConnections checks all connections and removes unhealthy ones
func (p *ConnectionPool) checkConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for address, conn := range p.conns {
		state := conn.GetState()

		switch state {
		case connectivity.TransientFailure, connectivity.Shutdown:
			// Connection is unhealthy, close and remove
			_ = conn.Close()
			delete(p.conns, address)
			p.logger.Warn("Removed unhealthy gRPC connection",
				"address", address,
				"state", state.String())

		case connectivity.Idle:
			// Try to connect to verify the connection is still valid
			ctx, cancel := context.WithTimeout(context.Background(), utils.GRPCRequestTimeout)
			conn.Connect()
			if !conn.WaitForStateChange(ctx, connectivity.Idle) {
				// Connection is stuck in idle, might be stale
				p.logger.Debug("Connection idle, attempting reconnect", "address", address)
			}
			cancel()
		}
	}
}

// GetConnectionCount returns the number of active connections
func (p *ConnectionPool) GetConnectionCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.conns)
}

// GetConnectionStates returns the state of all connections
func (p *ConnectionPool) GetConnectionStates() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	states := make(map[string]string, len(p.conns))
	for address, conn := range p.conns {
		states[address] = conn.GetState().String()
	}
	return states
}

// Close closes all connections and stops the health checker
func (p *ConnectionPool) Close() {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return
	}
	p.closed = true
	p.closeMu.Unlock()

	// Stop health checker
	close(p.stopCh)
	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()

	for address, conn := range p.conns {
		if err := conn.Close(); err != nil {
			p.logger.Warn("Failed to close gRPC connection", "address", address, "error", err)
		}
	}

	p.conns = make(map[string]*grpc.ClientConn)
	p.logger.Info("Closed all gRPC connections")
}
