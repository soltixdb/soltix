package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/grpc"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
	"github.com/soltixdb/soltix/internal/registry"
	"github.com/soltixdb/soltix/internal/storage"
	"github.com/soltixdb/soltix/internal/subscriber"
	"github.com/soltixdb/soltix/internal/sync"
)

var (
	Version   = "dev"     // Injected via ldflags during build
	GitCommit = "unknown" // Injected via ldflags during build
	BuildTime = "unknown" // Injected via ldflags during build
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to configuration file")
	flag.Parse()

	// 1. Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 2. Initialize logger
	logger, err := logging.NewFromConfig(cfg.Logging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	logging.SetGlobal(logger)

	logger.Info("Storage service starting...",
		"version", Version, "commit", GitCommit, "build time", BuildTime)

	// 3. Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Connect to etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Fatal("Failed to connect to etcd", "error", err)
	}
	defer func() { _ = etcdClient.Close() }()
	logger.Info("Connected to etcd", "endpoints", cfg.Etcd.Endpoints)

	// 5. Get node ID from config
	nodeID := cfg.Storage.NodeID
	logger.Info("Using configured node ID", "node_id", nodeID)

	// 6. Determine gRPC addresses
	// Bind address: what the server listens on
	// Advertise address: what other services use to connect
	grpcBindAddress := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.GRPCPort)
	grpcAdvertiseAddress := resolveGRPCAdvertiseAddress(logger, &cfg.Server)

	// 7. Initialize shard scanner
	shardScanner := registry.NewShardScanner(cfg.Storage.DataDir, logger)

	// 8. Create node info (use advertise address for registration)
	nodeInfo := models.NodeInfo{
		ID:      nodeID,
		Address: grpcAdvertiseAddress, // Use advertise address so other services can connect
		Status:  "active",
		Version: Version,
		Capacity: models.Capacity{
			TotalShards: 100, // Maximum shards this node can handle
		},
		Shards:    []models.ShardInfo{},
		UpdatedAt: time.Now(),
	}

	// 9. Initialize node registration
	registration := registry.NewNodeRegistration(
		etcdClient,
		nodeInfo,
		shardScanner,
		logger,
	)

	// 10. Register node with etcd
	if err := registration.Register(ctx); err != nil {
		logger.Fatal("Failed to register node", "error", err)
	}
	defer func() {
		deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer deregCancel()
		if err := registration.Deregister(deregCtx); err != nil {
			logger.Error("Failed to deregister node", "error", err)
		}
	}()

	// 11. Get storage timezone for consistent date handling
	storageTZ := cfg.Storage.GetStorageTimezone()
	logger.Info("Using storage timezone", "timezone", storageTZ.String())

	// 12. Create subscriber based on queue config
	subCfg := subscriber.Config{
		NodeID:        nodeID,
		ConsumerGroup: "soltix-storage",
	}
	sub, err := subscriber.NewSubscriber(cfg.Queue, subCfg)
	if err != nil {
		logger.Fatal("Failed to create subscriber", "error", err)
	}

	// 13. Initialize storage service with subscriber
	storageService, err := storage.NewStorageService(
		sub,
		nodeID,
		cfg.Storage.DataDir,
		logger,
		cfg.Storage.MemoryStore.MaxAge,
		cfg.Storage.MemoryStore.MaxSize,
		storage.StorageConfig{
			Timezone: storageTZ, // Use configured timezone
			// Columnar storage config
			MaxRowsPerPart:     cfg.Storage.Columnar.MaxRowsPerPart,
			MaxPartSize:        cfg.Storage.Columnar.MaxPartSize,
			MinRowsPerPart:     cfg.Storage.Columnar.MinRowsPerPart,
			MaxDevicesPerGroup: cfg.Storage.Columnar.MaxDevicesPerGroup,
		},
	)
	if err != nil {
		logger.Fatal("Failed to create storage service", "error", err)
	}

	// 14. Start storage service
	if err := storageService.Start(); err != nil {
		logger.Fatal("Failed to start storage service", "error", err)
	}
	defer func() { _ = storageService.Stop() }()

	logger.Info("Storage components started",
		"subject", fmt.Sprintf("soltix.write.node.%s", nodeID))

	// 15. Initialize and run sync from replicas (if enabled)
	var syncManager *sync.Manager
	var antiEntropy *sync.AntiEntropy
	if cfg.Storage.Sync.Enabled {
		syncManager, antiEntropy = initializeSync(
			ctx,
			cfg,
			nodeID,
			etcdClient,
			storageService,
			logger,
		)
		if syncManager != nil {
			defer syncManager.Stop()
		}
		if antiEntropy != nil {
			defer antiEntropy.Stop()
		}
	} else {
		logger.Info("Sync is disabled in configuration")
	}

	// 16. Initialize aggregate storage with configured timezone (for query handler)
	// Note: Aggregation files are stored under DataDir/agg/ subdirectory
	aggregateDataDir := filepath.Join(cfg.Storage.DataDir, "agg")
	aggregateStorage := aggregation.NewStorage(aggregateDataDir, logger)
	aggregateStorage.SetTimezone(storageTZ)

	// 17. Initialize gRPC server with V3 Columnar storage
	grpcServer := grpc.NewStorageServer(
		grpcBindAddress, // Server binds to this address (e.g., 0.0.0.0:5556)
		cfg.Storage.DataDir,
		nodeID,
		logger,
		storageService.GetMemoryStore(),   // Access memory store from storage service
		storageService.GetTieredStorage(), // 4-tier group-aware storage
		aggregateStorage,                  // Aggregate storage for querying aggregated data
	)

	// 18. Start gRPC server in a goroutine
	serverCtx, serverCancel := context.WithCancel(ctx)
	defer serverCancel()

	go func() {
		if err := grpcServer.Start(serverCtx); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	logger.Info("Storage service started successfully",
		"node_id", nodeID,
		"grpc_bind_address", grpcBindAddress,
		"grpc_advertise_address", grpcAdvertiseAddress,
		"data_dir", cfg.Storage.DataDir,
		"queue_type", cfg.Queue.Type,
		"queue_url", cfg.Queue.URL,
	)

	// 19. Wait for shutdown signal
	waitForShutdown(logger, cancel)

	logger.Info("Storage service stopped")
}

// waitForShutdown waits for interrupt signal and triggers graceful shutdown
func waitForShutdown(logger *logging.Logger, cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info("Received shutdown signal", "signal", sig.String())
	// Trigger context cancellation
	cancel()

	// Give some time for graceful shutdown
	time.Sleep(2 * time.Second)
}

// getOutboundIP gets the non-loopback IP address of this machine
// by attempting to establish a UDP connection to a public DNS server.
// Returns empty string if detection fails.
func getOutboundIP() string {
	// Use UDP connection to 8.8.8.8:80 (Google DNS)
	// This doesn't actually send any packets, just sets up the connection
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer func() { _ = conn.Close() }()

	// Get the local address from the connection
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func resolveGRPCAdvertiseAddress(logger *logging.Logger, serverCfg *config.ServerConfig) string {
	// If grpc_host is explicitly set to a non-wildcard address, use it directly
	if serverCfg.GRPCHost != "" && serverCfg.GRPCHost != "0.0.0.0" {
		return fmt.Sprintf("%s:%d", serverCfg.GRPCHost, serverCfg.GRPCPort)
	}

	// Advertise address: what other services use to connect
	var advertiseHost string
	// Try to get non-loopback IP address
	detectedIP := getOutboundIP()
	if detectedIP != "" {
		advertiseHost = detectedIP
		logger.Info("Auto-detected machine IP address for service discovery",
			"bind_address", serverCfg.Host,
			"advertise_address", fmt.Sprintf("%s:%d", advertiseHost, serverCfg.GRPCPort))
	} else {
		// Fallback to configured grpc_host or fail if not configured
		if serverCfg.GRPCHost != "" {
			advertiseHost = serverCfg.GRPCHost
			logger.Info("IP detection failed, using configured grpc_host for service discovery",
				"bind_address", serverCfg.Host,
				"advertise_address", fmt.Sprintf("%s:%d", advertiseHost, serverCfg.GRPCPort),
				"grpc_host", serverCfg.GRPCHost)
		} else {
			logger.Fatal("Failed to auto-detect IP address and no grpc_host configured. Please set server.grpc_host in config",
				"bind_address", serverCfg.Host)
		}
	}
	grpcAdvertiseAddress := fmt.Sprintf("%s:%d", advertiseHost, serverCfg.GRPCPort)
	return grpcAdvertiseAddress
}

// initializeSync initializes sync manager and anti-entropy service
func initializeSync(
	ctx context.Context,
	cfg *config.Config,
	nodeID string,
	etcdClient *clientv3.Client,
	storageService *storage.StorageService,
	logger *logging.Logger,
) (*sync.Manager, *sync.AntiEntropy) {
	logger.Info("Initializing sync components...")

	// Convert config to sync.Config
	syncConfig := sync.Config{
		Enabled:            cfg.Storage.Sync.Enabled,
		StartupSync:        cfg.Storage.Sync.StartupSync,
		StartupTimeout:     cfg.Storage.Sync.StartupTimeout,
		SyncBatchSize:      cfg.Storage.Sync.SyncBatchSize,
		MaxConcurrentSyncs: cfg.Storage.Sync.MaxConcurrentSyncs,
		AntiEntropy: sync.AntiEntropyConfig{
			Enabled:   cfg.Storage.Sync.AntiEntropy.Enabled,
			Interval:  cfg.Storage.Sync.AntiEntropy.Interval,
			BatchSize: cfg.Storage.Sync.AntiEntropy.BatchSize,
		},
	}

	// Create adapters
	metadataManager := sync.NewEtcdMetadataManager(etcdClient, logger)
	localStorage := sync.NewLocalStorageAdapter(
		storageService.GetStorage(),
		storageService.GetMemoryStore(),
		storageService.GetWriteWorkerPool(),
		logger,
	)
	remoteClient := sync.NewGRPCClient(logger, 30*time.Second)

	// Create sync manager
	syncManager := sync.NewManager(
		syncConfig,
		nodeID,
		logger,
		metadataManager,
		localStorage,
		remoteClient,
	)

	// Run startup sync if enabled
	if syncConfig.StartupSync {
		syncCtx, syncCancel := context.WithTimeout(ctx, syncConfig.StartupTimeout)
		defer syncCancel()

		logger.Info("Running startup sync from replicas...")
		if err := syncManager.SyncOnStartup(syncCtx); err != nil {
			logger.Error("Startup sync failed", "error", err)
			// Don't fatal - node can still serve partial data
			// Anti-entropy will eventually repair
		} else {
			logger.Info("Startup sync completed successfully")
		}
	}

	// Start anti-entropy service in background
	var antiEntropy *sync.AntiEntropy
	if syncConfig.AntiEntropy.Enabled {
		antiEntropy = sync.NewAntiEntropy(
			syncConfig.AntiEntropy,
			nodeID,
			logger,
			metadataManager,
			localStorage,
			remoteClient,
			syncManager,
		)
		go antiEntropy.Start(ctx)
		logger.Info("Anti-entropy service started",
			"interval", syncConfig.AntiEntropy.Interval)
	}

	return syncManager, antiEntropy
}
