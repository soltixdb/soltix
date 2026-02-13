package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/queue"
	"github.com/soltixdb/soltix/internal/router"
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

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger, err := logging.NewFromConfig(cfg.Logging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	logging.SetGlobal(logger)
	logger.Info("Router service starting...",
		"version", Version, "commit", GitCommit, "build time", BuildTime)

	// Setup etcd metadata manager
	logger.Info("Connecting to etcd", "endpoints", cfg.Etcd.Endpoints)
	metadataManager, err := metadata.NewEtcdManager(cfg.Etcd.Endpoints)
	if err != nil {
		logger.Fatal("Failed to connect to etcd", "error", err)
	}
	defer func() { _ = metadataManager.Close() }()
	logger.Info("Metadata manager initialized with built-in cache")

	// Connect to Queue (configurable backend)
	logger.Info("Connecting to Queue", "type", cfg.Queue.Type, "url", cfg.Queue.URL)
	queueClient, err := queue.NewQueue(cfg.Queue)
	if err != nil {
		logger.Fatal("Failed to connect to Queue", "error", err)
	}
	defer func() { _ = queueClient.Close() }()
	logger.Info("Queue connection established")

	// Log authentication status
	if cfg.Auth.Enabled {
		logger.Info("API key authentication enabled", "num_keys", len(cfg.Auth.APIKeys))
	} else {
		logger.Warn("API key authentication DISABLED - all requests will be allowed")
	}

	// Initialize router
	app := router.New(logger, metadataManager, queueClient, *cfg)

	// Create context for background services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start group auto-assigner (watches for node changes, auto-distributes groups)
	var autoAssigner *coordinator.GroupAutoAssigner
	{
		groupManager := coordinator.NewGroupManager(logger, metadataManager, cfg.Coordinator)

		autoAssignerCfg := coordinator.AutoAssignerConfig{
			Enabled:            cfg.Coordinator.AutoAssigner.Enabled,
			PollInterval:       cfg.Coordinator.AutoAssigner.PollInterval,
			RebalanceOnJoin:    cfg.Coordinator.AutoAssigner.RebalanceOnJoin,
			RebalanceThreshold: cfg.Coordinator.AutoAssigner.RebalanceThreshold,
		}

		// Apply defaults if not configured
		if autoAssignerCfg.PollInterval <= 0 {
			autoAssignerCfg.PollInterval = 15 * time.Second
		}

		autoAssigner = coordinator.NewGroupAutoAssigner(
			logger, metadataManager, groupManager, autoAssignerCfg,
		)
		autoAssigner.Start(ctx)
	}

	// Start server in goroutine
	go func() {
		addr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
		logger.Info("Server listening", "address", addr)
		if err := app.Listen(addr); err != nil {
			logger.Fatal("Failed to start server", "error", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	// Stop auto-assigner
	if autoAssigner != nil {
		autoAssigner.Stop()
	}

	// Graceful shutdown with 10 second timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		logger.Error("Server forced to shutdown", "error", err)
	}

	logger.Info("Server exited")
}
