package router

import (
	"path/filepath"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/handlers"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/middleware"
	"github.com/soltixdb/soltix/internal/queue"
)

// Setup configures all routes and middlewares
func Setup(app *fiber.App, logger *logging.Logger, metadataManager metadata.Manager, queueClient queue.Queue, cfg config.Config) *handlers.Handler {
	// Download directory for export files
	downloadDir := filepath.Join(cfg.Storage.DataDir, "downloads")

	// Create handler instance
	h := handlers.New(logger, metadataManager, queueClient, cfg.Coordinator, downloadDir)

	// Global middlewares
	app.Use(recover.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,PATCH,DELETE,OPTIONS",
		AllowHeaders: "Origin,Content-Type,Accept,Authorization,X-API-Key,X-Request-ID",
	}))
	app.Use(logging.FiberMiddleware(logger))

	// Health check (no auth required)
	app.Get("/health", h.Health)

	// API key authentication middleware
	authMiddleware := middleware.APIKeyAuth(logger, cfg.Auth.APIKeys, cfg.Auth.Enabled)

	// API v1 routes (protected by API key)
	v1 := app.Group("/v1", authMiddleware)

	// Database Management Routes
	v1.Post("/databases", h.CreateDatabase)
	v1.Get("/databases", h.ListDatabases)
	v1.Get("/databases/:database", h.GetDatabase)
	v1.Delete("/databases/:database", h.DeleteDatabase)

	// Collection Management Routes
	v1.Post("/databases/:database/collections", h.CreateCollection)
	v1.Get("/databases/:database/collections", h.ListCollections)
	v1.Get("/databases/:database/collections/:collection", h.GetCollection)
	v1.Delete("/databases/:database/collections/:collection", h.DeleteCollection)

	// Data Ingestion Routes
	v1.Post("/databases/:database/collections/:collection/write", h.Write)
	v1.Post("/databases/:database/collections/:collection/write/batch", h.WriteBatch)
	v1.Delete("/databases/:database/collections/:collection/delete", h.DeletePoints)

	// Data Query Routes
	v1.Get("/databases/:database/collections/:collection/query", h.Query)
	v1.Post("/databases/:database/collections/:collection/query", h.QueryPost)

	// Streaming Query Routes
	v1.Get("/databases/:database/collections/:collection/query/stream", h.QueryStream)
	v1.Post("/databases/:database/collections/:collection/query/stream", h.QueryStreamPost)

	// Forecast Routes
	v1.Get("/databases/:database/collections/:collection/forecast", h.Forecast)
	v1.Post("/databases/:database/collections/:collection/forecast", h.ForecastPost)

	// Download/Export Routes
	v1.Post("/databases/:database/collections/:collection/download", h.CreateDownload)
	v1.Get("/download/status/:request_id", h.GetDownloadStatus)
	v1.Get("/download/file/:request_id", h.DownloadFile)

	// Admin Routes (protected by API key)
	admin := app.Group("/admin", authMiddleware)
	admin.Post("/flush", h.TriggerFlush)

	// Group Management Routes (read-only, groups are auto-managed by GroupAutoAssigner)
	admin.Get("/groups", h.ListGroups)
	admin.Get("/groups/:group_id", h.GetGroup)
	admin.Get("/nodes/:node_id/groups", h.GetNodeGroups)

	// Device group lookup (which group a device belongs to)
	v1.Get("/databases/:database/collections/:collection/device/:device_id/group", h.LookupDeviceGroup)

	// 404 handler
	app.Use(h.NotFound)

	return h
}

// New creates a new Fiber app with configuration
func New(logger *logging.Logger, metadataManager metadata.Manager,
	queueClient queue.Queue, cfg config.Config,
) *fiber.App {
	app := fiber.New(fiber.Config{
		AppName:               "Soltix Router",
		DisableStartupMessage: true,
		ErrorHandler:          middleware.ErrorHandler(logger),
	})

	Setup(app, logger, metadataManager, queueClient, cfg)

	return app
}
