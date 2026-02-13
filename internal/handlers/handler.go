package handlers

import (
	"context"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/handlers/processing"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/queue"
	"github.com/soltixdb/soltix/internal/services"
	"github.com/soltixdb/soltix/internal/utils"
)

// Handler contains all HTTP handlers
type Handler struct {
	logger           *logging.Logger
	metadataManager  metadata.Manager
	queuePublisher   queue.Publisher
	shardRouter      *coordinator.ShardRouter
	queryCoordinator *coordinator.QueryCoordinator
	processor        *processing.Processor
	// Services
	queryService       *services.QueryService
	streamQueryService *services.StreamQueryService
	forecastService    *services.ForecastService
	downloadService    *services.DownloadService
}

// New creates a new handler instance
func New(logger *logging.Logger, metadataManager metadata.Manager,
	queuePublisher queue.Publisher, coordCfg config.CoordinatorConfig,
	downloadDir string,
) *Handler {
	shardRouter := coordinator.NewShardRouter(logger, metadataManager, coordCfg)
	queryCoordinator := coordinator.NewQueryCoordinator(logger, metadataManager, shardRouter)
	processor := processing.NewProcessor(logger)

	// Create services
	queryService := services.NewQueryService(logger, metadataManager, queryCoordinator, processor)
	streamQueryService := services.NewStreamQueryService(logger, metadataManager, queryCoordinator, processor)
	forecastService := services.NewForecastService(logger, metadataManager, queryCoordinator)
	downloadService := services.NewDownloadService(logger, metadataManager, queryCoordinator, processor, downloadDir)

	return &Handler{
		logger:             logger,
		metadataManager:    metadataManager,
		queuePublisher:     queuePublisher,
		shardRouter:        shardRouter,
		queryCoordinator:   queryCoordinator,
		processor:          processor,
		queryService:       queryService,
		streamQueryService: streamQueryService,
		forecastService:    forecastService,
		downloadService:    downloadService,
	}
}

// GetQueryCoordinator returns the query coordinator for use by other services
func (h *Handler) GetQueryCoordinator() *coordinator.QueryCoordinator {
	return h.queryCoordinator
}

// updateFirstDataTime updates the FirstDataTime of a collection if not set or if the new timestamp is earlier
func (h *Handler) updateFirstDataTime(database, collection string, timestamp time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), utils.MetadataTrackingTimeout)
	defer cancel()

	// Get current collection metadata
	coll, err := h.metadataManager.GetCollection(ctx, database, collection)
	if err != nil {
		h.logger.Error("Failed to get collection for FirstDataTime update",
			"error", err, "database", database, "collection", collection)
		return
	}

	// Truncate to date only (YYYY-MM-DD)
	dateOnly := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, time.UTC)

	// Update if not set or if this timestamp is earlier
	needsUpdate := false
	if coll.FirstDataTime == nil {
		needsUpdate = true
	} else if dateOnly.Before(*coll.FirstDataTime) {
		needsUpdate = true
	}

	if needsUpdate {
		coll.FirstDataTime = &dateOnly
		if err := h.metadataManager.UpdateCollection(ctx, database, coll); err != nil {
			h.logger.Error("Failed to update FirstDataTime",
				"error", err, "database", database, "collection", collection)
			return
		}
		h.logger.Debug("Updated FirstDataTime",
			"database", database, "collection", collection, "first_data_time", dateOnly.Format("2006-01-02"))
	}
}
