package services

import (
	"context"
	"time"

	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/handlers/processing"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// QueryService handles query business logic
type QueryService struct {
	logger           *logging.Logger
	metadataManager  metadata.Manager
	queryCoordinator *coordinator.QueryCoordinator
	processor        *processing.Processor
}

// NewQueryService creates a new QueryService
func NewQueryService(
	logger *logging.Logger,
	metadataManager metadata.Manager,
	queryCoordinator *coordinator.QueryCoordinator,
	processor *processing.Processor,
) *QueryService {
	return &QueryService{
		logger:           logger,
		metadataManager:  metadataManager,
		queryCoordinator: queryCoordinator,
		processor:        processor,
	}
}

// QueryResult represents the final query result
type QueryResult struct {
	Database   string                             `json:"database"`
	Collection string                             `json:"collection"`
	StartTime  string                             `json:"start_time"`
	EndTime    string                             `json:"end_time"`
	Results    []coordinator.FormattedQueryResult `json:"results"`
	Anomalies  []processing.AnomalyResult         `json:"anomalies,omitempty"`
}

// Execute performs a complete query with all post-processing
func (s *QueryService) Execute(ctx context.Context, input *models.QueryRequest) (*QueryResult, error) {
	startTime := time.Now()

	// Validate collection existence
	if err := s.metadataManager.ValidateCollection(ctx, input.Database, input.Collection); err != nil {
		return nil, &ServiceError{
			Code:    "COLLECTION_NOT_FOUND",
			Message: err.Error(),
		}
	}

	// Build coordinator request
	req := &coordinator.QueryRequest{
		Database:    input.Database,
		Collection:  input.Collection,
		DeviceIDs:   input.IDs,
		StartTime:   input.StartTimeParsed,
		EndTime:     input.EndTimeParsed,
		Fields:      input.Fields,
		Limit:       input.Limit,
		Interval:    input.Interval,
		Aggregation: input.Aggregation,
	}

	// Execute distributed query
	coordResults, err := s.queryCoordinator.Query(ctx, req)
	if err != nil {
		latency := time.Since(startTime)
		s.logger.Error("Query failed",
			"error", err,
			"latency_ms", latency.Milliseconds())
		return nil, &ServiceError{
			Code:    "QUERY_FAILED",
			Message: "Failed to execute query",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	// Convert coordinator results to processing results
	results := s.coordinatorToProcessing(coordResults)

	// Apply post-processing pipeline
	var anomalies []processing.AnomalyResult

	// 1. Detect anomalies before downsampling (on full data)
	if input.AnomalyDetection != "" && input.AnomalyDetection != "none" {
		anomalies = s.processor.DetectAnomalies(results, input.AnomalyDetection, input.AnomalyThreshold, input.AnomalyField)
	}

	// 2. Apply downsampling if specified
	if input.Downsampling != "" && input.Downsampling != "none" {
		results = s.processor.ApplyDownsampling(results, input.Downsampling, input.DownsamplingThreshold)
	}

	// 3. Convert times to requested output timezone
	outputLocation := input.StartTimeParsed.Location()
	if outputLocation != nil {
		results = s.processor.ConvertTimezone(results, outputLocation)
		if len(anomalies) > 0 {
			anomalies = s.processor.ConvertAnomalyTimezone(anomalies, outputLocation)
		}
	}

	latency := time.Since(startTime)
	s.logger.Info("Query completed",
		"database", input.Database,
		"collection", input.Collection,
		"devices", len(results),
		"interval", input.Interval,
		"aggregation", input.Aggregation,
		"anomalies", len(anomalies),
		"latency_ms", latency.Milliseconds())

	return &QueryResult{
		Database:   input.Database,
		Collection: input.Collection,
		StartTime:  input.StartTimeParsed.Format(time.RFC3339),
		EndTime:    input.EndTimeParsed.Format(time.RFC3339),
		Results:    s.processingToResponse(results),
		Anomalies:  anomalies,
	}, nil
}

// coordinatorToProcessing converts coordinator results to processing results
func (s *QueryService) coordinatorToProcessing(results []coordinator.FormattedQueryResult) []processing.QueryResult {
	converted := make([]processing.QueryResult, len(results))
	for i, r := range results {
		converted[i] = processing.QueryResult{
			DeviceID: r.DeviceID,
			Times:    r.Times,
			Fields:   r.Fields,
		}
	}
	return converted
}

// processingToResponse converts processing results back for JSON serialization
func (s *QueryService) processingToResponse(results []processing.QueryResult) []coordinator.FormattedQueryResult {
	converted := make([]coordinator.FormattedQueryResult, len(results))
	for i, r := range results {
		converted[i] = coordinator.FormattedQueryResult{
			DeviceID: r.DeviceID,
			Times:    r.Times,
			Fields:   r.Fields,
		}
	}
	return converted
}
