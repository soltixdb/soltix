package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/handlers/processing"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

// StreamWriter defines the interface for writing chunks to stream
type StreamWriter interface {
	WriteChunk(chunk interface{}) error
	WriteEvent(eventType string, data interface{}) error
	Flush() error
}

// StreamQueryService handles streaming query business logic
type StreamQueryService struct {
	logger           *logging.Logger
	metadataManager  metadata.Manager
	queryCoordinator *coordinator.QueryCoordinator
	processor        *processing.Processor
}

// NewStreamQueryService creates a new StreamQueryService
func NewStreamQueryService(
	logger *logging.Logger,
	metadataManager metadata.Manager,
	queryCoordinator *coordinator.QueryCoordinator,
	processor *processing.Processor,
) *StreamQueryService {
	return &StreamQueryService{
		logger:           logger,
		metadataManager:  metadataManager,
		queryCoordinator: queryCoordinator,
		processor:        processor,
	}
}

// StreamChunk represents a chunk of query results
type StreamChunk struct {
	ChunkID    int                                `json:"chunk_id"`
	Database   string                             `json:"database"`
	Collection string                             `json:"collection"`
	StartTime  string                             `json:"start_time"`
	EndTime    string                             `json:"end_time"`
	Results    []coordinator.FormattedQueryResult `json:"results"`
	TotalCount int                                `json:"total_count"`
	IsFinal    bool                               `json:"is_final"`
}

// ExecuteStream performs a streaming query with chunked results
func (s *StreamQueryService) ExecuteStream(
	ctx context.Context,
	input *models.StreamQueryRequest,
	writer StreamWriter,
) error {
	startTime := time.Now()

	// Validate collection existence
	if err := s.metadataManager.ValidateCollection(ctx, input.Database, input.Collection); err != nil {
		return &ServiceError{
			Code:    "COLLECTION_NOT_FOUND",
			Message: err.Error(),
		}
	}

	s.logger.Info("Starting streaming query",
		"database", input.Database,
		"collection", input.Collection,
		"start_time", input.StartTime,
		"end_time", input.EndTime,
		"chunk_size", input.ChunkSize,
		"chunk_interval", input.ChunkInterval,
	)

	var totalResults int
	var err error

	// Choose streaming strategy based on chunk configuration
	if input.ChunkInterval != "" {
		// Time-based chunking
		totalResults, err = s.streamByTimeInterval(ctx, input, writer)
	} else {
		// Size-based chunking
		totalResults, err = s.streamBySize(ctx, input, writer)
	}

	if err != nil {
		latency := time.Since(startTime)
		s.logger.Error("Streaming query failed",
			"error", err,
			"latency_ms", latency.Milliseconds(),
		)
		return err
	}

	latency := time.Since(startTime)
	s.logger.Info("Streaming query completed",
		"database", input.Database,
		"collection", input.Collection,
		"total_results", totalResults,
		"latency_ms", latency.Milliseconds(),
	)

	return nil
}

// streamBySize streams results by splitting them into fixed-size chunks
func (s *StreamQueryService) streamBySize(
	ctx context.Context,
	input *models.StreamQueryRequest,
	writer StreamWriter,
) (int, error) {
	// Build coordinator request
	req := &coordinator.QueryRequest{
		Database:    input.Database,
		Collection:  input.Collection,
		DeviceIDs:   input.IDs,
		StartTime:   input.StartTimeParsed,
		EndTime:     input.EndTimeParsed,
		Fields:      input.Fields,
		Limit:       0, // No limit for streaming
		Interval:    input.Interval,
		Aggregation: input.Aggregation,
	}

	// Execute full query
	coordResults, err := s.queryCoordinator.Query(ctx, req)
	if err != nil {
		return 0, &ServiceError{
			Code:    "QUERY_FAILED",
			Message: "Failed to execute query",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}

	// Convert and process results
	results := s.coordinatorToProcessing(coordResults)

	// Apply downsampling before chunking if specified
	if input.Downsampling != "" && input.Downsampling != "none" {
		results = s.processor.ApplyDownsampling(results, input.Downsampling, input.DownsamplingThreshold)
	}

	// Convert to output timezone
	outputLocation := input.StartTimeParsed.Location()
	if outputLocation != nil {
		results = s.processor.ConvertTimezone(results, outputLocation)
	}

	// Convert back to formatted results for response
	formattedResults := s.processingToResponse(results)

	// Count total data points
	totalPoints := 0
	for _, result := range formattedResults {
		totalPoints += len(result.Times)
	}

	// Split and stream by chunk size
	chunkID := 0
	currentChunk := make([]coordinator.FormattedQueryResult, 0)
	currentChunkSize := 0

	for _, result := range formattedResults {
		// If adding this result exceeds chunk size, send current chunk
		if currentChunkSize > 0 && currentChunkSize+len(result.Times) > input.ChunkSize {
			if err := s.sendChunk(writer, chunkID, input, currentChunk, totalPoints, false); err != nil {
				return 0, err
			}
			chunkID++
			currentChunk = make([]coordinator.FormattedQueryResult, 0)
			currentChunkSize = 0
		}

		// Add result to current chunk
		currentChunk = append(currentChunk, result)
		currentChunkSize += len(result.Times)
	}

	// Send remaining chunk
	if len(currentChunk) > 0 {
		if err := s.sendChunk(writer, chunkID, input, currentChunk, totalPoints, true); err != nil {
			return 0, err
		}
	}

	return totalPoints, nil
}

// streamByTimeInterval streams results by querying time intervals sequentially
func (s *StreamQueryService) streamByTimeInterval(
	ctx context.Context,
	input *models.StreamQueryRequest,
	writer StreamWriter,
) (int, error) {
	chunkDuration := input.GetChunkDuration()
	totalResults := 0
	chunkID := 0

	// Iterate through time ranges
	currentStart := input.StartTimeParsed
	for currentStart.Before(input.EndTimeParsed) {
		currentEnd := currentStart.Add(chunkDuration)
		if currentEnd.After(input.EndTimeParsed) {
			currentEnd = input.EndTimeParsed
		}

		// Build coordinator request for this time chunk
		req := &coordinator.QueryRequest{
			Database:    input.Database,
			Collection:  input.Collection,
			DeviceIDs:   input.IDs,
			StartTime:   currentStart,
			EndTime:     currentEnd,
			Fields:      input.Fields,
			Limit:       0,
			Interval:    input.Interval,
			Aggregation: input.Aggregation,
		}

		// Execute query for this time chunk
		coordResults, err := s.queryCoordinator.Query(ctx, req)
		if err != nil {
			return totalResults, &ServiceError{
				Code:    "QUERY_FAILED",
				Message: "Failed to execute query chunk",
				Details: map[string]interface{}{
					"error":      err.Error(),
					"chunk_id":   chunkID,
					"start_time": currentStart.Format(time.RFC3339),
					"end_time":   currentEnd.Format(time.RFC3339),
				},
			}
		}

		// Process results
		results := s.coordinatorToProcessing(coordResults)

		if input.Downsampling != "" && input.Downsampling != "none" {
			results = s.processor.ApplyDownsampling(results, input.Downsampling, input.DownsamplingThreshold)
		}

		outputLocation := input.StartTimeParsed.Location()
		if outputLocation != nil {
			results = s.processor.ConvertTimezone(results, outputLocation)
		}

		formattedResults := s.processingToResponse(results)

		// Count points in this chunk
		chunkPoints := 0
		for _, result := range formattedResults {
			chunkPoints += len(result.Times)
		}
		totalResults += chunkPoints

		// Send chunk
		isFinal := currentEnd.Equal(input.EndTimeParsed) || currentEnd.After(input.EndTimeParsed)
		chunk := StreamChunk{
			ChunkID:    chunkID,
			Database:   input.Database,
			Collection: input.Collection,
			StartTime:  currentStart.Format(time.RFC3339),
			EndTime:    currentEnd.Format(time.RFC3339),
			Results:    formattedResults,
			TotalCount: chunkPoints,
			IsFinal:    isFinal,
		}

		if err := writer.WriteChunk(chunk); err != nil {
			return totalResults, err
		}
		if err := writer.Flush(); err != nil {
			return totalResults, err
		}

		chunkID++
		currentStart = currentEnd
	}

	return totalResults, nil
}

// sendChunk sends a chunk to the stream writer
func (s *StreamQueryService) sendChunk(
	writer StreamWriter,
	chunkID int,
	input *models.StreamQueryRequest,
	results []coordinator.FormattedQueryResult,
	totalPoints int,
	isFinal bool,
) error {
	chunk := StreamChunk{
		ChunkID:    chunkID,
		Database:   input.Database,
		Collection: input.Collection,
		StartTime:  input.StartTime,
		EndTime:    input.EndTime,
		Results:    results,
		TotalCount: totalPoints,
		IsFinal:    isFinal,
	}

	if err := writer.WriteChunk(chunk); err != nil {
		return err
	}
	return writer.Flush()
}

// coordinatorToProcessing converts coordinator results to processing results
func (s *StreamQueryService) coordinatorToProcessing(results []coordinator.FormattedQueryResult) []processing.QueryResult {
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
func (s *StreamQueryService) processingToResponse(results []processing.QueryResult) []coordinator.FormattedQueryResult {
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

// SSEWriter implements StreamWriter for Server-Sent Events
type SSEWriter struct {
	writer  io.Writer
	eventID int
}

// NewSSEWriter creates a new SSE writer
func NewSSEWriter(w io.Writer) *SSEWriter {
	return &SSEWriter{
		writer:  w,
		eventID: 0,
	}
}

// WriteChunk writes a chunk as an SSE event
func (w *SSEWriter) WriteChunk(chunk interface{}) error {
	w.eventID++
	return w.WriteEvent("chunk", chunk)
}

// WriteEvent writes a custom event
func (w *SSEWriter) WriteEvent(eventType string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	// SSE format:
	// id: <id>
	// event: <type>
	// data: <json>
	// \n
	_, err = fmt.Fprintf(w.writer, "id: %d\nevent: %s\ndata: %s\n\n", w.eventID, eventType, jsonData)
	return err
}

// Flush flushes the writer (no-op for basic writer, override if needed)
func (w *SSEWriter) Flush() error {
	// If the writer implements http.Flusher, call Flush()
	if flusher, ok := w.writer.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

// StreamChunkOutput represents a chunk sent to client via SSE
type StreamChunkOutput struct {
	ChunkID    int32                  `json:"chunk_id"`
	DeviceID   string                 `json:"device_id"`
	DataPoints []models.DataPointView `json:"data_points"`
	IsFinal    bool                   `json:"is_final"`
}

// ExecuteStreamWithGRPC performs a streaming query using gRPC streaming
// This is the true streaming implementation that doesn't load all data into memory
func (s *StreamQueryService) ExecuteStreamWithGRPC(
	ctx context.Context,
	input *models.StreamQueryRequest,
	writer StreamWriter,
) error {
	startTime := time.Now()

	// Validate collection existence
	if err := s.metadataManager.ValidateCollection(ctx, input.Database, input.Collection); err != nil {
		return &ServiceError{
			Code:    "COLLECTION_NOT_FOUND",
			Message: err.Error(),
		}
	}

	s.logger.Info("Starting gRPC streaming query",
		"database", input.Database,
		"collection", input.Collection,
		"start_time", input.StartTime,
		"end_time", input.EndTime,
		"chunk_size", input.ChunkSize,
	)

	// Build streaming request
	streamReq := &coordinator.StreamQueryRequest{
		QueryRequest: coordinator.QueryRequest{
			Database:    input.Database,
			Collection:  input.Collection,
			DeviceIDs:   input.IDs,
			StartTime:   input.StartTimeParsed,
			EndTime:     input.EndTimeParsed,
			Fields:      input.Fields,
			Limit:       0, // No limit for streaming
			Interval:    input.Interval,
			Aggregation: input.Aggregation,
		},
		ChunkSize: int32(input.ChunkSize),
	}

	// Get streaming channel from coordinator
	chunkChan := s.queryCoordinator.QueryStream(ctx, streamReq)

	// Send start event
	if err := writer.WriteEvent("start", map[string]interface{}{
		"database":   input.Database,
		"collection": input.Collection,
		"start_time": input.StartTime,
		"end_time":   input.EndTime,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return &ServiceError{
			Code:    "STREAM_WRITE_ERROR",
			Message: "Failed to write start event",
			Details: map[string]interface{}{"error": err.Error()},
		}
	}
	_ = writer.Flush()

	// Process chunks from channel
	totalChunks := 0
	totalPoints := 0

	for chunk := range chunkChan {
		// Check for errors
		if chunk.Error != nil {
			s.logger.Error("Error in stream chunk",
				"error", chunk.Error.Error())
			// Send error event but continue
			_ = writer.WriteEvent("error", map[string]interface{}{
				"error": chunk.Error.Error(),
			})
			_ = writer.Flush()
			continue
		}

		// Check for final marker
		if chunk.IsFinal && chunk.DeviceID == "" {
			break
		}

		// Apply processing if needed (downsampling, timezone)
		dataPoints := chunk.DataPoints
		if input.Downsampling != "" && input.Downsampling != "none" {
			// Convert to processing format
			procResult := processing.QueryResult{
				DeviceID: chunk.DeviceID,
				Times:    make([]string, len(dataPoints)),
				Fields:   make(map[string][]interface{}),
			}
			for i, dp := range dataPoints {
				procResult.Times[i] = dp.Time
				for k, v := range dp.Fields {
					if procResult.Fields[k] == nil {
						procResult.Fields[k] = make([]interface{}, len(dataPoints))
					}
					procResult.Fields[k][i] = v
				}
			}
			// Apply downsampling
			downsampled := s.processor.ApplyDownsampling([]processing.QueryResult{procResult}, input.Downsampling, input.DownsamplingThreshold)
			if len(downsampled) > 0 {
				dataPoints = s.processingResultToDataPoints(downsampled[0])
			}
		}

		// Send chunk to client
		outputChunk := StreamChunkOutput{
			ChunkID:    chunk.ChunkID,
			DeviceID:   chunk.DeviceID,
			DataPoints: dataPoints,
			IsFinal:    chunk.IsFinal,
		}

		if err := writer.WriteChunk(outputChunk); err != nil {
			return &ServiceError{
				Code:    "STREAM_WRITE_ERROR",
				Message: "Failed to write chunk",
				Details: map[string]interface{}{
					"chunk_id": chunk.ChunkID,
					"error":    err.Error(),
				},
			}
		}
		_ = writer.Flush()

		totalChunks++
		totalPoints += len(dataPoints)
	}

	// Send end event
	if err := writer.WriteEvent("end", map[string]interface{}{
		"total_chunks": totalChunks,
		"total_points": totalPoints,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		s.logger.Warn("Failed to write end event", "error", err)
	}
	_ = writer.Flush()

	latency := time.Since(startTime)
	s.logger.Info("gRPC streaming query completed",
		"database", input.Database,
		"collection", input.Collection,
		"total_chunks", totalChunks,
		"total_points", totalPoints,
		"latency_ms", latency.Milliseconds(),
	)

	return nil
}

// processingResultToDataPoints converts processing result back to DataPointView
func (s *StreamQueryService) processingResultToDataPoints(result processing.QueryResult) []models.DataPointView {
	dataPoints := make([]models.DataPointView, len(result.Times))
	for i, t := range result.Times {
		dp := models.DataPointView{
			Time:   t,
			ID:     result.DeviceID,
			Fields: make(map[string]interface{}),
		}
		for k, values := range result.Fields {
			if i < len(values) {
				dp.Fields[k] = values[i]
			}
		}
		dataPoints[i] = dp
	}
	return dataPoints
}
