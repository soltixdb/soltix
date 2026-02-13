package services

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/handlers/processing"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestSSEWriter_WriteChunk(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	chunk := StreamChunk{
		ChunkID:    1,
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Results:    []coordinator.FormattedQueryResult{},
		TotalCount: 0,
		IsFinal:    false,
	}

	err := writer.WriteChunk(chunk)
	assert.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "id: 1")
	assert.Contains(t, output, "event: chunk")
	assert.Contains(t, output, "data:")
	assert.Contains(t, output, "testdb")

	// Verify JSON structure
	lines := strings.Split(output, "\n")
	var dataLine string
	for _, line := range lines {
		if strings.HasPrefix(line, "data:") {
			dataLine = strings.TrimPrefix(line, "data: ")
			break
		}
	}

	var parsedChunk StreamChunk
	err = json.Unmarshal([]byte(dataLine), &parsedChunk)
	assert.NoError(t, err)
	assert.Equal(t, 1, parsedChunk.ChunkID)
	assert.Equal(t, "testdb", parsedChunk.Database)
	assert.Equal(t, "testcoll", parsedChunk.Collection)
}

func TestSSEWriter_WriteEvent(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	data := map[string]interface{}{
		"status": "started",
		"time":   "2026-01-01T00:00:00Z",
	}

	err := writer.WriteEvent("start", data)
	assert.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "event: start")
	assert.Contains(t, output, "data:")
	assert.Contains(t, output, "started")
}

func TestSSEWriter_MultipleChunks(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	// Write multiple chunks
	for i := 0; i < 3; i++ {
		chunk := StreamChunk{
			ChunkID:    i,
			Database:   "testdb",
			Collection: "testcoll",
			IsFinal:    i == 2,
		}
		err := writer.WriteChunk(chunk)
		assert.NoError(t, err)
	}

	output := buf.String()
	// Each chunk should have its own id
	assert.Contains(t, output, "id: 1")
	assert.Contains(t, output, "id: 2")
	assert.Contains(t, output, "id: 3")
}

func TestStreamQueryService_New(t *testing.T) {
	// Test that the service can be created
	mockMeta := NewMockMetadataManager()
	service := createTestStreamQueryService(mockMeta)

	assert.NotNil(t, service)
	assert.NotNil(t, service.logger)
	assert.NotNil(t, service.metadataManager)
	assert.NotNil(t, service.queryCoordinator)
	assert.NotNil(t, service.processor)
}

// createTestStreamQueryService creates a StreamQueryService for testing
func createTestStreamQueryService(mockMeta *MockMetadataManager) *StreamQueryService {
	logger := logging.NewDevelopment()
	cfg := config.CoordinatorConfig{
		ShardKeyFormat: "monthly",
		ReplicaFactor:  2,
	}
	shardRouter := coordinator.NewShardRouter(logger, mockMeta, cfg)
	queryCoord := coordinator.NewQueryCoordinator(logger, mockMeta, shardRouter)
	processor := processing.NewProcessor(logger)

	return NewStreamQueryService(logger, mockMeta, queryCoord, processor)
}

func TestStreamQueryService_ExecuteStream_CollectionNotFound(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"nonexistent", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{}, []string{}, 0, "", "", "", 0, "", 0, "",
		100, "",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.Error(t, err)

	svcErr, ok := err.(*ServiceError)
	assert.True(t, ok)
	assert.Equal(t, "COLLECTION_NOT_FOUND", svcErr.Code)
}

func TestStreamQueryService_ExecuteStream_BySize(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{"device1"}, []string{}, 0, "", "", "", 0, "", 0, "",
		100, "",
	)

	// Should not error even with no data
	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestStreamQueryService_ExecuteStream_ByTimeInterval(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{"device1"}, []string{}, 0, "", "", "", 0, "", 0, "",
		100, "1h",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestStreamQueryService_ExecuteStream_WithDownsampling(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{"device1"}, []string{}, 0, "", "", "lttb", 50, "", 0, "",
		100, "",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestStreamQueryService_ExecuteStream_WithFields(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{"device1"}, []string{"temperature", "humidity"}, 0, "", "", "", 0, "", 0, "",
		100, "",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestStreamQueryService_ExecuteStream_WithIDs(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{"device1", "device2"}, []string{}, 0, "", "", "", 0, "", 0, "",
		100, "",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestStreamQueryService_ExecuteStream_SmallChunkSize(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{"device1"}, []string{}, 0, "", "", "", 0, "", 0, "",
		1, "",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestStreamQueryService_ExecuteStream_LargeChunkSize(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{"device1"}, []string{}, 0, "", "", "", 0, "", 0, "",
		100000, "",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestStreamQueryService_ExecuteStream_EmptyTimeRange(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	now := time.Now()
	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		now.Format(time.RFC3339), now.Format(time.RFC3339),
		[]string{"device1"}, []string{}, 0, "", "", "", 0, "", 0, "",
		100, "",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestStreamQueryService_ExecuteStream_WithAggregation(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	mockMeta.SetupDatabaseAndCollection("testdb", "testcoll")
	service := createTestStreamQueryService(mockMeta)

	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	input := models.NewStreamQueryRequest(
		"testdb", "testcoll",
		"2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z",
		[]string{"device1"}, []string{}, 0, "1h", "avg", "", 0, "", 0, "",
		100, "",
	)

	err := service.ExecuteStream(context.Background(), input, writer)
	assert.NoError(t, err)
}

func TestSSEWriter_Flush_WithFlusher(t *testing.T) {
	// Test that Flush works with a writer that implements Flush
	type flusherWriter struct {
		*bytes.Buffer
	}

	fw := &flusherWriter{Buffer: &bytes.Buffer{}}
	writer := NewSSEWriter(fw.Buffer)
	err := writer.Flush()
	assert.NoError(t, err)
}

func TestSSEWriter_WriteEvent_InvalidJSON(t *testing.T) {
	buf := &bytes.Buffer{}
	writer := NewSSEWriter(buf)

	// Try to write data that cannot be marshaled to JSON
	// In Go, channels cannot be marshaled
	invalidData := make(chan int)

	err := writer.WriteEvent("test", invalidData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "marshal")
}

func TestStreamQueryService_CoordinatorToProcessing(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestStreamQueryService(mockMeta)

	coordResults := []coordinator.FormattedQueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"},
			Fields: map[string][]interface{}{
				"temperature": {20.5, 21.0},
			},
		},
		{
			DeviceID: "device2",
			Times:    []string{"2026-01-01T00:00:00Z"},
			Fields: map[string][]interface{}{
				"humidity": {65.0},
			},
		},
	}

	procResults := service.coordinatorToProcessing(coordResults)

	assert.Len(t, procResults, 2)
	assert.Equal(t, "device1", procResults[0].DeviceID)
	assert.Len(t, procResults[0].Times, 2)
	assert.Equal(t, "device2", procResults[1].DeviceID)
	assert.Len(t, procResults[1].Times, 1)
}

func TestStreamQueryService_ProcessingToResponse(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestStreamQueryService(mockMeta)

	procResults := []processing.QueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2026-01-01T00:00:00Z"},
			Fields: map[string][]interface{}{
				"temperature": {25.0},
			},
		},
	}

	coordResults := service.processingToResponse(procResults)

	assert.Len(t, coordResults, 1)
	assert.Equal(t, "device1", coordResults[0].DeviceID)
	assert.Len(t, coordResults[0].Times, 1)
}

func TestStreamQueryService_CoordinatorToProcessing_EmptyResults(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestStreamQueryService(mockMeta)

	coordResults := []coordinator.FormattedQueryResult{}
	procResults := service.coordinatorToProcessing(coordResults)

	assert.Len(t, procResults, 0)
}

func TestStreamQueryService_ProcessingToResponse_EmptyResults(t *testing.T) {
	mockMeta := NewMockMetadataManager()
	service := createTestStreamQueryService(mockMeta)

	procResults := []processing.QueryResult{}
	coordResults := service.processingToResponse(procResults)

	assert.Len(t, coordResults, 0)
}
