package handlers

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/stretchr/testify/assert"
)

// setupStreamTestApp creates a simple test app for streaming endpoint tests
func setupStreamTestApp() (*fiber.App, *Handler) {
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()

	// Create test database and collection
	_ = mockMeta.CreateDatabase(context.Background(), &metadata.Database{Name: "testdb"})
	_ = mockMeta.CreateCollection(context.Background(), "testdb", &metadata.Collection{Name: "testcoll"})

	// Add a mock storage node
	mockMeta.AddStorageNode("node1", "localhost:9090")

	// Create handler with nil queue publisher (not used in query tests)
	handler := &Handler{
		logger:          logger,
		metadataManager: mockMeta,
	}

	app := fiber.New()
	return app, handler
}

func TestQueryStream_ValidationError_MissingTime(t *testing.T) {
	app, handler := setupStreamTestApp()
	app.Get("/v1/databases/:database/collections/:collection/query/stream", handler.QueryStream)

	req := httptest.NewRequest("GET", "/v1/databases/testdb/collections/testcoll/query/stream?chunk_size=1000&ids=device1", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "start_time and end_time are required")
}

func TestQueryStream_ValidationError_ChunkSizeTooSmall(t *testing.T) {
	app, handler := setupStreamTestApp()
	app.Get("/v1/databases/:database/collections/:collection/query/stream", handler.QueryStream)

	req := httptest.NewRequest("GET", "/v1/databases/testdb/collections/testcoll/query/stream?start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z&chunk_size=5&ids=device1", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "at least 10")
}

func TestQueryStream_ValidationError_ChunkSizeTooLarge(t *testing.T) {
	app, handler := setupStreamTestApp()
	app.Get("/v1/databases/:database/collections/:collection/query/stream", handler.QueryStream)

	req := httptest.NewRequest("GET", "/v1/databases/testdb/collections/testcoll/query/stream?start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z&chunk_size=20000&ids=device1", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "10000")
}

func TestQueryStreamPost_InvalidJSON(t *testing.T) {
	app, handler := setupStreamTestApp()
	app.Post("/v1/databases/:database/collections/:collection/query/stream", handler.QueryStreamPost)

	requestBody := `{"invalid json`

	req := httptest.NewRequest("POST", "/v1/databases/testdb/collections/testcoll/query/stream", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "INVALID_JSON")
}

func TestQueryStream_ValidationError_InvalidInterval(t *testing.T) {
	app, handler := setupStreamTestApp()
	app.Get("/v1/databases/:database/collections/:collection/query/stream", handler.QueryStream)

	req := httptest.NewRequest("GET", "/v1/databases/testdb/collections/testcoll/query/stream?start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z&interval=invalid&ids=device1", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "interval")
}

func TestQueryStream_ValidationError_InvalidChunkInterval(t *testing.T) {
	app, handler := setupStreamTestApp()
	app.Get("/v1/databases/:database/collections/:collection/query/stream", handler.QueryStream)

	req := httptest.NewRequest("GET", "/v1/databases/testdb/collections/testcoll/query/stream?start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z&chunk_interval=invalid&ids=device1", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "chunk_interval")
}

func TestQueryStream_ValidationError_InvalidAggregation(t *testing.T) {
	app, handler := setupStreamTestApp()
	app.Get("/v1/databases/:database/collections/:collection/query/stream", handler.QueryStream)

	req := httptest.NewRequest("GET", "/v1/databases/testdb/collections/testcoll/query/stream?start_time=2026-01-01T00:00:00Z&end_time=2026-01-02T00:00:00Z&aggregation=invalid&ids=device1", nil)
	resp, err := app.Test(req)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "aggregation")
}
