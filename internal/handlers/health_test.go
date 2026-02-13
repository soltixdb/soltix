package handlers

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
)

func TestHandler_Health(t *testing.T) {
	// Setup
	logger := logging.NewDevelopment()
	handler := &Handler{
		logger: logger,
	}

	app := fiber.New()
	app.Get("/health", handler.Health)

	// Test
	req := httptest.NewRequest("GET", "/health", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	// Assertions
	if resp.StatusCode != fiber.StatusOK {
		t.Errorf("Expected status %d, got %d", fiber.StatusOK, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	var healthResp models.HealthResponse
	if err := json.Unmarshal(body, &healthResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if healthResp.Status != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", healthResp.Status)
	}

	if healthResp.Version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", healthResp.Version)
	}

	if healthResp.Timestamp == "" {
		t.Error("Expected non-empty timestamp")
	}
}

func TestHandler_NotFound(t *testing.T) {
	// Setup
	logger := logging.NewDevelopment()
	handler := &Handler{
		logger: logger,
	}

	app := fiber.New()
	app.Use(handler.NotFound)

	// Test
	req := httptest.NewRequest("GET", "/nonexistent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Failed to perform request: %v", err)
	}

	// Assertions
	if resp.StatusCode != fiber.StatusNotFound {
		t.Errorf("Expected status %d, got %d", fiber.StatusNotFound, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	var errResp models.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if errResp.Error.Code != "NOT_FOUND" {
		t.Errorf("Expected error code 'NOT_FOUND', got '%s'", errResp.Error.Code)
	}

	if errResp.Error.Message != "Route not found" {
		t.Errorf("Expected message 'Route not found', got '%s'", errResp.Error.Message)
	}

	if errResp.Error.Path != "/nonexistent" {
		t.Errorf("Expected path '/nonexistent', got '%s'", errResp.Error.Path)
	}
}
