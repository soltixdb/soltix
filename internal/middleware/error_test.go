package middleware

import (
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
)

func TestErrorHandler_FiberError(t *testing.T) {
	logger := logging.NewDevelopment()

	tests := []struct {
		name           string
		fiberError     *fiber.Error
		expectedStatus int
		expectedMsg    string
	}{
		{
			name:           "BadRequest error",
			fiberError:     fiber.ErrBadRequest,
			expectedStatus: fiber.StatusBadRequest,
			expectedMsg:    "Bad Request",
		},
		{
			name:           "Unauthorized error",
			fiberError:     fiber.ErrUnauthorized,
			expectedStatus: fiber.StatusUnauthorized,
			expectedMsg:    "Unauthorized",
		},
		{
			name:           "Forbidden error",
			fiberError:     fiber.ErrForbidden,
			expectedStatus: fiber.StatusForbidden,
			expectedMsg:    "Forbidden",
		},
		{
			name:           "NotFound error",
			fiberError:     fiber.ErrNotFound,
			expectedStatus: fiber.StatusNotFound,
			expectedMsg:    "Not Found",
		},
		{
			name:           "InternalServerError",
			fiberError:     fiber.ErrInternalServerError,
			expectedStatus: fiber.StatusInternalServerError,
			expectedMsg:    "Internal Server Error",
		},
		{
			name:           "ServiceUnavailable error",
			fiberError:     fiber.ErrServiceUnavailable,
			expectedStatus: fiber.StatusServiceUnavailable,
			expectedMsg:    "Service Unavailable",
		},
		{
			name:           "Custom fiber error",
			fiberError:     fiber.NewError(fiber.StatusTeapot, "I'm a teapot"),
			expectedStatus: fiber.StatusTeapot,
			expectedMsg:    "I'm a teapot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New(fiber.Config{
				ErrorHandler: ErrorHandler(logger),
			})

			app.Get("/test", func(c *fiber.Ctx) error {
				return tt.fiberError
			})

			req := httptest.NewRequest("GET", "/test", nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to test request: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}

			body, _ := io.ReadAll(resp.Body)
			var errResp models.ErrorResponse
			if err := json.Unmarshal(body, &errResp); err != nil {
				t.Fatalf("Failed to unmarshal response: %v", err)
			}

			if errResp.Error.Message != tt.expectedMsg {
				t.Errorf("Expected message %q, got %q", tt.expectedMsg, errResp.Error.Message)
			}

			if errResp.Error.Code != "ERROR" {
				t.Errorf("Expected code 'ERROR', got %q", errResp.Error.Code)
			}
		})
	}
}

func TestErrorHandler_GenericError(t *testing.T) {
	logger := logging.NewDevelopment()

	app := fiber.New(fiber.Config{
		ErrorHandler: ErrorHandler(logger),
	})

	// Handler that returns a generic Go error (not fiber.Error)
	app.Get("/test", func(c *fiber.Ctx) error {
		return errors.New("something went wrong")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Failed to test request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Generic errors should return 500 Internal Server Error
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Errorf("Expected status %d, got %d", fiber.StatusInternalServerError, resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var errResp models.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if errResp.Error.Message != "Internal Server Error" {
		t.Errorf("Expected message 'Internal Server Error', got %q", errResp.Error.Message)
	}

	if errResp.Error.Code != "ERROR" {
		t.Errorf("Expected code 'ERROR', got %q", errResp.Error.Code)
	}
}

func TestErrorHandler_ResponseFormat(t *testing.T) {
	logger := logging.NewDevelopment()

	app := fiber.New(fiber.Config{
		ErrorHandler: ErrorHandler(logger),
	})

	app.Get("/test", func(c *fiber.Ctx) error {
		return fiber.ErrBadRequest
	})

	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Failed to test request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check content type is JSON
	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got %q", contentType)
	}

	// Check response structure
	body, _ := io.ReadAll(resp.Body)
	var rawResp map[string]interface{}
	if err := json.Unmarshal(body, &rawResp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify 'error' key exists
	errorObj, exists := rawResp["error"]
	if !exists {
		t.Error("Response should have 'error' key")
	}

	// Verify error object has required fields
	errorMap, ok := errorObj.(map[string]interface{})
	if !ok {
		t.Fatal("Error object should be a map")
	}

	if _, exists := errorMap["code"]; !exists {
		t.Error("Error object should have 'code' field")
	}

	if _, exists := errorMap["message"]; !exists {
		t.Error("Error object should have 'message' field")
	}
}

func TestErrorHandler_DifferentMethods(t *testing.T) {
	logger := logging.NewDevelopment()

	app := fiber.New(fiber.Config{
		ErrorHandler: ErrorHandler(logger),
	})

	// Add handlers for different methods
	app.Get("/test", func(c *fiber.Ctx) error {
		return fiber.ErrNotFound
	})
	app.Post("/test", func(c *fiber.Ctx) error {
		return fiber.ErrBadRequest
	})
	app.Put("/test", func(c *fiber.Ctx) error {
		return fiber.ErrForbidden
	})
	app.Delete("/test", func(c *fiber.Ctx) error {
		return fiber.ErrUnauthorized
	})

	tests := []struct {
		method         string
		expectedStatus int
	}{
		{"GET", fiber.StatusNotFound},
		{"POST", fiber.StatusBadRequest},
		{"PUT", fiber.StatusForbidden},
		{"DELETE", fiber.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/test", nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to test request: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status %d for %s, got %d", tt.expectedStatus, tt.method, resp.StatusCode)
			}
		})
	}
}

func TestErrorHandler_PanicRecovery(t *testing.T) {
	logger := logging.NewDevelopment()

	app := fiber.New(fiber.Config{
		ErrorHandler: ErrorHandler(logger),
	})

	// Add recover middleware before error handler kicks in
	app.Use(func(c *fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				_ = c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "PANIC",
						Message: "Internal Server Error",
					},
				})
			}
		}()
		return c.Next()
	})

	app.Get("/panic", func(c *fiber.Ctx) error {
		panic("test panic")
	})

	req := httptest.NewRequest("GET", "/panic", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Failed to test request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Should return 500 after panic recovery
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Errorf("Expected status 500 after panic, got %d", resp.StatusCode)
	}
}
