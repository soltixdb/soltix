package middleware

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
)

// generateAPIKey generates a valid API key of specified length
func generateAPIKey(length int) string {
	key := make([]byte, length)
	for i := range key {
		key[i] = 'a' + byte(i%26)
	}
	return string(key)
}

func TestValidateAPIKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{
			name:     "valid key - exactly 32 chars",
			key:      generateAPIKey(32),
			expected: true,
		},
		{
			name:     "valid key - longer than 32 chars",
			key:      generateAPIKey(64),
			expected: true,
		},
		{
			name:     "invalid key - too short (1 char)",
			key:      "a",
			expected: false,
		},
		{
			name:     "invalid key - too short (31 chars)",
			key:      generateAPIKey(31),
			expected: false,
		},
		{
			name:     "invalid key - empty string",
			key:      "",
			expected: false,
		},
		{
			name:     "invalid key - only spaces (less than 32)",
			key:      "          ",
			expected: false,
		},
		{
			name:     "invalid key - 32 spaces",
			key:      "                                ",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateAPIKey(tt.key)
			if result != tt.expected {
				t.Errorf("ValidateAPIKey(%q) = %v, want %v", tt.key, result, tt.expected)
			}
		})
	}
}

func TestMaskAPIKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "long key",
			key:      "abcdefghijklmnop",
			expected: "abcd****",
		},
		{
			name:     "exactly 4 chars",
			key:      "abcd",
			expected: "****",
		},
		{
			name:     "short key (3 chars)",
			key:      "abc",
			expected: "****",
		},
		{
			name:     "empty key",
			key:      "",
			expected: "****",
		},
		{
			name:     "5 chars",
			key:      "abcde",
			expected: "abcd****",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := maskAPIKey(tt.key)
			if result != tt.expected {
				t.Errorf("maskAPIKey(%q) = %q, want %q", tt.key, result, tt.expected)
			}
		})
	}
}

func TestAPIKeyAuth_Disabled(t *testing.T) {
	logger := logging.NewDevelopment()
	app := fiber.New()

	// Auth disabled - should allow all requests
	app.Use(APIKeyAuth(logger, []string{}, false))
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("OK")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Failed to test request: %v", err)
	}

	if resp.StatusCode != fiber.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestAPIKeyAuth_ValidKey(t *testing.T) {
	logger := logging.NewDevelopment()
	validKey := generateAPIKey(32) // 32 char key

	app := fiber.New()
	app.Use(APIKeyAuth(logger, []string{validKey}, true))
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("OK")
	})

	tests := []struct {
		name       string
		headerName string
		headerVal  string
		wantStatus int
	}{
		{
			name:       "X-API-Key header",
			headerName: "X-API-Key",
			headerVal:  validKey,
			wantStatus: fiber.StatusOK,
		},
		{
			name:       "Authorization Bearer header",
			headerName: "Authorization",
			headerVal:  "Bearer " + validKey,
			wantStatus: fiber.StatusOK,
		},
		{
			name:       "Authorization plain header",
			headerName: "Authorization",
			headerVal:  validKey,
			wantStatus: fiber.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set(tt.headerName, tt.headerVal)

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to test request: %v", err)
			}

			if resp.StatusCode != tt.wantStatus {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("Expected status %d, got %d, body: %s", tt.wantStatus, resp.StatusCode, string(body))
			}
		})
	}
}

func TestAPIKeyAuth_InvalidKey(t *testing.T) {
	logger := logging.NewDevelopment()
	validKey := generateAPIKey(32)

	app := fiber.New()
	app.Use(APIKeyAuth(logger, []string{validKey}, true))
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("OK")
	})

	tests := []struct {
		name       string
		headerName string
		headerVal  string
		wantStatus int
	}{
		{
			name:       "missing API key",
			headerName: "",
			headerVal:  "",
			wantStatus: fiber.StatusUnauthorized,
		},
		{
			name:       "wrong API key",
			headerName: "X-API-Key",
			headerVal:  generateAPIKey(32) + "wrong",
			wantStatus: fiber.StatusUnauthorized,
		},
		{
			name:       "short API key in request",
			headerName: "X-API-Key",
			headerVal:  "short",
			wantStatus: fiber.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.headerName != "" {
				req.Header.Set(tt.headerName, tt.headerVal)
			}

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Failed to test request: %v", err)
			}

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("Expected status %d, got %d", tt.wantStatus, resp.StatusCode)
			}
		})
	}
}

func TestAPIKeyAuth_WeakKeysRejected(t *testing.T) {
	logger := logging.NewDevelopment()

	// Try to configure with weak keys - they should be rejected
	weakKeys := []string{
		"a",                // 1 char
		"short",            // 5 chars
		generateAPIKey(31), // 31 chars - just under limit
	}

	app := fiber.New()
	app.Use(APIKeyAuth(logger, weakKeys, true))
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("OK")
	})

	// Even if we send one of the weak keys, it should be rejected
	// because they weren't added to the valid key map
	for _, weakKey := range weakKeys {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-API-Key", weakKey)

		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Failed to test request: %v", err)
		}

		if resp.StatusCode != fiber.StatusUnauthorized {
			t.Errorf("Weak key %q (len=%d) should be rejected, got status %d",
				maskAPIKey(weakKey), len(weakKey), resp.StatusCode)
		}
	}
}

func TestMinAPIKeyLength(t *testing.T) {
	if MinAPIKeyLength != 32 {
		t.Errorf("MinAPIKeyLength should be 32, got %d", MinAPIKeyLength)
	}
}
