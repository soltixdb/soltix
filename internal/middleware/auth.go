package middleware

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
)

// MinAPIKeyLength is the minimum required length for API keys
const MinAPIKeyLength = 32

// ValidateAPIKey checks if an API key meets the security requirements
func ValidateAPIKey(key string) bool {
	// Key must be at least MinAPIKeyLength characters
	if len(key) < MinAPIKeyLength {
		return false
	}
	// Key must not contain only whitespace
	if strings.TrimSpace(key) == "" {
		return false
	}
	return true
}

// APIKeyAuth creates an API key authentication middleware
func APIKeyAuth(logger *logging.Logger, apiKeys []string, enabled bool) fiber.Handler {
	// If auth is disabled, allow all requests
	if !enabled {
		return func(c *fiber.Ctx) error {
			return c.Next()
		}
	}

	// Build map for O(1) lookup with validation
	keyMap := make(map[string]bool)
	for _, key := range apiKeys {
		if key != "" {
			if !ValidateAPIKey(key) {
				logger.Warn("API key does not meet security requirements",
					"key_length", len(key),
					"min_required", MinAPIKeyLength,
					"key_prefix", maskAPIKey(key),
				)
				continue
			}
			keyMap[key] = true
		}
	}

	// Warn if no valid API keys configured
	if len(keyMap) == 0 && len(apiKeys) > 0 {
		logger.Error("No valid API keys configured - all provided keys failed validation",
			"total_keys", len(apiKeys),
			"min_required_length", MinAPIKeyLength,
		)
	}

	return func(c *fiber.Ctx) error {
		// Get API key from header
		// Support multiple header formats:
		// 1. X-API-Key: your-api-key
		// 2. Authorization: Bearer your-api-key
		// 3. Authorization: your-api-key
		apiKey := c.Get("X-API-Key")
		if apiKey == "" {
			authHeader := c.Get("Authorization")
			if authHeader != "" {
				// Try "Bearer token" format
				if after, ok := strings.CutPrefix(authHeader, "Bearer "); ok {
					apiKey = after
				} else {
					// Try plain token format
					apiKey = authHeader
				}
			}
		}

		// Check if API key is valid
		if apiKey == "" {
			logger.Warn("API key missing",
				"path", c.Path(),
				"method", c.Method(),
				"ip", c.IP(),
			)
			return c.Status(fiber.StatusUnauthorized).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "UNAUTHORIZED",
					Message: "API key is required. Provide it via X-API-Key header or Authorization header.",
				},
			})
		}

		if !keyMap[apiKey] {
			logger.Warn("Invalid API key",
				"path", c.Path(),
				"method", c.Method(),
				"ip", c.IP(),
				"api_key_prefix", maskAPIKey(apiKey),
			)
			return c.Status(fiber.StatusUnauthorized).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "UNAUTHORIZED",
					Message: "Invalid API key.",
				},
			})
		}

		// Log successful authentication
		logger.Debug("API key authenticated",
			"path", c.Path(),
			"method", c.Method(),
			"ip", c.IP(),
		)

		return c.Next()
	}
}

// maskAPIKey masks API key for logging (show only first 4 chars)
func maskAPIKey(key string) string {
	if len(key) <= 4 {
		return "****"
	}
	return key[:4] + "****"
}
