package logging

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// FiberMiddleware returns a Fiber middleware for request logging
func FiberMiddleware(logger *Logger) fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()

		// Generate request ID
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
			c.Set("X-Request-ID", requestID)
		}

		// Add logger and request ID to context
		ctx := c.UserContext()
		ctx = WithRequestID(ctx, requestID)
		ctx = WithLogger(ctx, logger)
		c.SetUserContext(ctx)

		// Process request
		err := c.Next()

		// Log request
		duration := time.Since(start)
		statusCode := c.Response().StatusCode()

		k1, v1 := String("method", c.Method())
		k2, v2 := String("path", c.Path())
		k3, v3 := String("ip", c.IP())
		k4, v4 := Int("status", statusCode)
		k5, v5 := Duration("duration", duration)
		k6, v6 := String("request_id", requestID)

		fields := []interface{}{k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6}

		// Add error if exists
		if err != nil {
			kErr, vErr := Err(err)
			fields = append(fields, kErr, vErr)
			logger.Error("Request failed", fields...)
			return err
		}

		// Log based on status code
		if statusCode >= 500 {
			logger.Error("Server error", fields...)
		} else if statusCode >= 400 {
			logger.Warn("Client error", fields...)
		} else {
			logger.Info("Request completed", fields...)
		}

		return nil
	}
}

// FiberMiddlewareWithConfig returns a Fiber middleware with custom config
func FiberMiddlewareWithConfig(logger *Logger, cfg MiddlewareConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Skip logging for excluded paths
		for _, path := range cfg.SkipPaths {
			if c.Path() == path {
				return c.Next()
			}
		}

		start := time.Now()

		// Generate request ID
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
			c.Set("X-Request-ID", requestID)
		}

		// Add logger and request ID to context
		ctx := c.UserContext()
		ctx = WithRequestID(ctx, requestID)
		ctx = WithLogger(ctx, logger)
		c.SetUserContext(ctx)

		// Process request
		err := c.Next()

		// Log request
		duration := time.Since(start)
		statusCode := c.Response().StatusCode()

		k1, v1 := String("method", c.Method())
		k2, v2 := String("path", c.Path())
		k3, v3 := String("ip", c.IP())
		k4, v4 := Int("status", statusCode)
		k5, v5 := Duration("duration", duration)
		k6, v6 := Int64("duration_ms", duration.Milliseconds())
		k7, v7 := String("request_id", requestID)

		fields := []interface{}{k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7}

		// Add custom fields if provided
		if cfg.AdditionalFields != nil {
			fields = append(fields, cfg.AdditionalFields(c)...)
		}

		// Add error if exists
		if err != nil {
			kErr, vErr := Err(err)
			fields = append(fields, kErr, vErr)
			logger.Error("Request failed", fields...)
			return err
		}

		// Log based on status code
		if statusCode >= 500 {
			logger.Error("Server error", fields...)
		} else if statusCode >= 400 {
			logger.Warn("Client error", fields...)
		} else {
			logger.Info("Request completed", fields...)
		}

		return nil
	}
}

// MiddlewareConfig defines configuration for logging middleware
type MiddlewareConfig struct {
	// SkipPaths defines paths to skip logging
	SkipPaths []string

	// AdditionalFields adds custom fields to log entries
	AdditionalFields func(c *fiber.Ctx) []interface{}
}

// DefaultMiddlewareConfig returns default middleware configuration
func DefaultMiddlewareConfig() MiddlewareConfig {
	return MiddlewareConfig{
		SkipPaths: []string{"/health", "/metrics"},
	}
}
