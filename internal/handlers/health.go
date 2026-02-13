package handlers

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/models"
)

// Health handles health check requests
func (h *Handler) Health(c *fiber.Ctx) error {
	return c.JSON(models.HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().Format(time.RFC3339),
		Version:   "1.0.0",
	})
}

// NotFound handles 404 errors
func (h *Handler) NotFound(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
		Error: models.ErrorDetail{
			Code:    "NOT_FOUND",
			Message: "Route not found",
			Path:    c.Path(),
		},
	})
}
