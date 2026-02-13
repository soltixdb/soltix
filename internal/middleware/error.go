package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/models"
)

// ErrorHandler returns a custom error handler middleware
func ErrorHandler(logger *logging.Logger) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError
		message := "Internal Server Error"

		if e, ok := err.(*fiber.Error); ok {
			code = e.Code
			message = e.Message
		}

		logger.Error("Request error",
			"path", c.Path(),
			"method", c.Method(),
			"status", code,
			"error", err,
		)

		return c.Status(code).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "ERROR",
				Message: message,
			},
		})
	}
}
