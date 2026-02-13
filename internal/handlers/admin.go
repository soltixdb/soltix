package handlers

import (
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
)

// TriggerFlush triggers flush on all storage nodes via NATS
func (h *Handler) TriggerFlush(c *fiber.Ctx) error {
	// Publish flush trigger message to all storage nodes
	subject := "soltix.admin.flush.trigger"
	
	message := map[string]interface{}{
		"action":    "flush",
		"timestamp": time.Now().Unix(),
	}
	
	data, err := json.Marshal(message)
	if err != nil {
		h.logger.Error("Failed to marshal flush trigger message", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}
	
	if err := h.queuePublisher.Publish(c.Context(), subject, data); err != nil {
		h.logger.Error("Failed to publish flush trigger",
			"subject", subject,
			"error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}
	
	h.logger.Info("Flush trigger published to all storage nodes", "subject", subject)
	
	return c.JSON(fiber.Map{
		"success": true,
		"message": "Flush triggered on all storage nodes",
		"subject": subject,
	})
}
