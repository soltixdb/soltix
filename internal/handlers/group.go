package handlers

import (
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/models"
)

// ListGroups returns all group assignments
func (h *Handler) ListGroups(c *fiber.Ctx) error {
	groupManager := h.shardRouter.GetGroupManager()

	groups, err := groupManager.ListGroups(c.Context())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to list groups: " + err.Error(),
			},
		})
	}

	return c.JSON(fiber.Map{
		"groups": groups,
		"count":  len(groups),
	})
}

// GetGroup returns a single group assignment
func (h *Handler) GetGroup(c *fiber.Ctx) error {
	groupIDStr := c.Params("group_id")
	groupID, err := strconv.Atoi(groupIDStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "group_id must be an integer",
			},
		})
	}

	groupManager := h.shardRouter.GetGroupManager()
	group, err := groupManager.GetGroup(c.Context(), groupID)
	if err != nil || group == nil {
		return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "NOT_FOUND",
				Message: "Group not found",
			},
		})
	}

	return c.JSON(group)
}

// GetNodeGroups returns all groups that a specific node belongs to
func (h *Handler) GetNodeGroups(c *fiber.Ctx) error {
	nodeID := c.Params("node_id")
	if nodeID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "node_id is required",
			},
		})
	}

	groupManager := h.shardRouter.GetGroupManager()
	groups, err := groupManager.GetGroupsForNode(c.Context(), nodeID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: err.Error(),
			},
		})
	}

	return c.JSON(fiber.Map{
		"node_id": nodeID,
		"groups":  groups,
		"count":   len(groups),
	})
}

// LookupDeviceGroup returns which group a device belongs to
func (h *Handler) LookupDeviceGroup(c *fiber.Ctx) error {
	database := c.Params("database")
	collection := c.Params("collection")
	deviceID := c.Params("device_id")

	if deviceID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "device_id path parameter is required",
			},
		})
	}

	groupManager := h.shardRouter.GetGroupManager()
	groupAssignment, err := groupManager.RouteWrite(c.Context(), database, collection, deviceID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "ROUTING_ERROR",
				Message: err.Error(),
			},
		})
	}

	return c.JSON(fiber.Map{
		"database":    database,
		"collection":  collection,
		"device_id":   deviceID,
		"group_id":    groupAssignment.GroupID,
		"primary":     groupAssignment.PrimaryNode,
		"replicas":    groupAssignment.ReplicaNodes,
		"state":       groupAssignment.State,
		"total_nodes": len(groupAssignment.GetAllNodes()),
	})
}
