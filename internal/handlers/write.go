package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/models"
	"github.com/soltixdb/soltix/internal/queue"
	"github.com/soltixdb/soltix/internal/utils"
)

// WriteMessage represents the message published to NATS
type WriteMessage struct {
	Database   string                 `json:"database"`
	Collection string                 `json:"collection"`
	GroupID    int                    `json:"group_id"`
	Nodes      []string               `json:"nodes"`
	Time       string                 `json:"time"`
	ID         string                 `json:"id"`
	Fields     map[string]interface{} `json:"fields"`
}

// Buffer pool for JSON marshaling - reduces allocations in batch writes
var jsonBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

// inferFieldType infers the type of a field value
func inferFieldType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch v := value.(type) {
	case bool:
		return "bool"
	case float64, float32:
		return "float"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "int"
	case string:
		return "string"
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	default:
		// Use reflection for other types
		return reflect.TypeOf(v).String()
	}
}

// trackMetadataAsync tracks device ID and field schemas asynchronously
func (h *Handler) trackMetadataAsync(database, collection, deviceID string, fields map[string]interface{}) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), utils.MetadataTrackingTimeout)
		defer cancel()

		// Track device ID
		if err := h.metadataManager.TrackDeviceID(ctx, database, collection, deviceID); err != nil {
			h.logger.Warn("Failed to track device ID",
				"error", err,
				"database", database,
				"collection", collection,
				"device_id", deviceID)
		}

		// Build field schemas map
		fieldSchemas := make(map[string]string, len(fields))
		for fieldName, fieldValue := range fields {
			fieldSchemas[fieldName] = inferFieldType(fieldValue)
		}

		// Track all fields at once (batch operation)
		if err := h.metadataManager.TrackFields(ctx, database, collection, fieldSchemas); err != nil {
			h.logger.Warn("Failed to track fields",
				"error", err,
				"database", database,
				"collection", collection,
				"field_count", len(fieldSchemas))
		}
	}()
}

// Write handles single data point write
func (h *Handler) Write(c *fiber.Ctx) error {
	database := c.Params("database")
	collection := c.Params("collection")

	// Parse body directly into map for dynamic fields
	var body map[string]interface{}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Failed to parse request body: " + err.Error(),
			},
		})
	}

	// Extract and validate required fields
	timeStr, ok := body["time"].(string)
	if !ok || timeStr == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "'time' field is required and must be a string",
			},
		})
	}

	idStr, ok := body["id"].(string)
	if !ok || idStr == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "'id' field is required and must be a string",
			},
		})
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_TIME_FORMAT",
				Message: "Time must be in RFC3339 format",
			},
		})
	}

	// Extract fields (all except "time" and "id")
	fields := make(map[string]interface{}, len(body)-2)
	for k, v := range body {
		if k != "time" && k != "id" {
			fields[k] = v
		}
	}

	// Validate database and collection exist
	validateCtx, validateCancel := context.WithTimeout(c.Context(), utils.ValidationTimeout)
	defer validateCancel()

	if err := h.metadataManager.ValidateCollection(validateCtx, database, collection); err != nil {
		return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "COLLECTION_NOT_FOUND",
				Message: err.Error(),
			},
		})
	}

	// Update FirstDataTime if this is the first data point
	go h.updateFirstDataTime(database, collection, timestamp)

	// Track device ID and field schemas asynchronously
	h.trackMetadataAsync(database, collection, idStr, fields)

	// Route to group based on (db, collection, device_id)
	routeCtx, routeCancel := context.WithTimeout(c.Context(), utils.RouteTimeout)
	defer routeCancel()

	groupAssignment, err := h.shardRouter.RouteWriteByDevice(routeCtx, database, collection, idStr)
	if err != nil {
		h.logger.Error("Failed to route write request",
			"error", err,
			"database", database,
			"collection", collection,
			"device_id", idStr)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "ROUTING_ERROR",
				Message: "Failed to route write request: " + err.Error(),
			},
		})
	}

	// Get all nodes for replication from group
	nodes := groupAssignment.GetAllNodes()

	// Create write message
	writeMsg := WriteMessage{
		Database:   database,
		Collection: collection,
		GroupID:    groupAssignment.GroupID,
		Nodes:      nodes,
		Time:       timeStr,
		ID:         idStr,
		Fields:     fields,
	}

	// Serialize message once
	msgData, err := json.Marshal(writeMsg)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to serialize message",
			},
		})
	}

	// Publish to each node separately using node-specific subjects
	// This allows NATS to route messages only to relevant storage nodes
	// Track success/failure for each node to provide accurate response
	var successNodes []string
	var failedNodes []string

	for _, nodeID := range nodes {
		subject := fmt.Sprintf("soltix.write.node.%s", nodeID)
		if err := h.queuePublisher.Publish(c.Context(), subject, msgData); err != nil {
			h.logger.Error("Failed to publish write message to queue",
				"error", err,
				"subject", subject,
				"node", nodeID)
			failedNodes = append(failedNodes, nodeID)
			continue
		}
		successNodes = append(successNodes, nodeID)
		h.logger.Debug("Write request published to NATS",
			"database", database,
			"collection", collection,
			"group_id", groupAssignment.GroupID,
			"node", nodeID,
			"subject", subject)
	}

	// If all nodes failed, return error
	if len(successNodes) == 0 {
		h.logger.Error("Failed to publish to any node",
			"database", database,
			"collection", collection,
			"group_id", groupAssignment.GroupID,
			"total_nodes", len(nodes),
			"failed_nodes", failedNodes)
		return c.Status(fiber.StatusServiceUnavailable).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "QUEUE_UNAVAILABLE",
				Message: "Failed to queue write request to any storage node",
			},
		})
	}

	// Build response based on success/failure
	response := fiber.Map{
		"group_id":      groupAssignment.GroupID,
		"success_nodes": successNodes,
	}

	// If some nodes failed, return partial success with warning
	if len(failedNodes) > 0 {
		h.logger.Warn("Partial write success - some nodes failed",
			"database", database,
			"collection", collection,
			"group_id", groupAssignment.GroupID,
			"success_nodes", successNodes,
			"failed_nodes", failedNodes)
		response["status"] = "partial"
		response["failed_nodes"] = failedNodes
		response["message"] = fmt.Sprintf("Write queued to %d/%d nodes. Some replicas may be missing.", len(successNodes), len(nodes))
		return c.Status(fiber.StatusAccepted).JSON(response)
	}

	// All nodes succeeded
	response["status"] = "accepted"
	response["message"] = "Write request accepted and queued for processing"
	return c.Status(fiber.StatusAccepted).JSON(response)
}

// WriteBatch handles batch write with optimized performance
// Optimizations:
// 1. Pre-group points by nodeID to minimize route calculations
// 2. Track metadata once per unique device instead of per point
// 3. Use batch async NATS publish for efficient network usage
// 4. Use buffer pool for JSON marshaling to reduce allocations
func (h *Handler) WriteBatch(c *fiber.Ctx) error {
	database := c.Params("database")
	collection := c.Params("collection")

	// Parse request body
	var req models.WriteBatchRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Failed to parse request body: " + err.Error(),
			},
		})
	}

	if len(req.Points) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Points array cannot be empty",
			},
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), utils.BatchWriteTimeout)
	defer cancel()

	// Validate database and collection exist
	if err := h.metadataManager.ValidateCollection(ctx, database, collection); err != nil {
		return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "COLLECTION_NOT_FOUND",
				Message: err.Error(),
			},
		})
	}

	// Phase 1: Parse and validate all points, group by group (device-based routing)
	var earliestTimestamp *time.Time
	type GroupBatch struct {
		Assignment *coordinator.GroupAssignment
		Points     []WriteMessage
	}
	groupBatches := make(map[int]*GroupBatch) // groupID -> batch

	// Track unique devices for metadata (1 per device instead of 1 per point)
	seenDevices := make(map[string]map[string]interface{}) // deviceID -> merged fields

	// Route cache to avoid redundant route calculations for same device
	groupCache := make(map[string]*coordinator.GroupAssignment) // deviceID -> group

	for _, point := range req.Points {
		// Validate required fields
		timeStr, ok := point["time"].(string)
		if !ok {
			return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "INVALID_REQUEST",
					Message: "Each point must have a 'time' field (string)",
				},
			})
		}

		idStr, ok := point["id"].(string)
		if !ok {
			return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "INVALID_REQUEST",
					Message: "Each point must have an 'id' field (string)",
				},
			})
		}

		// Parse timestamp
		timestamp, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "INVALID_TIME_FORMAT",
					Message: "Time must be in RFC3339 format",
				},
			})
		}

		// Track earliest timestamp
		if earliestTimestamp == nil || timestamp.Before(*earliestTimestamp) {
			earliestTimestamp = &timestamp
		}

		// Extract fields (all except "time" and "id")
		fields := make(map[string]interface{}, len(point)-2)
		for k, v := range point {
			if k != "time" && k != "id" {
				fields[k] = v
			}
		}

		// Track unique device and field names for metadata (don't share map reference!)
		if existingFields, exists := seenDevices[idStr]; exists {
			// Merge new field names into existing (for metadata tracking only)
			for k, v := range fields {
				existingFields[k] = v
			}
		} else {
			// Copy fields to avoid sharing reference with WriteMessage.Fields
			fieldsCopy := make(map[string]interface{}, len(fields))
			for k, v := range fields {
				fieldsCopy[k] = v
			}
			seenDevices[idStr] = fieldsCopy
		}

		// Route to group by device ID with caching
		groupAssignment, cached := groupCache[idStr]
		if !cached {
			groupAssignment, err = h.shardRouter.RouteWriteByDevice(ctx, database, collection, idStr)
			if err != nil {
				h.logger.Error("Failed to route write request",
					"error", err,
					"database", database,
					"collection", collection,
					"device_id", idStr)
				return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "ROUTING_ERROR",
						Message: "Failed to route write request: " + err.Error(),
					},
				})
			}
			groupCache[idStr] = groupAssignment
		}

		// Group by group ID
		if _, exists := groupBatches[groupAssignment.GroupID]; !exists {
			groupBatches[groupAssignment.GroupID] = &GroupBatch{
				Assignment: groupAssignment,
				Points:     make([]WriteMessage, 0, 100), // Pre-allocate
			}
		}

		groupBatches[groupAssignment.GroupID].Points = append(groupBatches[groupAssignment.GroupID].Points, WriteMessage{
			Database:   database,
			Collection: collection,
			GroupID:    groupAssignment.GroupID,
			Nodes:      groupAssignment.GetAllNodes(),
			Time:       timeStr,
			ID:         idStr,
			Fields:     fields,
		})
	}

	// Phase 2: Track metadata once per unique device (not per point)
	go func() {
		for deviceID, fields := range seenDevices {
			h.trackMetadataAsync(database, collection, deviceID, fields)
		}
	}()

	// Phase 3: Group messages by nodeID for efficient batch publish
	nodeMessages := make(map[string][]queue.BatchMessage)

	for _, batch := range groupBatches {
		for _, writeMsg := range batch.Points {
			// Get buffer from pool
			buf := jsonBufferPool.Get().(*bytes.Buffer)
			buf.Reset()

			// Marshal using encoder (more efficient for streaming)
			encoder := json.NewEncoder(buf)
			if err := encoder.Encode(writeMsg); err != nil {
				jsonBufferPool.Put(buf)
				h.logger.Warn("Failed to serialize message", "error", err)
				continue
			}

			// Make a copy of the data since buffer will be reused
			msgData := make([]byte, buf.Len()-1) // -1 to remove trailing newline
			copy(msgData, buf.Bytes())
			jsonBufferPool.Put(buf)

			// Group by each target node
			for _, nodeID := range writeMsg.Nodes {
				subject := fmt.Sprintf("soltix.write.node.%s", nodeID)
				if nodeMessages[nodeID] == nil {
					nodeMessages[nodeID] = make([]queue.BatchMessage, 0, 100)
				}
				nodeMessages[nodeID] = append(nodeMessages[nodeID], queue.BatchMessage{
					Subject: subject,
					Data:    msgData,
				})
			}
		}
	}

	// Phase 4: Batch publish to each node and track failures
	publishedCount := 0
	var successNodes []string
	var failedNodes []string
	totalExpectedMessages := 0

	for nodeID, messages := range nodeMessages {
		totalExpectedMessages += len(messages)
		count, err := h.queuePublisher.PublishBatch(ctx, messages)
		if err != nil {
			h.logger.Error("Failed to batch publish to node",
				"error", err,
				"node", nodeID,
				"count", len(messages))
			failedNodes = append(failedNodes, nodeID)
		} else {
			successNodes = append(successNodes, nodeID)
		}
		publishedCount += count
	}

	// If all nodes failed, return error
	if len(successNodes) == 0 && len(nodeMessages) > 0 {
		h.logger.Error("Failed to publish batch to any node",
			"database", database,
			"collection", collection,
			"total_points", len(req.Points),
			"failed_nodes", failedNodes)
		return c.Status(fiber.StatusServiceUnavailable).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "QUEUE_UNAVAILABLE",
				Message: "Failed to queue batch write request to any storage node",
			},
		})
	}

	h.logger.Info("Batch write request processed (optimized)",
		"database", database,
		"collection", collection,
		"total_points", len(req.Points),
		"published_count", publishedCount,
		"group_count", len(groupBatches),
		"node_count", len(nodeMessages),
		"success_nodes", len(successNodes),
		"failed_nodes", len(failedNodes),
		"unique_devices", len(seenDevices),
		"route_cache_hits", len(req.Points)-len(groupCache))

	// Update FirstDataTime if we have any timestamps
	if earliestTimestamp != nil {
		go h.updateFirstDataTime(database, collection, *earliestTimestamp)
	}

	// Build response based on success/failure
	response := fiber.Map{
		"total_points":    len(req.Points),
		"published_count": publishedCount,
		"group_count":     len(groupBatches),
		"success_nodes":   successNodes,
	}

	// If some nodes failed, return partial success
	if len(failedNodes) > 0 {
		response["status"] = "partial"
		response["failed_nodes"] = failedNodes
		response["message"] = fmt.Sprintf("Batch write queued to %d/%d nodes. Some replicas may be missing.", len(successNodes), len(nodeMessages))
		return c.Status(fiber.StatusAccepted).JSON(response)
	}

	response["status"] = "accepted"
	response["message"] = "Batch write request accepted and queued for processing"
	return c.Status(fiber.StatusAccepted).JSON(response)
}

// DeletePoints handles deleting data points
func (h *Handler) DeletePoints(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(models.ErrorResponse{
		Error: models.ErrorDetail{
			Code:    "NOT_IMPLEMENTED",
			Message: "DeletePoints not implemented yet",
		},
	})
}
