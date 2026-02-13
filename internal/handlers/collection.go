package handlers

import (
	"regexp"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

var (
	collectionNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	reservedCollNames   = map[string]bool{"metadata": true, "config": true, "system": true}
)

// CreateCollection creates a new collection in a database
func (h *Handler) CreateCollection(c *fiber.Ctx) error {
	dbName := c.Params("database")
	if dbName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Database name is required",
				Path:    c.Path(),
			},
		})
	}

	var req models.CreateCollectionRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Invalid request body: " + err.Error(),
				Path:    c.Path(),
			},
		})
	}

	// Validate collection name
	if !collectionNameRegex.MatchString(req.Name) {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_NAME",
				Message: "Collection name must contain only alphanumeric characters, underscores, and hyphens",
				Path:    c.Path(),
			},
		})
	}

	if len(req.Name) > 64 {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_NAME",
				Message: "Collection name must not exceed 64 characters",
				Path:    c.Path(),
			},
		})
	}

	if reservedCollNames[req.Name] {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_NAME",
				Message: "Collection name is reserved",
				Path:    c.Path(),
			},
		})
	}

	// Validate fields - time and id are required
	hasTime := false
	hasID := false
	fieldNames := make(map[string]bool)

	for _, field := range req.Fields {
		// Check for duplicates
		if fieldNames[field.Name] {
			return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "INVALID_FIELDS",
					Message: "Duplicate field name: " + field.Name,
					Path:    c.Path(),
				},
			})
		}
		fieldNames[field.Name] = true

		// Check for required fields
		if field.Name == "time" {
			hasTime = true
			if field.Type != "timestamp" {
				return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "INVALID_FIELDS",
						Message: "Field 'time' must be of type 'timestamp'",
						Path:    c.Path(),
					},
				})
			}
			if !field.Required {
				return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "INVALID_FIELDS",
						Message: "Field 'time' must be required",
						Path:    c.Path(),
					},
				})
			}
		}

		if field.Name == "id" {
			hasID = true
			if field.Type != "string" {
				return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "INVALID_FIELDS",
						Message: "Field 'id' must be of type 'string'",
						Path:    c.Path(),
					},
				})
			}
			if !field.Required {
				return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "INVALID_FIELDS",
						Message: "Field 'id' must be required",
						Path:    c.Path(),
					},
				})
			}
		}
	}

	if !hasTime {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_FIELDS",
				Message: "Field 'time' (timestamp, required) is mandatory",
				Path:    c.Path(),
			},
		})
	}

	if !hasID {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_FIELDS",
				Message: "Field 'id' (string, required) is mandatory",
				Path:    c.Path(),
			},
		})
	}

	// Convert fields to metadata format
	metadataFields := make([]metadata.FieldInfo, len(req.Fields))
	for i, field := range req.Fields {
		metadataFields[i] = metadata.FieldInfo{
			Name:     field.Name,
			Type:     field.Type,
			Required: field.Required,
		}
	}

	// Create collection metadata
	coll := &metadata.Collection{
		Name:        req.Name,
		Description: req.Description,
		SchemaHints: req.SchemaHints,
		Fields:      metadataFields,
		CreatedAt:   time.Now(),
	}

	if err := h.metadataManager.CreateCollection(c.Context(), dbName, coll); err != nil {
		if err.Error() == "database "+dbName+" not found" {
			return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "DATABASE_NOT_FOUND",
					Message: err.Error(),
					Path:    c.Path(),
				},
			})
		}

		if err.Error() == "collection "+req.Name+" already exists in database "+dbName {
			return c.Status(fiber.StatusConflict).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "COLLECTION_EXISTS",
					Message: err.Error(),
					Path:    c.Path(),
				},
			})
		}

		h.logger.Error("Failed to create collection", "error", err, "database", dbName, "collection", req.Name)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to create collection",
				Path:    c.Path(),
			},
		})
	}

	h.logger.Info("Collection created", "database", dbName, "collection", req.Name)

	// Format FirstDataTime if exists
	var firstDataTime *string
	if coll.FirstDataTime != nil {
		formatted := coll.FirstDataTime.Format("2006-01-02")
		firstDataTime = &formatted
	}

	return c.Status(fiber.StatusCreated).JSON(models.CollectionResponse{
		Name:          coll.Name,
		Description:   coll.Description,
		SchemaHints:   coll.SchemaHints,
		FirstDataTime: firstDataTime,
		Fields:        coll.FieldSchemas,
		CreatedAt:     coll.CreatedAt.Format(time.RFC3339),
	})
}

// ListCollections lists all collections in a database
func (h *Handler) ListCollections(c *fiber.Ctx) error {
	dbName := c.Params("database")
	if dbName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Database name is required",
				Path:    c.Path(),
			},
		})
	}

	collections, err := h.metadataManager.ListCollections(c.Context(), dbName)
	if err != nil {
		if err.Error() == "database "+dbName+" not found" {
			return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "DATABASE_NOT_FOUND",
					Message: err.Error(),
					Path:    c.Path(),
				},
			})
		}

		h.logger.Error("Failed to list collections", "error", err, "database", dbName)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to list collections",
				Path:    c.Path(),
			},
		})
	}

	response := models.CollectionListResponse{
		Collections: make([]models.CollectionResponse, 0, len(collections)),
	}

	for _, coll := range collections {
		// Format FirstDataTime if exists
		var firstDataTime *string
		if coll.FirstDataTime != nil {
			formatted := coll.FirstDataTime.Format("2006-01-02")
			firstDataTime = &formatted
		}

		// Convert DeviceIDs map to slice
		deviceIDs := make([]string, 0, len(coll.DeviceIDs))
		for id := range coll.DeviceIDs {
			deviceIDs = append(deviceIDs, id)
		}

		response.Collections = append(response.Collections, models.CollectionResponse{
			Name:          coll.Name,
			Description:   coll.Description,
			SchemaHints:   coll.SchemaHints,
			FirstDataTime: firstDataTime,
			DeviceIDs:     deviceIDs,
			Fields:        coll.FieldSchemas,
			CreatedAt:     coll.CreatedAt.Format(time.RFC3339),
		})
	}

	return c.JSON(response)
}

// GetCollection gets collection information
func (h *Handler) GetCollection(c *fiber.Ctx) error {
	dbName := c.Params("database")
	collName := c.Params("collection")

	if dbName == "" || collName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Database name and collection name are required",
				Path:    c.Path(),
			},
		})
	}

	coll, err := h.metadataManager.GetCollection(c.Context(), dbName, collName)
	if err != nil {
		if err.Error() == "database "+dbName+" not found" {
			return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "DATABASE_NOT_FOUND",
					Message: err.Error(),
					Path:    c.Path(),
				},
			})
		}

		if err.Error() == "collection "+collName+" not found in database "+dbName {
			return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "COLLECTION_NOT_FOUND",
					Message: err.Error(),
					Path:    c.Path(),
				},
			})
		}

		h.logger.Error("Failed to get collection", "error", err, "database", dbName, "collection", collName)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to get collection",
				Path:    c.Path(),
			},
		})
	}

	// Format FirstDataTime if exists
	var firstDataTime *string
	if coll.FirstDataTime != nil {
		formatted := coll.FirstDataTime.Format("2006-01-02")
		firstDataTime = &formatted
	}

	// Convert DeviceIDs map to slice
	deviceIDs := make([]string, 0, len(coll.DeviceIDs))
	for id := range coll.DeviceIDs {
		deviceIDs = append(deviceIDs, id)
	}

	return c.JSON(models.CollectionResponse{
		Name:          coll.Name,
		Description:   coll.Description,
		SchemaHints:   coll.SchemaHints,
		FirstDataTime: firstDataTime,
		DeviceIDs:     deviceIDs,
		Fields:        coll.FieldSchemas,
		CreatedAt:     coll.CreatedAt.Format(time.RFC3339),
	})
}

// DeleteCollection deletes a collection
func (h *Handler) DeleteCollection(c *fiber.Ctx) error {
	dbName := c.Params("database")
	collName := c.Params("collection")

	if dbName == "" || collName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Database name and collection name are required",
				Path:    c.Path(),
			},
		})
	}

	// Check if collection exists first
	exists, err := h.metadataManager.CollectionExists(c.Context(), dbName, collName)
	if err != nil {
		h.logger.Error("Failed to check collection existence", "error", err, "database", dbName, "collection", collName)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to check collection existence",
				Path:    c.Path(),
			},
		})
	}

	if !exists {
		return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "COLLECTION_NOT_FOUND",
				Message: "collection " + collName + " not found in database " + dbName,
				Path:    c.Path(),
			},
		})
	}

	if err := h.metadataManager.DeleteCollection(c.Context(), dbName, collName); err != nil {
		h.logger.Error("Failed to delete collection", "error", err, "database", dbName, "collection", collName)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to delete collection",
				Path:    c.Path(),
			},
		})
	}

	h.logger.Info("Collection deleted", "database", dbName, "collection", collName)

	return c.SendStatus(fiber.StatusNoContent)
}
