package handlers

import (
	"regexp"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

var (
	databaseNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	reservedDBNames   = map[string]bool{"system": true, "admin": true, "internal": true}
)

// CreateDatabase creates a new database
func (h *Handler) CreateDatabase(c *fiber.Ctx) error {
	var req models.CreateDatabaseRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Invalid request body: " + err.Error(),
				Path:    c.Path(),
			},
		})
	}

	// Validate database name
	if !databaseNameRegex.MatchString(req.Name) {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_NAME",
				Message: "Database name must contain only alphanumeric characters, underscores, and hyphens",
				Path:    c.Path(),
			},
		})
	}

	if len(req.Name) > 64 {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_NAME",
				Message: "Database name must not exceed 64 characters",
				Path:    c.Path(),
			},
		})
	}

	if reservedDBNames[req.Name] {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_NAME",
				Message: "Database name is reserved",
				Path:    c.Path(),
			},
		})
	}

	// Create database metadata
	db := &metadata.Database{
		Name:        req.Name,
		Description: req.Description,
		Metadata:    req.Metadata,
		CreatedAt:   time.Now(),
	}

	if err := h.metadataManager.CreateDatabase(c.Context(), db); err != nil {
		if err.Error() == "database "+req.Name+" already exists" {
			return c.Status(fiber.StatusConflict).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "DATABASE_EXISTS",
					Message: err.Error(),
					Path:    c.Path(),
				},
			})
		}

		h.logger.Error("Failed to create database", "error", err, "database", req.Name)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to create database",
				Path:    c.Path(),
			},
		})
	}

	h.logger.Info("Database created", "database", req.Name)

	return c.Status(fiber.StatusCreated).JSON(models.DatabaseResponse{
		Name:        db.Name,
		Description: db.Description,
		Metadata:    db.Metadata,
		CreatedAt:   db.CreatedAt.Format(time.RFC3339),
	})
}

// ListDatabases lists all databases
func (h *Handler) ListDatabases(c *fiber.Ctx) error {
	databases, err := h.metadataManager.ListDatabases(c.Context())
	if err != nil {
		h.logger.Error("Failed to list databases", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to list databases",
				Path:    c.Path(),
			},
		})
	}

	response := models.DatabaseListResponse{
		Databases: make([]models.DatabaseResponse, len(databases)),
	}

	// Use goroutines to fetch collections for each database in parallel
	var wg sync.WaitGroup
	for i, db := range databases {
		wg.Add(1)
		go func(idx int, database *metadata.Database) {
			defer wg.Done()

			// Get collections for this database
			collections, err := h.metadataManager.ListCollections(c.Context(), database.Name)
			if err != nil {
				h.logger.Error("Failed to list collections", "error", err, "database", database.Name)
				// Continue with empty collections list instead of failing
				collections = []*metadata.Collection{}
			}

			collectionResponses := make([]models.CollectionResponse, 0, len(collections))
			for _, col := range collections {
				// Format FirstDataTime if exists
				var firstDataTime *string
				if col.FirstDataTime != nil {
					formatted := col.FirstDataTime.Format("2006-01-02")
					firstDataTime = &formatted
				}

				// Convert DeviceIDs map to slice
				deviceIDs := make([]string, 0, len(col.DeviceIDs))
				for id := range col.DeviceIDs {
					deviceIDs = append(deviceIDs, id)
				}

				collectionResponses = append(collectionResponses, models.CollectionResponse{
					Name:          col.Name,
					Description:   col.Description,
					SchemaHints:   col.SchemaHints,
					FirstDataTime: firstDataTime,
					DeviceIDs:     deviceIDs,
					Fields:        col.FieldSchemas,
					CreatedAt:     col.CreatedAt.Format(time.RFC3339),
				})
			}

			response.Databases[idx] = models.DatabaseResponse{
				Name:        database.Name,
				Description: database.Description,
				Metadata:    database.Metadata,
				CreatedAt:   database.CreatedAt.Format(time.RFC3339),
				Collections: collectionResponses,
			}
		}(i, db)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	return c.JSON(response)
}

// GetDatabase gets database information
func (h *Handler) GetDatabase(c *fiber.Ctx) error {
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

	db, err := h.metadataManager.GetDatabase(c.Context(), dbName)
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

		h.logger.Error("Failed to get database", "error", err, "database", dbName)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to get database",
				Path:    c.Path(),
			},
		})
	}

	// Get collections for this database
	collections, err := h.metadataManager.ListCollections(c.Context(), dbName)
	if err != nil {
		h.logger.Error("Failed to list collections", "error", err, "database", dbName)
		// Continue with empty collections list instead of failing
		collections = []*metadata.Collection{}
	}

	collectionResponses := make([]models.CollectionResponse, 0, len(collections))
	for _, col := range collections {
		// Format FirstDataTime if exists
		var firstDataTime *string
		if col.FirstDataTime != nil {
			formatted := col.FirstDataTime.Format("2006-01-02")
			firstDataTime = &formatted
		}

		// Convert DeviceIDs map to slice
		deviceIDs := make([]string, 0, len(col.DeviceIDs))
		for id := range col.DeviceIDs {
			deviceIDs = append(deviceIDs, id)
		}

		collectionResponses = append(collectionResponses, models.CollectionResponse{
			Name:          col.Name,
			Description:   col.Description,
			SchemaHints:   col.SchemaHints,
			FirstDataTime: firstDataTime,
			DeviceIDs:     deviceIDs,
			Fields:        col.FieldSchemas,
			CreatedAt:     col.CreatedAt.Format(time.RFC3339),
		})
	}

	return c.JSON(models.DatabaseResponse{
		Name:        db.Name,
		Description: db.Description,
		Metadata:    db.Metadata,
		CreatedAt:   db.CreatedAt.Format(time.RFC3339),
		Collections: collectionResponses,
	})
}

// DeleteDatabase deletes a database
func (h *Handler) DeleteDatabase(c *fiber.Ctx) error {
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

	// Check if database exists first
	exists, err := h.metadataManager.DatabaseExists(c.Context(), dbName)
	if err != nil {
		h.logger.Error("Failed to check database existence", "error", err, "database", dbName)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to check database existence",
				Path:    c.Path(),
			},
		})
	}

	if !exists {
		return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "DATABASE_NOT_FOUND",
				Message: "database " + dbName + " not found",
				Path:    c.Path(),
			},
		})
	}

	if err := h.metadataManager.DeleteDatabase(c.Context(), dbName); err != nil {
		h.logger.Error("Failed to delete database", "error", err, "database", dbName)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to delete database",
				Path:    c.Path(),
			},
		})
	}

	h.logger.Info("Database deleted", "database", dbName)

	return c.SendStatus(fiber.StatusNoContent)
}
