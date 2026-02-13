package handlers

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/soltixdb/soltix/internal/models"
	"github.com/soltixdb/soltix/internal/services"
)

// CreateDownload handles POST /v1/databases/:database/collections/:collection/download
// Creates an async download task and returns request_id
func (h *Handler) CreateDownload(c *fiber.Ctx) error {
	database := c.Params("database")
	collection := c.Params("collection")

	// Parse request body
	var body struct {
		StartTime    string   `json:"start_time"`
		EndTime      string   `json:"end_time"`
		IDs          []string `json:"ids"`
		Fields       []string `json:"fields"`
		Interval     string   `json:"interval"`
		Aggregation  string   `json:"aggregation"`
		Downsampling string   `json:"downsampling"`
		Format       string   `json:"format"`
		Filename     string   `json:"filename"`
	}

	if err := c.BodyParser(&body); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "invalid request body: " + err.Error(),
			},
		})
	}

	// Also support query parameters for GET-style requests
	if body.StartTime == "" {
		body.StartTime = c.Query("start_time")
	}
	if body.EndTime == "" {
		body.EndTime = c.Query("end_time")
	}
	if body.Format == "" {
		body.Format = c.Query("format", "csv")
	}
	if body.Interval == "" {
		body.Interval = c.Query("interval")
	}
	if body.Aggregation == "" {
		body.Aggregation = c.Query("aggregation")
	}
	if body.Downsampling == "" {
		body.Downsampling = c.Query("downsampling")
	}
	if len(body.IDs) == 0 && c.Query("ids") != "" {
		body.IDs = strings.Split(c.Query("ids"), ",")
	}
	if len(body.Fields) == 0 && c.Query("fields") != "" {
		body.Fields = strings.Split(c.Query("fields"), ",")
	}
	if body.Filename == "" {
		body.Filename = c.Query("filename")
	}

	// Create download request
	request := models.NewDownloadRequest(
		database, collection,
		body.StartTime, body.EndTime,
		body.IDs, body.Fields,
		body.Interval, body.Aggregation, body.Downsampling,
		body.Format, body.Filename,
	)

	// Validate request
	if err := request.Validate(); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: err.Error(),
			},
		})
	}

	// Create download task
	task, err := h.downloadService.CreateDownload(c.Context(), request)
	if err != nil {
		if svcErr, ok := err.(*services.ServiceError); ok {
			return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
				Error: models.ErrorDetail{
					Code:    "COLLECTION_NOT_FOUND",
					Message: svcErr.Message,
				},
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "failed to create download task: " + err.Error(),
			},
		})
	}

	// Return response
	return c.Status(fiber.StatusAccepted).JSON(&models.DownloadCreateResponse{
		RequestID: task.RequestID,
		Status:    string(task.Status),
		Message:   "Download task created. Use the status endpoint to check progress.",
		ExpiresAt: task.ExpiresAt,
	})
}

// GetDownloadStatus handles GET /v1/download/status/:request_id
// Returns the status of a download task
func (h *Handler) GetDownloadStatus(c *fiber.Ctx) error {
	requestID := c.Params("request_id")

	if requestID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "request_id is required",
			},
		})
	}

	// Get task status
	task, err := h.downloadService.GetTaskStatus(requestID)
	if err != nil {
		if svcErr, ok := err.(*services.ServiceError); ok {
			if svcErr.Code == "TASK_NOT_FOUND" {
				return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "TASK_NOT_FOUND",
						Message: svcErr.Message,
					},
				})
			}
		}
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "failed to get download status: " + err.Error(),
			},
		})
	}

	// Get base URL for download link
	baseURL := getBaseURL(c)

	return c.JSON(task.ToStatusResponse(baseURL))
}

// DownloadFile handles GET /v1/download/file/:request_id
// Downloads the generated file
func (h *Handler) DownloadFile(c *fiber.Ctx) error {
	requestID := c.Params("request_id")

	if requestID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "request_id is required",
			},
		})
	}

	// Get file path
	filePath, filename, contentType, err := h.downloadService.GetFilePath(requestID)
	if err != nil {
		if svcErr, ok := err.(*services.ServiceError); ok {
			switch svcErr.Code {
			case "TASK_NOT_FOUND":
				return c.Status(fiber.StatusNotFound).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "TASK_NOT_FOUND",
						Message: svcErr.Message,
					},
				})
			case "DOWNLOAD_EXPIRED":
				return c.Status(fiber.StatusGone).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "DOWNLOAD_EXPIRED",
						Message: svcErr.Message,
					},
				})
			case "DOWNLOAD_NOT_READY":
				return c.Status(fiber.StatusConflict).JSON(models.ErrorResponse{
					Error: models.ErrorDetail{
						Code:    "DOWNLOAD_NOT_READY",
						Message: svcErr.Message,
					},
				})
			}
		}
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "failed to get download file: " + err.Error(),
			},
		})
	}

	// Set response headers
	c.Set("Content-Type", contentType)
	c.Set("Content-Disposition", "attachment; filename=\""+filename+"\"")

	// Send file
	return c.SendFile(filePath, false)
}

// getBaseURL extracts the base URL from the request
func getBaseURL(c *fiber.Ctx) string {
	scheme := "http"
	if c.Protocol() == "https" || c.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	return scheme + "://" + c.Hostname()
}
