package models

import (
	"time"

	"github.com/gofiber/fiber/v2"
)

// DownloadFormat represents the export file format
type DownloadFormat string

const (
	DownloadFormatCSV  DownloadFormat = "csv"
	DownloadFormatJSON DownloadFormat = "json"
)

// DownloadStatus represents the status of a download task
type DownloadStatus string

const (
	DownloadStatusPending    DownloadStatus = "pending"
	DownloadStatusProcessing DownloadStatus = "processing"
	DownloadStatusCompleted  DownloadStatus = "completed"
	DownloadStatusFailed     DownloadStatus = "failed"
	DownloadStatusExpired    DownloadStatus = "expired"
)

// DownloadRequest represents a request to export data
type DownloadRequest struct {
	Database     string   `json:"database"`
	Collection   string   `json:"collection"`
	StartTime    string   `json:"start_time"`
	EndTime      string   `json:"end_time"`
	IDs          []string `json:"ids,omitempty"`
	Fields       []string `json:"fields,omitempty"`
	Interval     string   `json:"interval,omitempty"`     // 1m, 5m, 1h, 1d, 1mo, 1y
	Aggregation  string   `json:"aggregation,omitempty"`  // max, min, avg, sum, count
	Downsampling string   `json:"downsampling,omitempty"` // none, auto, lttb, minmax, avg, m4
	Format       string   `json:"format"`                 // csv, json
	Filename     string   `json:"filename,omitempty"`     // Optional custom filename

	// Parsed fields
	StartTimeParsed time.Time `json:"-"`
	EndTimeParsed   time.Time `json:"-"`
}

// NewDownloadRequest creates a new DownloadRequest with defaults
func NewDownloadRequest(
	database, collection, startTime, endTime string,
	ids, fields []string,
	interval, aggregation, downsampling, format, filename string,
) *DownloadRequest {
	// Apply default format
	if format == "" {
		format = "csv"
	}

	// Apply default aggregation
	if aggregation == "" {
		aggregation = "sum"
	}

	// Apply default downsampling
	if downsampling == "" {
		downsampling = "none"
	}

	return &DownloadRequest{
		Database:     database,
		Collection:   collection,
		StartTime:    startTime,
		EndTime:      endTime,
		IDs:          ids,
		Fields:       fields,
		Interval:     interval,
		Aggregation:  aggregation,
		Downsampling: downsampling,
		Format:       format,
		Filename:     filename,
	}
}

// Validate validates the download request
// Download API has NO time range limits (async processing)
func (d *DownloadRequest) Validate() error {
	// Validate required parameters
	if d.Database == "" || d.Collection == "" {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "database and collection are required",
		}
	}

	if len(d.IDs) == 0 {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "'ids' (device IDs) is required",
		}
	}

	if d.StartTime == "" || d.EndTime == "" {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "start_time and end_time are required",
		}
	}

	// Parse start_time
	startTime, err := time.Parse(time.RFC3339, d.StartTime)
	if err != nil {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "start_time must be in RFC3339 format (e.g., 2006-01-02T15:04:05Z)",
		}
	}

	// Parse end_time
	endTime, err := time.Parse(time.RFC3339, d.EndTime)
	if err != nil {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "end_time must be in RFC3339 format (e.g., 2006-01-02T15:04:05Z)",
		}
	}

	// Validate time range
	if endTime.Before(startTime) {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "end_time must be after start_time",
		}
	}

	d.StartTimeParsed = startTime
	d.EndTimeParsed = endTime

	// Validate format
	validFormats := map[string]bool{
		"csv": true, "json": true,
	}
	if !validFormats[d.Format] {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "format must be one of: csv, json",
		}
	}

	// Validate interval if provided
	if d.Interval != "" {
		validIntervals := map[string]bool{
			"1m": true, "5m": true, "1h": true, "1d": true, "1mo": true, "1y": true,
		}
		if !validIntervals[d.Interval] {
			return &fiber.Error{
				Code:    fiber.StatusBadRequest,
				Message: "interval must be one of: 1m, 5m, 1h, 1d, 1mo, 1y",
			}
		}
	}

	// Validate aggregation
	validAggregations := map[string]bool{
		"sum": true, "avg": true, "min": true, "max": true, "count": true,
	}
	if !validAggregations[d.Aggregation] {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "aggregation must be one of: sum, avg, min, max, count",
		}
	}

	// Validate downsampling
	validDownsamplingModes := map[string]bool{
		"none": true, "auto": true, "lttb": true, "minmax": true, "avg": true, "m4": true,
	}
	if !validDownsamplingModes[d.Downsampling] {
		return &fiber.Error{
			Code:    fiber.StatusBadRequest,
			Message: "downsampling must be one of: none, auto, lttb, minmax, avg, m4",
		}
	}

	return nil
}

// ToQueryRequest converts DownloadRequest to QueryRequest for data fetching
func (d *DownloadRequest) ToQueryRequest() *QueryRequest {
	return &QueryRequest{
		Database:        d.Database,
		Collection:      d.Collection,
		StartTime:       d.StartTime,
		EndTime:         d.EndTime,
		IDs:             d.IDs,
		Fields:          d.Fields,
		Limit:           0, // No limit for downloads
		Interval:        d.Interval,
		Aggregation:     d.Aggregation,
		Downsampling:    d.Downsampling,
		StartTimeParsed: d.StartTimeParsed,
		EndTimeParsed:   d.EndTimeParsed,
	}
}

// DownloadTask represents a download task with status
type DownloadTask struct {
	RequestID   string          `json:"request_id"`
	Status      DownloadStatus  `json:"status"`
	Request     DownloadRequest `json:"request"`
	Progress    int             `json:"progress"`     // 0-100
	TotalRows   int64           `json:"total_rows"`   // Total rows processed
	FileSize    int64           `json:"file_size"`    // File size in bytes
	FilePath    string          `json:"-"`            // Internal file path (not exposed)
	Filename    string          `json:"filename"`     // Download filename
	ContentType string          `json:"content_type"` // MIME type
	Error       string          `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	ExpiresAt   time.Time       `json:"expires_at"` // File expiration time
}

// NewDownloadTask creates a new download task
func NewDownloadTask(requestID string, request DownloadRequest, expirationDuration time.Duration) *DownloadTask {
	now := time.Now()

	// Generate filename if not provided
	filename := request.Filename
	if filename == "" {
		filename = generateDownloadFilename(request)
	}

	contentType := "text/csv"
	if request.Format == "json" {
		contentType = "application/json"
	}

	return &DownloadTask{
		RequestID:   requestID,
		Status:      DownloadStatusPending,
		Request:     request,
		Progress:    0,
		Filename:    filename,
		ContentType: contentType,
		CreatedAt:   now,
		ExpiresAt:   now.Add(expirationDuration),
	}
}

// generateDownloadFilename generates a default filename for download
func generateDownloadFilename(request DownloadRequest) string {
	// Format: {database}_{collection}_{startDate}_{endDate}.{format}
	startDate := ""
	endDate := ""

	if !request.StartTimeParsed.IsZero() {
		startDate = request.StartTimeParsed.Format("20060102")
	}
	if !request.EndTimeParsed.IsZero() {
		endDate = request.EndTimeParsed.Format("20060102")
	}

	return request.Database + "_" + request.Collection + "_" + startDate + "_" + endDate + "." + request.Format
}

// IsExpired checks if the download task has expired
func (t *DownloadTask) IsExpired() bool {
	return time.Now().After(t.ExpiresAt)
}

// CanDownload checks if the file can be downloaded
func (t *DownloadTask) CanDownload() bool {
	return t.Status == DownloadStatusCompleted && !t.IsExpired()
}

// DownloadCreateResponse is the response when creating a download request
type DownloadCreateResponse struct {
	RequestID string    `json:"request_id"`
	Status    string    `json:"status"`
	Message   string    `json:"message"`
	ExpiresAt time.Time `json:"expires_at"`
}

// DownloadStatusResponse is the response for download status check
type DownloadStatusResponse struct {
	RequestID   string     `json:"request_id"`
	Status      string     `json:"status"`
	Progress    int        `json:"progress"`
	TotalRows   int64      `json:"total_rows,omitempty"`
	FileSize    int64      `json:"file_size,omitempty"`
	Filename    string     `json:"filename,omitempty"`
	Error       string     `json:"error,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	ExpiresAt   time.Time  `json:"expires_at"`
	DownloadURL string     `json:"download_url,omitempty"`
}

// ToStatusResponse converts DownloadTask to DownloadStatusResponse
func (t *DownloadTask) ToStatusResponse(baseURL string) *DownloadStatusResponse {
	resp := &DownloadStatusResponse{
		RequestID:   t.RequestID,
		Status:      string(t.Status),
		Progress:    t.Progress,
		TotalRows:   t.TotalRows,
		FileSize:    t.FileSize,
		Filename:    t.Filename,
		Error:       t.Error,
		CreatedAt:   t.CreatedAt,
		StartedAt:   t.StartedAt,
		CompletedAt: t.CompletedAt,
		ExpiresAt:   t.ExpiresAt,
	}

	if t.Status == DownloadStatusCompleted && !t.IsExpired() {
		resp.DownloadURL = baseURL + "/v1/download/file/" + t.RequestID
	}

	return resp
}
