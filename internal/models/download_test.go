package models

import (
	"testing"
	"time"
)

func TestDownloadRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		request *DownloadRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request - csv",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				[]string{"device1"}, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: false,
		},
		{
			name: "valid request - json",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				[]string{"device1"}, nil,
				"1h", "avg", "lttb",
				"json", "",
			),
			wantErr: false,
		},
		{
			name: "valid request - no time range limit",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2020-01-01T00:00:00Z", "2026-12-31T00:00:00Z", // 6+ years
				[]string{"device1"}, nil,
				"1mo", "", "",
				"csv", "",
			),
			wantErr: false,
		},
		{
			name: "missing database",
			request: NewDownloadRequest(
				"", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				nil, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "database and collection are required",
		},
		{
			name: "missing collection",
			request: NewDownloadRequest(
				"testdb", "",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				nil, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "database and collection are required",
		},
		{
			name: "missing ids",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				nil, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "'ids' (device IDs) is required",
		},
		{
			name: "missing start_time",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"", "2026-01-31T00:00:00Z",
				[]string{"device1"}, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "start_time and end_time are required",
		},
		{
			name: "missing end_time",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "",
				[]string{"device1"}, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "start_time and end_time are required",
		},
		{
			name: "invalid start_time format",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01", "2026-01-31T00:00:00Z",
				[]string{"device1"}, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "start_time must be in RFC3339 format",
		},
		{
			name: "invalid end_time format",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31",
				[]string{"device1"}, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "end_time must be in RFC3339 format",
		},
		{
			name: "end_time before start_time",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-31T00:00:00Z", "2026-01-01T00:00:00Z",
				[]string{"device1"}, nil,
				"", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "end_time must be after start_time",
		},
		{
			name: "invalid format",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				[]string{"device1"}, nil,
				"", "", "",
				"xml", "",
			),
			wantErr: true,
			errMsg:  "format must be one of: csv, json",
		},
		{
			name: "invalid interval",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				[]string{"device1"}, nil,
				"2h", "", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "interval must be one of",
		},
		{
			name: "invalid aggregation",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				[]string{"device1"}, nil,
				"", "median", "",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "aggregation must be one of",
		},
		{
			name: "invalid downsampling",
			request: NewDownloadRequest(
				"testdb", "testcol",
				"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
				[]string{"device1"}, nil,
				"", "", "invalid",
				"csv", "",
			),
			wantErr: true,
			errMsg:  "downsampling must be one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error message containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestDownloadRequest_ToQueryRequest(t *testing.T) {
	req := NewDownloadRequest(
		"testdb", "testcol",
		"2026-01-01T00:00:00Z", "2026-01-31T00:00:00Z",
		[]string{"device1", "device2"},
		[]string{"temperature", "humidity"},
		"1h", "avg", "lttb",
		"csv", "",
	)
	_ = req.Validate() // Parse times

	queryReq := req.ToQueryRequest()

	if queryReq.Database != "testdb" {
		t.Errorf("expected database testdb, got %s", queryReq.Database)
	}
	if queryReq.Collection != "testcol" {
		t.Errorf("expected collection testcol, got %s", queryReq.Collection)
	}
	if len(queryReq.IDs) != 2 {
		t.Errorf("expected 2 IDs, got %d", len(queryReq.IDs))
	}
	if queryReq.Interval != "1h" {
		t.Errorf("expected interval 1h, got %s", queryReq.Interval)
	}
	if queryReq.Aggregation != "avg" {
		t.Errorf("expected aggregation avg, got %s", queryReq.Aggregation)
	}
	if queryReq.Limit != 0 {
		t.Errorf("expected limit 0, got %d", queryReq.Limit)
	}
}

func TestDownloadTask(t *testing.T) {
	req := DownloadRequest{
		Database:   "testdb",
		Collection: "testcol",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-31T00:00:00Z",
		Format:     "csv",
	}

	task := NewDownloadTask("test-123", req, 1*time.Hour)

	if task.RequestID != "test-123" {
		t.Errorf("expected request_id test-123, got %s", task.RequestID)
	}
	if task.Status != DownloadStatusPending {
		t.Errorf("expected status pending, got %s", task.Status)
	}
	if task.ContentType != "text/csv" {
		t.Errorf("expected content type text/csv, got %s", task.ContentType)
	}
	if task.IsExpired() {
		t.Error("task should not be expired immediately after creation")
	}
	if task.CanDownload() {
		t.Error("pending task should not be downloadable")
	}

	// Test expired task
	task.ExpiresAt = time.Now().Add(-1 * time.Hour)
	if !task.IsExpired() {
		t.Error("task should be expired")
	}

	// Test completed task
	task.Status = DownloadStatusCompleted
	task.ExpiresAt = time.Now().Add(1 * time.Hour)
	if !task.CanDownload() {
		t.Error("completed non-expired task should be downloadable")
	}
}

func TestDownloadTask_ToStatusResponse(t *testing.T) {
	req := DownloadRequest{
		Database:   "testdb",
		Collection: "testcol",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-31T00:00:00Z",
		Format:     "json",
	}

	task := NewDownloadTask("test-456", req, 1*time.Hour)
	task.Status = DownloadStatusCompleted
	task.Progress = 100
	task.TotalRows = 1000
	task.FileSize = 51200

	resp := task.ToStatusResponse("http://localhost:8080")

	if resp.RequestID != "test-456" {
		t.Errorf("expected request_id test-456, got %s", resp.RequestID)
	}
	if resp.Status != "completed" {
		t.Errorf("expected status completed, got %s", resp.Status)
	}
	if resp.Progress != 100 {
		t.Errorf("expected progress 100, got %d", resp.Progress)
	}
	if resp.TotalRows != 1000 {
		t.Errorf("expected total_rows 1000, got %d", resp.TotalRows)
	}
	if resp.DownloadURL != "http://localhost:8080/v1/download/file/test-456" {
		t.Errorf("unexpected download_url: %s", resp.DownloadURL)
	}

	// Test expired task - should not have download URL
	task.ExpiresAt = time.Now().Add(-1 * time.Hour)
	resp = task.ToStatusResponse("http://localhost:8080")
	if resp.DownloadURL != "" {
		t.Errorf("expired task should not have download_url, got %s", resp.DownloadURL)
	}
}

func TestGenerateDownloadFilename(t *testing.T) {
	req := DownloadRequest{
		Database:   "mydb",
		Collection: "sensors",
		Format:     "csv",
	}

	// Without parsed times
	filename := generateDownloadFilename(req)
	if filename != "mydb_sensors__.csv" {
		t.Errorf("unexpected filename without times: %s", filename)
	}

	// With parsed times
	req.StartTimeParsed = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	req.EndTimeParsed = time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	filename = generateDownloadFilename(req)
	if filename != "mydb_sensors_20260101_20260131.csv" {
		t.Errorf("unexpected filename: %s", filename)
	}

	// JSON format
	req.Format = "json"
	filename = generateDownloadFilename(req)
	if filename != "mydb_sensors_20260101_20260131.json" {
		t.Errorf("unexpected JSON filename: %s", filename)
	}
}
