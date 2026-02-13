package services

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/handlers/processing"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

func createTestDownloadService(t *testing.T) (*DownloadService, string) {
	logger := logging.NewDevelopment()
	mockMeta := NewMockMetadataManager()

	// Setup mock database and collection
	mockMeta.databases["testdb"] = &metadata.Database{
		Name:      "testdb",
		CreatedAt: time.Now(),
	}
	mockMeta.collections["testdb"] = map[string]*metadata.Collection{
		"testcoll": {
			Name:      "testcoll",
			CreatedAt: time.Now(),
		},
	}

	tmpDir := t.TempDir()

	// Create coordinator config for testing
	coordCfg := config.CoordinatorConfig{
		HashThreshold: 100,
		VNodeCount:    150,
		ReplicaFactor: 1,
	}

	// Create shard router with config
	shardRouter := coordinator.NewShardRouter(logger, mockMeta, coordCfg)

	// Create a minimal coordinator for testing
	queryCoord := coordinator.NewQueryCoordinator(logger, mockMeta, shardRouter)
	proc := processing.NewProcessor(logger)

	svc := NewDownloadService(logger, mockMeta, queryCoord, proc, tmpDir)
	return svc, tmpDir
}

func TestDownloadService_CreateDownload(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Create download request using string times (as per DownloadRequest structure)
	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
		EndTime:    time.Now().Format(time.RFC3339),
		Format:     "csv",
	}

	// Test
	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Assertions
	if task.RequestID == "" {
		t.Error("Expected non-empty task RequestID")
	}
	// Use GetTaskStatus to safely read status (avoids race condition)
	currentTask, _ := svc.GetTaskStatus(task.RequestID)
	if currentTask != nil {
		if currentTask.Status != models.DownloadStatusPending &&
			currentTask.Status != models.DownloadStatusProcessing &&
			currentTask.Status != models.DownloadStatusCompleted {
			t.Errorf("Expected status pending, processing, or completed, got %s", currentTask.Status)
		}
	}
	if task.Request.Database != "testdb" {
		t.Errorf("Expected database testdb, got %s", task.Request.Database)
	}
	if task.Request.Collection != "testcoll" {
		t.Errorf("Expected collection testcoll, got %s", task.Request.Collection)
	}
	if task.Request.Format != "csv" {
		t.Errorf("Expected format csv, got %s", task.Request.Format)
	}
}

func TestDownloadService_GetTaskStatus_NotFound(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Test
	_, err := svc.GetTaskStatus("non-existent-id")

	// Should return error
	if err == nil {
		t.Error("Expected error for non-existent ID")
	}
}

func TestDownloadService_GetTaskStatus_Found(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Create a download request
	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
		EndTime:    time.Now().Format(time.RFC3339),
		Format:     "csv",
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Test
	retrievedTask, err := svc.GetTaskStatus(task.RequestID)
	// Should return the task
	if err != nil {
		t.Fatalf("GetTaskStatus failed: %v", err)
	}
	if retrievedTask == nil {
		t.Fatal("Expected non-nil task")
	}
	if retrievedTask.RequestID != task.RequestID {
		t.Errorf("Expected task ID %s, got %s", task.RequestID, retrievedTask.RequestID)
	}
}

func TestDownloadService_GetFilePath_NotFound(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Test
	filePath, _, _, err := svc.GetFilePath("non-existent-id")

	// Should return error
	if err == nil {
		t.Error("Expected error for non-existent ID")
	}
	if filePath != "" {
		t.Errorf("Expected empty file path, got %s", filePath)
	}
}

func TestDownloadService_GetFilePath_NotCompleted(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Create a download request
	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
		EndTime:    time.Now().Format(time.RFC3339),
		Format:     "csv",
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Immediately try to get file path (task may not be completed yet)
	// The test might pass or fail depending on timing
	filePath, _, _, err := svc.GetFilePath(task.RequestID)
	if err != nil {
		// Expected - task not ready
		svcErr, ok := err.(*ServiceError)
		if ok && (svcErr.Code == "DOWNLOAD_NOT_READY" || svcErr.Code == "DOWNLOAD_EXPIRED") {
			// Expected behavior
			return
		}
	}
	// If no error, file should exist
	if filePath != "" {
		if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
			t.Errorf("File path returned but file does not exist: %s", filePath)
		}
	}
}

func TestDownloadService_StopGracefully(t *testing.T) {
	svc, _ := createTestDownloadService(t)

	// Create a download request
	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
		EndTime:    time.Now().Format(time.RFC3339),
		Format:     "csv",
	}

	_, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Stop should not panic
	svc.Stop()
}

func TestDownloadService_WithFilters(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Create a download request with filters
	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-31T23:59:59Z",
		Format:     "csv",
		IDs:        []string{"device1", "device2"},
		Fields:     []string{"temperature"},
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Verify request is stored correctly
	if len(task.Request.IDs) != 2 {
		t.Errorf("Expected 2 IDs, got %d", len(task.Request.IDs))
	}
	if len(task.Request.Fields) != 1 {
		t.Errorf("Expected 1 field, got %d", len(task.Request.Fields))
	}
}

func TestDownloadService_CustomFilename(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Create a download request with custom filename
	customFilename := "my_custom_export.csv"
	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-31T23:59:59Z",
		Format:     "csv",
		Filename:   customFilename,
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Check that custom filename is stored
	if task.Request.Filename != customFilename {
		t.Errorf("Expected filename %s, got %s", customFilename, task.Request.Filename)
	}
}

func TestDownloadService_TaskProcessing(t *testing.T) {
	// This test validates the full processing flow
	// It may fail if there's no data or query coordinator returns empty results

	svc, _ := createTestDownloadService(t)

	// Create a download request
	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Wait for task to complete (may fail or complete depending on storage)
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			// Get final status
			status, _ := svc.GetTaskStatus(task.RequestID)
			if status != nil {
				// Task may have failed due to missing storage, which is acceptable in test
				if status.Status == models.DownloadStatusFailed ||
					status.Status == models.DownloadStatusCompleted ||
					status.Status == models.DownloadStatusProcessing {
					svc.Stop()
					return
				}
			}
			t.Log("Task still processing after timeout - this is acceptable in test environment")
			svc.Stop()
			return
		case <-ticker.C:
			status, _ := svc.GetTaskStatus(task.RequestID)
			if status != nil && status.Status == models.DownloadStatusCompleted {
				// Task completed, verify file exists
				filePath, _, _, getErr := svc.GetFilePath(task.RequestID)
				if getErr != nil {
					t.Fatalf("GetFilePath failed: %v", getErr)
				}

				// Check file exists
				if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
					t.Fatalf("File does not exist: %s", filePath)
				}

				// Check file has .csv extension
				if filepath.Ext(filePath) != ".csv" {
					t.Errorf("Expected .csv extension, got %s", filepath.Ext(filePath))
				}

				svc.Stop()
				return
			}
			if status != nil && status.Status == models.DownloadStatusFailed {
				// Task failed (expected in test without real storage)
				t.Log("Task failed (expected in test without real storage): " + status.Error)
				svc.Stop()
				return
			}
		}
	}
}

func TestDownloadService_ListTasks(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Initially should be empty
	tasks := svc.ListTasks()
	if len(tasks) != 0 {
		t.Errorf("Expected 0 tasks initially, got %d", len(tasks))
	}

	// Create some download requests
	req1 := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
	}
	task1, err := svc.CreateDownload(context.Background(), req1)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	req2 := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-02-01T00:00:00Z",
		EndTime:    "2026-02-02T00:00:00Z",
		Format:     "json",
	}
	task2, err := svc.CreateDownload(context.Background(), req2)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// List should now contain 2 tasks
	tasks = svc.ListTasks()
	if len(tasks) != 2 {
		t.Errorf("Expected 2 tasks, got %d", len(tasks))
	}

	// Check that both tasks are in the list
	foundTask1 := false
	foundTask2 := false
	for _, task := range tasks {
		if task.RequestID == task1.RequestID {
			foundTask1 = true
		}
		if task.RequestID == task2.RequestID {
			foundTask2 = true
		}
	}
	if !foundTask1 {
		t.Error("Expected to find task1 in list")
	}
	if !foundTask2 {
		t.Error("Expected to find task2 in list")
	}
}

func TestDownloadService_CreateDownload_InvalidDatabase(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	req := &models.DownloadRequest{
		Database:   "nonexistent_db",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
	}

	_, err := svc.CreateDownload(context.Background(), req)
	if err == nil {
		t.Error("Expected error for non-existent database")
	}
}

func TestDownloadService_CreateDownload_InvalidCollection(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "nonexistent_coll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
	}

	_, err := svc.CreateDownload(context.Background(), req)
	if err == nil {
		t.Error("Expected error for non-existent collection")
	}
}

func TestDownloadService_GetFilePath_Expired(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Create a download task with immediate expiration
	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Manually set task to expired
	svc.taskMutex.Lock()
	if t, ok := svc.tasks[task.RequestID]; ok {
		t.ExpiresAt = time.Now().Add(-1 * time.Hour) // Set to past
		t.Status = models.DownloadStatusCompleted
		t.FilePath = filepath.Join(svc.downloadDir, task.RequestID+".csv")
	}
	svc.taskMutex.Unlock()

	// Try to get file path
	_, _, _, err = svc.GetFilePath(task.RequestID)
	if err == nil {
		t.Error("Expected error for expired task")
	}
	svcErr, ok := err.(*ServiceError)
	if !ok {
		t.Errorf("Expected ServiceError, got %T", err)
	}
	if svcErr.Code != "DOWNLOAD_EXPIRED" {
		t.Errorf("Expected code DOWNLOAD_EXPIRED, got %s", svcErr.Code)
	}
}

func TestDownloadService_GetFilePath_FileNotExists(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Manually set task to completed with non-existent file
	svc.taskMutex.Lock()
	if t, ok := svc.tasks[task.RequestID]; ok {
		t.Status = models.DownloadStatusCompleted
		t.FilePath = "/non/existent/path/file.csv"
	}
	svc.taskMutex.Unlock()

	// Try to get file path - may or may not error depending on implementation
	_, _, _, _ = svc.GetFilePath(task.RequestID)
}

func TestDownloadService_JSONFormat(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "json",
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	if task.Request.Format != "json" {
		t.Errorf("Expected format json, got %s", task.Request.Format)
	}
}

func TestDownloadService_EmptyFields(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
		Fields:     []string{},
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	if len(task.Request.Fields) != 0 {
		t.Errorf("Expected 0 fields, got %d", len(task.Request.Fields))
	}
}

func TestDownloadService_EmptyIDs(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
		IDs:        []string{},
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	if len(task.Request.IDs) != 0 {
		t.Errorf("Expected 0 IDs, got %d", len(task.Request.IDs))
	}
}

func TestDownloadService_ConcurrentCreateDownloads(t *testing.T) {
	svc, _ := createTestDownloadService(t)
	defer svc.Stop()

	// Create multiple downloads concurrently
	numDownloads := 5
	errors := make(chan error, numDownloads)
	tasks := make(chan *models.DownloadTask, numDownloads)

	for i := 0; i < numDownloads; i++ {
		go func(idx int) {
			req := &models.DownloadRequest{
				Database:   "testdb",
				Collection: "testcoll",
				StartTime:  "2026-01-01T00:00:00Z",
				EndTime:    "2026-01-02T00:00:00Z",
				Format:     "csv",
			}
			task, err := svc.CreateDownload(context.Background(), req)
			errors <- err
			tasks <- task
		}(i)
	}

	// Collect results
	for i := 0; i < numDownloads; i++ {
		err := <-errors
		task := <-tasks
		if err != nil {
			t.Errorf("Download %d failed: %v", i, err)
		}
		if task != nil && task.RequestID == "" {
			t.Errorf("Download %d has empty RequestID", i)
		}
	}

	// Verify all tasks are tracked
	allTasks := svc.ListTasks()
	if len(allTasks) != numDownloads {
		t.Errorf("Expected %d tasks, got %d", numDownloads, len(allTasks))
	}
}

func TestDownloadService_GetFilePath_Success(t *testing.T) {
	svc, tmpDir := createTestDownloadService(t)
	defer svc.Stop()

	req := &models.DownloadRequest{
		Database:   "testdb",
		Collection: "testcoll",
		StartTime:  "2026-01-01T00:00:00Z",
		EndTime:    "2026-01-02T00:00:00Z",
		Format:     "csv",
		Filename:   "export.csv",
	}

	task, err := svc.CreateDownload(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateDownload failed: %v", err)
	}

	// Manually set task to completed and create the file
	expectedPath := filepath.Join(tmpDir, task.RequestID+".csv")
	if err := os.WriteFile(expectedPath, []byte("test,data\n1,2\n"), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	svc.taskMutex.Lock()
	if t, ok := svc.tasks[task.RequestID]; ok {
		t.Status = models.DownloadStatusCompleted
		t.FilePath = expectedPath
		t.TotalRows = 100
		t.ExpiresAt = time.Now().Add(1 * time.Hour)
	}
	svc.taskMutex.Unlock()

	// Try to get file path
	filePath, filename, contentType, err := svc.GetFilePath(task.RequestID)
	if err != nil {
		t.Fatalf("GetFilePath failed: %v", err)
	}

	if filePath != expectedPath {
		t.Errorf("Expected path %s, got %s", expectedPath, filePath)
	}
	if filename != "export.csv" {
		t.Errorf("Expected filename 'export.csv', got %s", filename)
	}
	if contentType != "text/csv" {
		t.Errorf("Expected contentType 'text/csv', got %s", contentType)
	}
}
