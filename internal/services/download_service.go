package services

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/soltixdb/soltix/internal/coordinator"
	"github.com/soltixdb/soltix/internal/handlers/processing"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/metadata"
	"github.com/soltixdb/soltix/internal/models"
)

const (
	// DefaultDownloadExpiration is the default expiration time for download files
	DefaultDownloadExpiration = 1 * time.Hour

	// DefaultCleanupInterval is the interval for cleaning up expired downloads
	DefaultCleanupInterval = 5 * time.Minute

	// MaxConcurrentDownloads is the maximum number of concurrent download tasks
	MaxConcurrentDownloads = 10

	// ChunkSize is the number of rows to fetch per chunk during download
	DownloadChunkSize = 10000

	// etcdDownloadPrefix is the prefix for download tasks in etcd
	etcdDownloadPrefix = "/soltix/downloads/"
)

// DownloadService handles async download/export operations
type DownloadService struct {
	logger           *logging.Logger
	metadataManager  metadata.Manager
	queryCoordinator *coordinator.QueryCoordinator
	processor        *processing.Processor

	// Task management
	tasks     map[string]*models.DownloadTask
	taskMutex sync.RWMutex

	// Download directory
	downloadDir string

	// Configuration
	expirationDuration time.Duration

	// Worker pool
	taskQueue chan *models.DownloadTask
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// NewDownloadService creates a new DownloadService
func NewDownloadService(
	logger *logging.Logger,
	metadataManager metadata.Manager,
	queryCoordinator *coordinator.QueryCoordinator,
	processor *processing.Processor,
	downloadDir string,
) *DownloadService {
	// Ensure download directory exists
	if err := os.MkdirAll(downloadDir, 0o755); err != nil {
		logger.Error("Failed to create download directory", "error", err, "path", downloadDir)
	}

	s := &DownloadService{
		logger:             logger,
		metadataManager:    metadataManager,
		queryCoordinator:   queryCoordinator,
		processor:          processor,
		tasks:              make(map[string]*models.DownloadTask),
		downloadDir:        downloadDir,
		expirationDuration: DefaultDownloadExpiration,
		taskQueue:          make(chan *models.DownloadTask, 100),
		stopChan:           make(chan struct{}),
	}

	// Load existing tasks from etcd
	s.loadTasksFromEtcd()

	// Start worker pool
	s.startWorkers(MaxConcurrentDownloads)

	// Start cleanup goroutine
	go s.cleanupLoop()

	return s
}

// startWorkers starts the worker pool for processing download tasks
func (s *DownloadService) startWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
	s.logger.Info("Download workers started", "count", numWorkers)
}

// worker processes download tasks from the queue
func (s *DownloadService) worker(id int) {
	defer s.wg.Done()

	for {
		select {
		case task := <-s.taskQueue:
			s.processTask(task)
		case <-s.stopChan:
			s.logger.Debug("Download worker stopping", "worker_id", id)
			return
		}
	}
}

// Stop stops the download service
func (s *DownloadService) Stop() {
	close(s.stopChan)
	s.wg.Wait()
	s.logger.Info("Download service stopped")
}

// CreateDownload creates a new download task
func (s *DownloadService) CreateDownload(ctx context.Context, request *models.DownloadRequest) (*models.DownloadTask, error) {
	// Validate collection existence
	if err := s.metadataManager.ValidateCollection(ctx, request.Database, request.Collection); err != nil {
		return nil, &ServiceError{
			Code:    "COLLECTION_NOT_FOUND",
			Message: err.Error(),
		}
	}

	// Generate request ID
	requestID := uuid.New().String()

	// Create task
	task := models.NewDownloadTask(requestID, *request, s.expirationDuration)

	// Store task in memory and etcd
	s.taskMutex.Lock()
	s.tasks[requestID] = task
	s.taskMutex.Unlock()

	// Persist to etcd
	if err := s.saveTaskToEtcd(ctx, task); err != nil {
		s.logger.Error("Failed to save task to etcd", "request_id", requestID, "error", err)
	}

	// Queue task for processing
	select {
	case s.taskQueue <- task:
		s.logger.Info("Download task queued",
			"request_id", requestID,
			"database", request.Database,
			"collection", request.Collection,
			"format", request.Format,
		)
	default:
		// Queue is full
		s.taskMutex.Lock()
		task.Status = models.DownloadStatusFailed
		task.Error = "download queue is full, please try again later"
		s.taskMutex.Unlock()
		// Update etcd
		if err := s.saveTaskToEtcd(ctx, task); err != nil {
			s.logger.Error("Failed to update task in etcd", "request_id", requestID, "error", err)
		}
		// Return a copy to avoid race conditions
		taskCopy := *task
		return &taskCopy, nil
	}

	// Return a copy to avoid race conditions
	s.taskMutex.RLock()
	taskCopy := *task
	s.taskMutex.RUnlock()
	return &taskCopy, nil
}

// GetTaskStatus returns the status of a download task
func (s *DownloadService) GetTaskStatus(requestID string) (*models.DownloadTask, error) {
	s.taskMutex.RLock()
	task, exists := s.tasks[requestID]
	s.taskMutex.RUnlock()

	if !exists {
		// Try to load from etcd
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		task, err := s.loadTaskFromEtcd(ctx, requestID)
		if err != nil || task == nil {
			return nil, &ServiceError{
				Code:    "TASK_NOT_FOUND",
				Message: "download task not found",
			}
		}

		// Cache in memory
		s.taskMutex.Lock()
		s.tasks[requestID] = task
		s.taskMutex.Unlock()
	}

	// Check if expired
	if task.IsExpired() && task.Status == models.DownloadStatusCompleted {
		s.taskMutex.Lock()
		task.Status = models.DownloadStatusExpired
		s.taskMutex.Unlock()

		// Update etcd
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.saveTaskToEtcd(ctx, task); err != nil {
			s.logger.Error("Failed to update expired task in etcd", "request_id", requestID, "error", err)
		}
	}

	// Return a copy to avoid race conditions
	s.taskMutex.RLock()
	taskCopy := *task
	s.taskMutex.RUnlock()
	return &taskCopy, nil
}

// GetFilePath returns the file path for a completed download
func (s *DownloadService) GetFilePath(requestID string) (string, string, string, error) {
	task, err := s.GetTaskStatus(requestID)
	if err != nil {
		return "", "", "", err
	}

	if !task.CanDownload() {
		if task.IsExpired() {
			return "", "", "", &ServiceError{
				Code:    "DOWNLOAD_EXPIRED",
				Message: "download has expired",
			}
		}
		return "", "", "", &ServiceError{
			Code:    "DOWNLOAD_NOT_READY",
			Message: "download is not ready yet, status: " + string(task.Status),
		}
	}

	return task.FilePath, task.Filename, task.ContentType, nil
}

// processTask processes a download task
func (s *DownloadService) processTask(task *models.DownloadTask) {
	startTime := time.Now()

	// Update status to processing
	s.taskMutex.Lock()
	task.Status = models.DownloadStatusProcessing
	task.StartedAt = &startTime
	s.taskMutex.Unlock()

	s.logger.Info("Processing download task",
		"request_id", task.RequestID,
		"database", task.Request.Database,
		"collection", task.Request.Collection,
	)

	// Create context with timeout (1 hour max)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	// Process based on format
	var err error
	switch task.Request.Format {
	case "csv":
		err = s.generateCSV(ctx, task)
	case "json":
		err = s.generateJSON(ctx, task)
	default:
		err = fmt.Errorf("unsupported format: %s", task.Request.Format)
	}

	completedAt := time.Now()

	s.taskMutex.Lock()
	if err != nil {
		task.Status = models.DownloadStatusFailed
		task.Error = err.Error()
		s.logger.Error("Download task failed",
			"request_id", task.RequestID,
			"error", err,
			"duration", completedAt.Sub(startTime),
		)
	} else {
		task.Status = models.DownloadStatusCompleted
		task.CompletedAt = &completedAt
		task.Progress = 100

		// Get file size
		if info, err := os.Stat(task.FilePath); err == nil {
			task.FileSize = info.Size()
		}

		s.logger.Info("Download task completed",
			"request_id", task.RequestID,
			"total_rows", task.TotalRows,
			"file_size", task.FileSize,
			"duration", completedAt.Sub(startTime),
		)
	}
	s.taskMutex.Unlock()

	// Persist updated status to etcd
	if err := s.saveTaskToEtcd(ctx, task); err != nil {
		s.logger.Error("Failed to save completed task to etcd", "request_id", task.RequestID, "error", err)
	}
}

// generateCSV generates a CSV file from query results
func (s *DownloadService) generateCSV(ctx context.Context, task *models.DownloadTask) error {
	// Create output file
	filePath := filepath.Join(s.downloadDir, task.RequestID+".csv")
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Use buffered writer for better performance
	bufferedWriter := bufio.NewWriterSize(file, 64*1024) // 64KB buffer
	defer func() { _ = bufferedWriter.Flush() }()

	csvWriter := csv.NewWriter(bufferedWriter)
	defer csvWriter.Flush()

	// Convert to query request
	queryReq := task.Request.ToQueryRequest()

	// Stream data in chunks using gRPC streaming
	streamReq := &coordinator.StreamQueryRequest{
		QueryRequest: coordinator.QueryRequest{
			Database:   queryReq.Database,
			Collection: queryReq.Collection,
			DeviceIDs:  queryReq.IDs,
			StartTime:  queryReq.StartTimeParsed,
			EndTime:    queryReq.EndTimeParsed,
			Fields:     queryReq.Fields,
			Interval:   queryReq.Interval,
		},
		ChunkSize: DownloadChunkSize,
	}

	chunkChan := s.queryCoordinator.QueryStream(ctx, streamReq)

	var headerWritten bool
	var totalRows int64
	var fieldNames []string

	for chunk := range chunkChan {
		if chunk.Error != nil {
			return chunk.Error
		}

		// Process data points from chunk
		for _, dp := range chunk.DataPoints {
			// Write header on first data point
			if !headerWritten {
				fieldNames = getFieldNames(dp.Fields)
				header := append([]string{"id", "time"}, fieldNames...)
				if err := csvWriter.Write(header); err != nil {
					return fmt.Errorf("failed to write CSV header: %w", err)
				}
				headerWritten = true
			}

			// Write data row
			row := []string{dp.ID, dp.Time}
			for _, fn := range fieldNames {
				if val, ok := dp.Fields[fn]; ok {
					row = append(row, fmt.Sprintf("%v", val))
				} else {
					row = append(row, "")
				}
			}
			if err := csvWriter.Write(row); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
			totalRows++
		}

		// Update progress
		s.taskMutex.Lock()
		task.TotalRows = totalRows
		// Estimate progress (we don't know total upfront)
		if task.Progress < 95 {
			task.Progress += 5
		}
		s.taskMutex.Unlock()
	}

	task.FilePath = filePath
	task.TotalRows = totalRows
	return nil
}

// getFieldNames extracts sorted field names from a map
func getFieldNames(fields map[string]interface{}) []string {
	names := make([]string, 0, len(fields))
	for k := range fields {
		names = append(names, k)
	}
	return names
}

// generateJSON generates a JSON file from query results
func (s *DownloadService) generateJSON(ctx context.Context, task *models.DownloadTask) error {
	// Create output file
	filePath := filepath.Join(s.downloadDir, task.RequestID+".json")
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Use buffered writer
	bufferedWriter := bufio.NewWriterSize(file, 64*1024)
	defer func() { _ = bufferedWriter.Flush() }()
	// Start JSON array
	if _, err := bufferedWriter.WriteString("[\n"); err != nil {
		return err
	}

	// Convert to query request
	queryReq := task.Request.ToQueryRequest()

	// Stream data in chunks
	streamReq := &coordinator.StreamQueryRequest{
		QueryRequest: coordinator.QueryRequest{
			Database:   queryReq.Database,
			Collection: queryReq.Collection,
			DeviceIDs:  queryReq.IDs,
			StartTime:  queryReq.StartTimeParsed,
			EndTime:    queryReq.EndTimeParsed,
			Fields:     queryReq.Fields,
			Interval:   queryReq.Interval,
		},
		ChunkSize: DownloadChunkSize,
	}

	chunkChan := s.queryCoordinator.QueryStream(ctx, streamReq)

	var totalRows int64
	firstItem := true

	for chunk := range chunkChan {
		if chunk.Error != nil {
			return chunk.Error
		}

		// Write each data point as JSON
		for _, dp := range chunk.DataPoints {
			if !firstItem {
				if _, err := bufferedWriter.WriteString(",\n"); err != nil {
					return err
				}
			}
			firstItem = false

			// Create JSON object for this data point
			jsonObj := map[string]interface{}{
				"id":   dp.ID,
				"time": dp.Time,
			}
			for k, v := range dp.Fields {
				jsonObj[k] = v
			}

			jsonBytes, err := json.Marshal(jsonObj)
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}

			if _, err := bufferedWriter.Write(jsonBytes); err != nil {
				return err
			}
			totalRows++
		}

		// Update progress
		s.taskMutex.Lock()
		task.TotalRows = totalRows
		if task.Progress < 95 {
			task.Progress += 5
		}
		s.taskMutex.Unlock()
	}

	// End JSON array
	if _, err := bufferedWriter.WriteString("\n]"); err != nil {
		return err
	}

	task.FilePath = filePath
	task.TotalRows = totalRows
	return nil
}

// cleanupLoop periodically cleans up expired downloads
func (s *DownloadService) cleanupLoop() {
	ticker := time.NewTicker(DefaultCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanupExpiredDownloads()
		case <-s.stopChan:
			return
		}
	}
}

// cleanupExpiredDownloads removes expired download files and tasks
func (s *DownloadService) cleanupExpiredDownloads() {
	s.taskMutex.Lock()
	defer s.taskMutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Now()
	expiredCount := 0

	for requestID, task := range s.tasks {
		// Check if task is expired (add buffer time for cleanup)
		if now.After(task.ExpiresAt.Add(5 * time.Minute)) {
			// First, delete from etcd
			if err := s.deleteTaskFromEtcd(ctx, requestID); err != nil {
				s.logger.Error("Failed to delete task from etcd",
					"request_id", requestID,
					"error", err,
				)
				// Continue anyway to clean up file
			}

			// Then, delete file if exists
			if task.FilePath != "" {
				if err := os.Remove(task.FilePath); err != nil && !os.IsNotExist(err) {
					s.logger.Error("Failed to remove expired download file",
						"request_id", requestID,
						"error", err,
					)
				}
			}

			// Remove task from memory
			delete(s.tasks, requestID)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		s.logger.Info("Cleaned up expired downloads", "count", expiredCount)
	}
}

// ListTasks returns all active download tasks (for admin/debugging)
func (s *DownloadService) ListTasks() []*models.DownloadTask {
	s.taskMutex.RLock()
	defer s.taskMutex.RUnlock()

	tasks := make([]*models.DownloadTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		tasks = append(tasks, task)
	}
	return tasks
}

// ============================================================================
// Etcd persistence methods
// ============================================================================

// saveTaskToEtcd persists a download task to etcd
func (s *DownloadService) saveTaskToEtcd(ctx context.Context, task *models.DownloadTask) error {
	key := etcdDownloadPrefix + task.RequestID

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	if err := s.metadataManager.Put(ctx, key, string(data)); err != nil {
		return fmt.Errorf("failed to save task to etcd: %w", err)
	}

	return nil
}

// loadTaskFromEtcd loads a single download task from etcd
func (s *DownloadService) loadTaskFromEtcd(ctx context.Context, requestID string) (*models.DownloadTask, error) {
	key := etcdDownloadPrefix + requestID

	data, err := s.metadataManager.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get task from etcd: %w", err)
	}

	if data == "" {
		return nil, nil
	}

	var task models.DownloadTask
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

// deleteTaskFromEtcd removes a download task from etcd
func (s *DownloadService) deleteTaskFromEtcd(ctx context.Context, requestID string) error {
	key := etcdDownloadPrefix + requestID

	if err := s.metadataManager.Delete(ctx, key); err != nil {
		return fmt.Errorf("failed to delete task from etcd: %w", err)
	}

	return nil
}

// loadTasksFromEtcd loads all existing download tasks from etcd on startup
func (s *DownloadService) loadTasksFromEtcd() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tasks, err := s.metadataManager.GetPrefix(ctx, etcdDownloadPrefix)
	if err != nil {
		s.logger.Error("Failed to load download tasks from etcd", "error", err)
		return
	}

	loadedCount := 0
	expiredCount := 0
	now := time.Now()

	for key, data := range tasks {
		if data == "" {
			continue // Skip deleted entries
		}

		var task models.DownloadTask
		if err := json.Unmarshal([]byte(data), &task); err != nil {
			s.logger.Error("Failed to unmarshal task from etcd", "key", key, "error", err)
			continue
		}

		// Skip expired tasks (will be cleaned up by cleanup loop)
		if now.After(task.ExpiresAt.Add(5 * time.Minute)) {
			expiredCount++
			continue
		}

		// Add to memory cache
		s.taskMutex.Lock()
		s.tasks[task.RequestID] = &task
		s.taskMutex.Unlock()
		loadedCount++

		// Re-queue pending/processing tasks that were interrupted
		if task.Status == models.DownloadStatusPending || task.Status == models.DownloadStatusProcessing {
			// Reset status to pending for re-processing
			task.Status = models.DownloadStatusPending
			select {
			case s.taskQueue <- &task:
				s.logger.Info("Re-queued interrupted download task", "request_id", task.RequestID)
			default:
				s.logger.Warn("Failed to re-queue download task, queue full", "request_id", task.RequestID)
			}
		}
	}

	if loadedCount > 0 || expiredCount > 0 {
		s.logger.Info("Loaded download tasks from etcd",
			"loaded", loadedCount,
			"expired_skipped", expiredCount,
		)
	}
}
