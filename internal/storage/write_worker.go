package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/wal"
)

// WriteWorkerPool manages write workers partitioned by database + collection + date
// Each worker handles writes for a specific partition to enable parallel processing
type WriteWorkerPool struct {
	workers     map[string]*WriteWorker // key: "database:collection:YYYY-MM-DD"
	workersMu   sync.RWMutex
	logger      *logging.Logger
	wal         wal.PartitionedWriter // Partitioned WAL by database+date
	memStore    *MemoryStore
	flushPool   *FlushWorkerPool // Event-driven flush pool
	memMaxAge   time.Duration    // Max age for data to be stored in memory
	bufferSize  int              // Channel buffer size per worker
	idleTimeout time.Duration    // Worker idle timeout before shutdown
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// WriteWorker handles writes for a specific partition (database + collection + date)
type WriteWorker struct {
	key        string          // partition key: "database:collection:YYYY-MM-DD"
	database   string          // database name
	collection string          // collection name
	date       string          // date string YYYY-MM-DD
	dateParsed time.Time       // parsed date for WAL partitioning
	dataCh     chan *WriteTask // buffered channel for incoming writes
	logger     *logging.Logger
	wal        wal.PartitionedWriter // Partitioned WAL
	memStore   *MemoryStore
	flushPool  *FlushWorkerPool // Event-driven flush pool
	memMaxAge  time.Duration    // Max age for data to be stored in memory
	lastActive time.Time
	stopCh     chan struct{}
	stopped    bool
	mu         sync.Mutex
}

// WriteTask represents a write task to be processed by a worker
type WriteTask struct {
	Entry     *wal.Entry
	DataPoint *DataPoint
	ResultCh  chan error // Optional: for sync writes
}

// NewWriteWorkerPool creates a new write worker pool
func NewWriteWorkerPool(
	logger *logging.Logger,
	walWriter wal.PartitionedWriter,
	memStore *MemoryStore,
	flushPool *FlushWorkerPool,
) *WriteWorkerPool {
	return &WriteWorkerPool{
		workers:     make(map[string]*WriteWorker),
		logger:      logger,
		wal:         walWriter,
		memStore:    memStore,
		flushPool:   flushPool,
		memMaxAge:   memStore.GetMaxAge(), // Get max age from memory store
		bufferSize:  1000,                 // Buffer 1000 writes per worker
		idleTimeout: 5 * time.Minute,      // Shutdown idle workers after 5 min
		stopCh:      make(chan struct{}),
	}
}

// Start starts the worker pool and idle worker cleanup routine
func (p *WriteWorkerPool) Start() {
	p.wg.Add(1)
	go p.cleanupIdleWorkers()
	p.logger.Info("Write worker pool started")
}

// Stop stops all workers and the pool
func (p *WriteWorkerPool) Stop() {
	close(p.stopCh)

	// Stop all workers
	p.workersMu.Lock()
	for _, worker := range p.workers {
		worker.Stop()
	}
	p.workersMu.Unlock()

	p.wg.Wait()
	p.logger.Info("Write worker pool stopped")
}

// Submit submits a write task to the appropriate worker
// Creates a new worker if one doesn't exist for the partition
func (p *WriteWorkerPool) Submit(msg WriteMessage) error {
	// Parse timestamp to get date
	timeParsed, err := time.Parse(time.RFC3339, msg.Time)
	if err != nil {
		p.logger.Error("WriteWorkerPool.Submit: failed to parse time",
			"id", msg.ID,
			"time", msg.Time,
			"error", err)
		return fmt.Errorf("failed to parse time: %w", err)
	}

	// Generate partition key: database:collection:YYYY-MM-DD
	dateStr := timeParsed.Format("2006-01-02")
	partitionKey := fmt.Sprintf("%s:%s:%s", msg.Database, msg.Collection, dateStr)

	// Get or create worker for this partition
	worker := p.getOrCreateWorker(partitionKey, msg.Database, msg.Collection, dateStr)

	// Create WAL entry
	entry := &wal.Entry{
		Type:       wal.EntryTypeWrite,
		Database:   msg.Database,
		Collection: msg.Collection,
		ShardID:    msg.ShardID,
		GroupID:    msg.GroupID,
		Time:       msg.Time,
		ID:         msg.ID,
		Fields:     msg.Fields,
		Timestamp:  timeParsed.UnixNano(),
	}

	// Create data point
	dataPoint := &DataPoint{
		Database:   msg.Database,
		Collection: msg.Collection,
		ShardID:    msg.ShardID,
		GroupID:    msg.GroupID,
		Time:       timeParsed,
		ID:         msg.ID,
		Fields:     msg.Fields,
		InsertedAt: time.Now(),
	}

	// Create task
	task := &WriteTask{
		Entry:     entry,
		DataPoint: dataPoint,
	}

	// Submit to worker (non-blocking with buffered channel)
	select {
	case worker.dataCh <- task:
		return nil
	default:
		// Channel full, try with timeout to prevent indefinite blocking
	}

	// Retry with timeout - log warning and attempt with backpressure
	p.logger.Warn("Worker channel full, applying backpressure",
		"partition", partitionKey,
		"buffer_size", p.bufferSize,
		"pending", len(worker.dataCh))

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	select {
	case worker.dataCh <- task:
		return nil
	case <-timer.C:
		// Timeout - this is a critical error, should not happen in normal operation
		p.logger.Error("Worker channel blocked for too long, potential data loss",
			"partition", partitionKey,
			"buffer_size", p.bufferSize,
			"id", msg.ID)
		return fmt.Errorf("worker channel blocked: buffer full for partition %s", partitionKey)
	case <-p.stopCh:
		return fmt.Errorf("worker pool stopping")
	}
}

// SubmitSync submits a write task and waits for completion
func (p *WriteWorkerPool) SubmitSync(msg WriteMessage) error {
	// Parse timestamp to get date
	timeParsed, err := time.Parse(time.RFC3339, msg.Time)
	if err != nil {
		return fmt.Errorf("failed to parse time: %w", err)
	}

	// Generate partition key: database:collection:YYYY-MM-DD
	dateStr := timeParsed.Format("2006-01-02")
	partitionKey := fmt.Sprintf("%s:%s:%s", msg.Database, msg.Collection, dateStr)

	// Get or create worker
	worker := p.getOrCreateWorker(partitionKey, msg.Database, msg.Collection, dateStr)

	// Create WAL entry
	entry := &wal.Entry{
		Type:       wal.EntryTypeWrite,
		Database:   msg.Database,
		Collection: msg.Collection,
		ShardID:    msg.ShardID,
		GroupID:    msg.GroupID,
		Time:       msg.Time,
		ID:         msg.ID,
		Fields:     msg.Fields,
		Timestamp:  timeParsed.UnixNano(),
	}

	// Create data point
	dataPoint := &DataPoint{
		Database:   msg.Database,
		Collection: msg.Collection,
		ShardID:    msg.ShardID,
		GroupID:    msg.GroupID,
		Time:       timeParsed,
		ID:         msg.ID,
		Fields:     msg.Fields,
		InsertedAt: time.Now(),
	}

	// Create task with result channel
	resultCh := make(chan error, 1)
	task := &WriteTask{
		Entry:     entry,
		DataPoint: dataPoint,
		ResultCh:  resultCh,
	}

	// Submit to worker
	worker.dataCh <- task

	// Wait for result
	return <-resultCh
}

// getOrCreateWorker gets an existing worker or creates a new one
func (p *WriteWorkerPool) getOrCreateWorker(key, database, collection, date string) *WriteWorker {
	// Fast path: read lock
	p.workersMu.RLock()
	worker, exists := p.workers[key]
	p.workersMu.RUnlock()

	if exists {
		worker.touch()
		return worker
	}

	// Slow path: write lock
	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	// Double check
	if worker, exists = p.workers[key]; exists {
		worker.touch()
		return worker
	}

	// Parse date for partitioned WAL
	dateParsed, _ := time.Parse("2006-01-02", date)

	// Create new worker
	worker = &WriteWorker{
		key:        key,
		database:   database,
		collection: collection,
		date:       date,
		dateParsed: dateParsed,
		dataCh:     make(chan *WriteTask, p.bufferSize),
		logger:     p.logger,
		wal:        p.wal,
		memStore:   p.memStore,
		flushPool:  p.flushPool,
		memMaxAge:  p.memMaxAge,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}

	p.workers[key] = worker
	go worker.run()

	p.logger.Info("Created write worker",
		"partition", key,
		"database", database,
		"collection", collection,
		"date", date)

	return worker
}

// cleanupIdleWorkers periodically removes idle workers
func (p *WriteWorkerPool) cleanupIdleWorkers() {
	defer p.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.doCleanup()
		}
	}
}

// doCleanup removes workers that have been idle too long
func (p *WriteWorkerPool) doCleanup() {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	now := time.Now()
	for key, worker := range p.workers {
		if now.Sub(worker.getLastActive()) > p.idleTimeout {
			worker.Stop()
			delete(p.workers, key)
			p.logger.Info("Removed idle write worker",
				"partition", key,
				"idle_time", now.Sub(worker.getLastActive()))
		}
	}
}

// Stats returns worker pool statistics
func (p *WriteWorkerPool) Stats() map[string]interface{} {
	p.workersMu.RLock()
	defer p.workersMu.RUnlock()

	workerStats := make([]map[string]interface{}, 0, len(p.workers))
	for key, worker := range p.workers {
		workerStats = append(workerStats, map[string]interface{}{
			"partition":    key,
			"pending":      len(worker.dataCh),
			"last_active":  worker.getLastActive(),
			"idle_seconds": time.Since(worker.getLastActive()).Seconds(),
		})
	}

	return map[string]interface{}{
		"worker_count": len(p.workers),
		"buffer_size":  p.bufferSize,
		"idle_timeout": p.idleTimeout.String(),
		"workers":      workerStats,
	}
}

// WriteWorker methods

// run is the main loop for the worker
func (w *WriteWorker) run() {
	w.logger.Debug("Write worker started", "partition", w.key)

	for {
		select {
		case <-w.stopCh:
			// Drain remaining tasks before exit
			w.drain()
			return
		case task := <-w.dataCh:
			w.processTask(task)
		}
	}
}

// processTask processes a single write task
func (w *WriteWorker) processTask(task *WriteTask) {
	w.touch()
	err := w.processWrite(task)

	// Send result if sync write
	if task.ResultCh != nil {
		task.ResultCh <- err
	}

	if err != nil {
		w.logger.Error("Write task failed",
			"partition", w.key,
			"id", task.Entry.ID,
			"time", task.Entry.Time,
			"error", err)
	}
}

// processWrite handles all write operations
// Only data within memMaxAge is stored in MemoryStore
// All data is written to WAL for durability
// Notification is sent only when WAL writes to a new segment file
func (w *WriteWorker) processWrite(task *WriteTask) error {
	// Write to partitioned WAL (always, for durability)
	// WritePartitioned returns whether this write triggered a new segment
	result, err := w.wal.WritePartitioned(task.Entry, w.database, w.dateParsed)
	if err != nil {
		w.logger.Error("processWrite: WAL write failed",
			"partition", w.key,
			"id", task.Entry.ID,
			"time", task.Entry.Time,
			"error", err)
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Check if data is recent enough for memory store
	dataAge := time.Since(task.DataPoint.Time)
	isRecentData := dataAge <= w.memMaxAge

	if isRecentData {
		// Recent data: write to memory store for fast queries
		if err := w.memStore.Write(task.DataPoint); err != nil {
			return fmt.Errorf("failed to write to memory store: %w", err)
		}
	}

	// Only notify flush pool when WAL writes to a new segment file
	// This drastically reduces notifications - only 1 per segment instead of 1 per record
	if result.IsNewSegment && w.flushPool != nil {
		w.flushPool.Notify(WriteNotification{
			Database:   w.database,
			Date:       w.dateParsed,
			Collection: w.collection,
			EntryCount: 1,
			Immediate:  !isRecentData, // Immediate flush for historical data
		})
		w.logger.Info("New WAL segment created, notified flush pool",
			"partition", w.key,
			"immediate", !isRecentData)
	}

	return nil
}

// drain processes remaining tasks in the channel
func (w *WriteWorker) drain() {
	for {
		select {
		case task := <-w.dataCh:
			w.processTask(task)
		default:
			return
		}
	}
}

// Stop stops the worker
func (w *WriteWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopped {
		return
	}
	w.stopped = true
	close(w.stopCh)
}

// touch updates last active time
func (w *WriteWorker) touch() {
	w.mu.Lock()
	w.lastActive = time.Now()
	w.mu.Unlock()
}

// getLastActive returns last active time
func (w *WriteWorker) getLastActive() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastActive
}
