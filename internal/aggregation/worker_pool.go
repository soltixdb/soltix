package aggregation

import (
	"fmt"
	"sync"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

// =============================================================================
// AggregationWorkerPool - One goroutine per partition key
// Partition key: db:collection:level:date
// Each partition has exactly 1 dedicated worker goroutine
// =============================================================================

// WorkerPoolConfig contains configuration for aggregation worker pool
type WorkerPoolConfig struct {
	// MaxActiveWorkers limits the number of active worker goroutines
	// This prevents too many goroutines when there are many partitions
	MaxActiveWorkers int

	// WorkerIdleTimeout is how long a worker waits before shutting down
	WorkerIdleTimeout time.Duration

	// QueueSize is the buffer size for each worker's notification queue
	QueueSize int

	// BatchDelay is the delay before processing after receiving notification
	// This allows batching multiple notifications
	BatchDelay time.Duration

	// Timezone for time calculations
	Timezone *time.Location
}

// DefaultWorkerPoolConfig returns default configuration
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		MaxActiveWorkers:  100, // Max 100 active partition workers
		WorkerIdleTimeout: 10 * time.Minute,
		QueueSize:         100,
		BatchDelay:        5 * time.Second,
		Timezone:          time.UTC,
	}
}

// AggregationNotification represents a notification to aggregate a partition
type AggregationNotification struct {
	Database   string
	Collection string
	Level      AggregationLevel
	TimeKey    time.Time // Time key for the partition
}

// PartitionWorker represents a dedicated worker for one partition
// Each partition key has exactly one worker goroutine
type PartitionWorker struct {
	key        string
	database   string
	collection string
	level      AggregationLevel
	timeKey    time.Time

	// Notification channel - only this worker reads from it
	notifyCh chan struct{}

	// State
	pendingCount  int
	lastNotify    time.Time
	lastProcessed time.Time
	state         workerState // idle, pending, running, waitingForJob
	idleSince     time.Time   // When worker started waiting for new job

	mu     sync.Mutex
	stopCh chan struct{}
}

// workerState represents the state of a partition worker
type workerState int

const (
	workerIdle          workerState = iota // Not running, no goroutine
	workerPending                          // Waiting for semaphore (in pending queue)
	workerRunning                          // Actively processing
	workerWaitingForJob                    // Finished processing, waiting for new job (has semaphore)
)

// AggregationWorkerPool manages partition workers
type AggregationWorkerPool struct {
	config     WorkerPoolConfig
	logger     *logging.Logger
	rawReader  RawDataReader
	aggStorage AggregationStorage

	// Workers by partition key - one goroutine per key
	workers   map[string]*PartitionWorker
	workersMu sync.RWMutex

	// Semaphore to limit active workers
	semaphore chan struct{}

	// Pending queue for workers waiting for semaphore
	pendingQueue chan *PartitionWorker

	// Input channel for notifications (from external sources)
	notifyCh chan AggregationNotification

	// Cascade: notify next level when done
	nextLevelPool map[AggregationLevel]*AggregationWorkerPool

	// Stats
	totalProcessed int64
	totalFailed    int64
	statsMu        sync.RWMutex

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewAggregationWorkerPool creates a new aggregation worker pool
func NewAggregationWorkerPool(
	config WorkerPoolConfig,
	logger *logging.Logger,
	rawReader RawDataReader,
	aggStorage AggregationStorage,
) *AggregationWorkerPool {
	if config.MaxActiveWorkers <= 0 {
		config.MaxActiveWorkers = 100
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 100
	}
	if config.Timezone == nil {
		config.Timezone = time.UTC
	}

	return &AggregationWorkerPool{
		config:        config,
		logger:        logger,
		rawReader:     rawReader,
		aggStorage:    aggStorage,
		workers:       make(map[string]*PartitionWorker),
		semaphore:     make(chan struct{}, config.MaxActiveWorkers),
		pendingQueue:  make(chan *PartitionWorker, config.MaxActiveWorkers*10),
		notifyCh:      make(chan AggregationNotification, config.QueueSize*10),
		nextLevelPool: make(map[AggregationLevel]*AggregationWorkerPool),
		stopCh:        make(chan struct{}),
	}
}

// SetNextLevelPool sets the pool to notify when aggregation completes
func (p *AggregationWorkerPool) SetNextLevelPool(level AggregationLevel, pool *AggregationWorkerPool) {
	p.nextLevelPool[level] = pool
}

// Start starts the worker pool
func (p *AggregationWorkerPool) Start() {
	// Start dispatcher
	p.wg.Add(1)
	go p.dispatchLoop()

	// Start pending queue processor
	p.wg.Add(1)
	go p.pendingQueueLoop()

	// Start cleanup loop
	p.wg.Add(1)
	go p.cleanupLoop()

	p.logger.Info("Aggregation worker pool started",
		"max_active_workers", p.config.MaxActiveWorkers,
		"batch_delay", p.config.BatchDelay)
}

// Stop stops the worker pool
func (p *AggregationWorkerPool) Stop() {
	close(p.stopCh)

	// Stop all workers
	p.workersMu.Lock()
	for _, worker := range p.workers {
		worker.mu.Lock()
		if worker.state == workerRunning {
			close(worker.stopCh)
		}
		worker.mu.Unlock()
	}
	p.workersMu.Unlock()

	p.wg.Wait()
	p.logger.Info("Aggregation worker pool stopped")
}

// Notify sends a notification to trigger aggregation
func (p *AggregationWorkerPool) Notify(notif AggregationNotification) {
	select {
	case p.notifyCh <- notif:
	case <-p.stopCh:
	default:
		p.logger.Warn("Aggregation notification channel full",
			"database", notif.Database,
			"collection", notif.Collection,
			"level", notif.Level)
	}
}

// dispatchLoop routes notifications to partition workers
func (p *AggregationWorkerPool) dispatchLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return

		case notif := <-p.notifyCh:
			p.dispatchToWorker(notif)
		}
	}
}

// dispatchToWorker sends notification to the appropriate partition worker
func (p *AggregationWorkerPool) dispatchToWorker(notif AggregationNotification) {
	key := p.partitionKey(notif.Database, notif.Collection, notif.Level, notif.TimeKey)

	p.workersMu.Lock()
	worker, exists := p.workers[key]
	if !exists {
		// Create new worker for this partition
		worker = &PartitionWorker{
			key:        key,
			database:   notif.Database,
			collection: notif.Collection,
			level:      notif.Level,
			timeKey:    notif.TimeKey,
			notifyCh:   make(chan struct{}, p.config.QueueSize),
			stopCh:     make(chan struct{}),
			state:      workerIdle,
		}
		p.workers[key] = worker
	} else {
		// Update timeKey if notification has a more recent time
		// This ensures aggregation uses the correct time context
		worker.mu.Lock()
		if notif.TimeKey.After(worker.timeKey) {
			worker.timeKey = notif.TimeKey
		}
		worker.mu.Unlock()
	}
	p.workersMu.Unlock()

	worker.mu.Lock()
	worker.pendingCount++
	worker.lastNotify = time.Now()

	// Try to start worker if idle
	if worker.state == workerIdle {
		// Try to acquire semaphore (non-blocking)
		select {
		case p.semaphore <- struct{}{}:
			// Got semaphore, start worker immediately
			worker.state = workerRunning
			worker.stopCh = make(chan struct{}) // Reset stop channel
			p.wg.Add(1)
			go p.runWorker(worker)
		default:
			// Semaphore full, add to pending queue and try to preempt idle worker
			worker.state = workerPending
			select {
			case p.pendingQueue <- worker:
				p.logger.Debug("Worker added to pending queue", "key", key)
				// Try to preempt a worker that's waiting for job
				worker.mu.Unlock()
				p.preemptIdleWorker()
				return
			default:
				// Pending queue also full, reset state
				worker.state = workerIdle
				p.logger.Warn("Pending queue full", "key", key)
			}
		}
	}

	// Signal the worker if running or waiting for job
	// Check state while still holding the lock
	shouldNotify := worker.state == workerRunning || worker.state == workerWaitingForJob
	worker.mu.Unlock()

	if shouldNotify {
		select {
		case worker.notifyCh <- struct{}{}:
		default:
			// Channel full, worker will process pending anyway
		}
	}
}

// preemptIdleWorker finds the worker that has been waiting for job the longest and stops it
func (p *AggregationWorkerPool) preemptIdleWorker() {
	var oldestWorker *PartitionWorker
	var oldestIdleTime time.Time

	p.workersMu.RLock()
	for _, w := range p.workers {
		w.mu.Lock()
		if w.state == workerWaitingForJob && w.pendingCount == 0 {
			if oldestWorker == nil || w.idleSince.Before(oldestIdleTime) {
				oldestWorker = w
				oldestIdleTime = w.idleSince
			}
		}
		w.mu.Unlock()
	}
	p.workersMu.RUnlock()

	if oldestWorker != nil {
		oldestWorker.mu.Lock()
		// Double check state hasn't changed
		if oldestWorker.state == workerWaitingForJob && oldestWorker.pendingCount == 0 {
			p.logger.Debug("Preempting idle worker",
				"key", oldestWorker.key,
				"idle_duration", time.Since(oldestWorker.idleSince))
			close(oldestWorker.stopCh) // Signal worker to stop
		}
		oldestWorker.mu.Unlock()
	}
}

// pendingQueueLoop processes workers waiting for semaphore
func (p *AggregationWorkerPool) pendingQueueLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return

		case worker := <-p.pendingQueue:
			// Wait for semaphore
			select {
			case p.semaphore <- struct{}{}:
				worker.mu.Lock()
				if worker.state == workerPending && worker.pendingCount > 0 {
					worker.state = workerRunning
					worker.stopCh = make(chan struct{}) // Reset stop channel
					worker.mu.Unlock()
					p.wg.Add(1)
					go p.runWorker(worker)
				} else {
					// No longer needed, release semaphore
					worker.state = workerIdle
					worker.mu.Unlock()
					<-p.semaphore
				}
			case <-p.stopCh:
				return
			}
		}
	}
}

// runWorker is the dedicated goroutine for one partition
// Semaphore is already acquired before calling this function
func (p *AggregationWorkerPool) runWorker(worker *PartitionWorker) {
	defer p.wg.Done()
	defer func() { <-p.semaphore }() // Release semaphore on exit

	p.logger.Debug("Partition worker started", "key", worker.key)

	batchTimer := time.NewTimer(p.config.BatchDelay)
	defer batchTimer.Stop()

	idleTimer := time.NewTimer(p.config.WorkerIdleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-p.stopCh:
			p.logger.Debug("Partition worker stopped (pool stop)", "key", worker.key)
			worker.mu.Lock()
			worker.state = workerIdle
			worker.mu.Unlock()
			return

		case <-worker.stopCh:
			p.logger.Debug("Partition worker stopped (preempted)", "key", worker.key)
			worker.mu.Lock()
			worker.state = workerIdle
			worker.mu.Unlock()
			return

		case <-worker.notifyCh:
			// Got new work, switch to running state
			worker.mu.Lock()
			if worker.state == workerWaitingForJob {
				worker.state = workerRunning
			}
			worker.mu.Unlock()

			// Reset timers on new notification
			if !batchTimer.Stop() {
				select {
				case <-batchTimer.C:
				default:
				}
			}
			batchTimer.Reset(p.config.BatchDelay)

			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(p.config.WorkerIdleTimeout)

		case <-batchTimer.C:
			// Batch delay passed, process if has pending
			worker.mu.Lock()
			hasPending := worker.pendingCount > 0
			worker.mu.Unlock()

			if hasPending {
				// Set state to running while processing
				worker.mu.Lock()
				worker.state = workerRunning
				worker.mu.Unlock()

				p.processPartition(worker)

				// After processing, set to waiting for job
				worker.mu.Lock()
				worker.state = workerWaitingForJob
				worker.idleSince = time.Now()
				worker.mu.Unlock()
			}

			// Reset idle timer after processing
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(p.config.WorkerIdleTimeout)

		case <-idleTimer.C:
			// Idle timeout - check if we should exit
			worker.mu.Lock()
			if worker.pendingCount == 0 {
				worker.state = workerIdle
				worker.mu.Unlock()
				p.logger.Debug("Partition worker idle exit", "key", worker.key)
				return
			}
			worker.mu.Unlock()
			idleTimer.Reset(p.config.WorkerIdleTimeout)
		}
	}
}

// processPartition executes aggregation for a partition
func (p *AggregationWorkerPool) processPartition(worker *PartitionWorker) {
	// Get and reset pending count
	worker.mu.Lock()
	pendingCount := worker.pendingCount
	worker.pendingCount = 0
	worker.mu.Unlock()

	if pendingCount == 0 {
		return
	}

	p.logger.Debug("Processing partition",
		"key", worker.key,
		"level", worker.level,
		"pending_count", pendingCount)

	// Execute aggregation based on level
	var err error
	var nextLevel AggregationLevel
	var nextTimeKey time.Time

	switch worker.level {
	case AggregationHourly:
		err = p.aggregateHourly(worker)
		nextLevel = AggregationDaily
		nextTimeKey = TruncateToMonth(worker.timeKey.In(p.config.Timezone))

	case AggregationDaily:
		err = p.aggregateDaily(worker)
		nextLevel = AggregationMonthly
		nextTimeKey = TruncateToYear(worker.timeKey.In(p.config.Timezone))

	case AggregationMonthly:
		err = p.aggregateMonthly(worker)
		nextLevel = AggregationYearly
		nextTimeKey = TruncateToYear(worker.timeKey.In(p.config.Timezone)) // Use year start for yearly aggregation

	case AggregationYearly:
		err = p.aggregateYearly(worker)
		// No next level
	}

	// Update state
	worker.mu.Lock()
	worker.lastProcessed = time.Now()
	worker.mu.Unlock()

	// Update stats
	p.statsMu.Lock()
	if err != nil {
		p.totalFailed++
		p.statsMu.Unlock()
		p.logger.Error("Aggregation failed",
			"key", worker.key,
			"level", worker.level,
			"error", err)
		return
	}
	p.totalProcessed++
	p.statsMu.Unlock()

	// Notify next level if exists
	if nextLevel != "" && worker.level != AggregationYearly {
		if nextPool, ok := p.nextLevelPool[nextLevel]; ok {
			nextPool.Notify(AggregationNotification{
				Database:   worker.database,
				Collection: worker.collection,
				Level:      nextLevel,
				TimeKey:    nextTimeKey,
			})
		}
	}
}

// partitionKey generates the partition key
// Format: db:collection:level:date
func (p *AggregationWorkerPool) partitionKey(database, collection string, level AggregationLevel, timeKey time.Time) string {
	var dateStr string
	t := timeKey.In(p.config.Timezone)

	switch level {
	case AggregationHourly:
		dateStr = t.Format("20060102") // YYYYMMDD
	case AggregationDaily:
		dateStr = t.Format("200601") // YYYYMM
	case AggregationMonthly:
		dateStr = t.Format("2006") // YYYY
	case AggregationYearly:
		dateStr = t.Format("2006") // YYYY - use year-specific key to support multi-year data
	default:
		dateStr = t.Format("20060102")
	}

	return fmt.Sprintf("%s:%s:%s:%s", database, collection, level, dateStr)
}

// cleanupLoop removes stopped workers periodically
func (p *AggregationWorkerPool) cleanupLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return

		case <-ticker.C:
			p.cleanupStoppedWorkers()
		}
	}
}

// cleanupStoppedWorkers removes workers that are not running
func (p *AggregationWorkerPool) cleanupStoppedWorkers() {
	var toRemove []string

	p.workersMu.RLock()
	for key, worker := range p.workers {
		worker.mu.Lock()
		if worker.state == workerIdle && worker.pendingCount == 0 {
			toRemove = append(toRemove, key)
		}
		worker.mu.Unlock()
	}
	p.workersMu.RUnlock()

	if len(toRemove) > 0 {
		p.workersMu.Lock()
		for _, key := range toRemove {
			delete(p.workers, key)
		}
		p.workersMu.Unlock()

		p.logger.Debug("Cleaned up stopped workers", "count", len(toRemove))
	}
}

// =============================================================================
// Aggregation logic
// =============================================================================

// aggregateHourly aggregates raw data to hourly
func (p *AggregationWorkerPool) aggregateHourly(worker *PartitionWorker) error {
	// Read raw data for the day using Query interface
	dayStart := time.Date(
		worker.timeKey.Year(), worker.timeKey.Month(), worker.timeKey.Day(),
		0, 0, 0, 0, p.config.Timezone,
	)
	dayEnd := dayStart.Add(24 * time.Hour)

	dataPoints, err := p.rawReader.Query(worker.database, worker.collection, nil, dayStart, dayEnd, nil)
	if err != nil {
		return fmt.Errorf("failed to read raw data: %w", err)
	}

	if len(dataPoints) == 0 {
		return nil
	}

	// Group by device and hour
	type hourKey struct {
		deviceID string
		hour     time.Time
	}
	byDeviceHour := make(map[hourKey][]*RawDataPoint)
	for _, dp := range dataPoints {
		deviceID := dp.GetID()
		hourStart := dp.GetTime().Truncate(time.Hour)
		key := hourKey{deviceID: deviceID, hour: hourStart}
		byDeviceHour[key] = append(byDeviceHour[key], &RawDataPoint{
			Time:   dp.GetTime(),
			Fields: dp.GetFields(),
		})
	}

	// Aggregate each device-hour bucket
	var aggPoints []*AggregatedPoint
	for key, points := range byDeviceHour {
		aggPoint, err := AggregateRawDataPoints(key.deviceID, points, AggregationHourly)
		if err != nil {
			continue
		}
		aggPoint.Time = key.hour
		aggPoints = append(aggPoints, aggPoint)
	}

	// Write to storage
	if err := p.aggStorage.WriteHourly(worker.database, worker.collection, aggPoints); err != nil {
		return fmt.Errorf("failed to write hourly aggregates: %w", err)
	}

	p.logger.Info("Hourly aggregation complete",
		"database", worker.database,
		"collection", worker.collection,
		"date", dayStart.Format("2006-01-02"),
		"points", len(aggPoints))

	return nil
}

// aggregateDaily aggregates hourly data to daily
func (p *AggregationWorkerPool) aggregateDaily(worker *PartitionWorker) error {
	// Read hourly data for the month
	monthStart := TruncateToMonth(worker.timeKey.In(p.config.Timezone))
	monthEnd := monthStart.AddDate(0, 1, 0)

	// Iterate through each day and aggregate
	var allAggPoints []*AggregatedPoint
	for day := monthStart; day.Before(monthEnd); day = day.AddDate(0, 0, 1) {
		// Read hourly aggregates for this day
		hourlyPoints, err := p.aggStorage.ReadHourlyForDay(worker.database, worker.collection, day)
		if err != nil {
			p.logger.Warn("Failed to read hourly data for day",
				"database", worker.database,
				"collection", worker.collection,
				"date", day.Format("2006-01-02"),
				"error", err)
			continue
		}

		if len(hourlyPoints) == 0 {
			continue
		}

		// Group by device
		byDevice := make(map[string][]*AggregatedPoint)
		for _, point := range hourlyPoints {
			byDevice[point.DeviceID] = append(byDevice[point.DeviceID], point)
		}

		// Aggregate each device
		for deviceID, devicePoints := range byDevice {
			aggPoint, err := AggregateAggregatedPoints(deviceID, devicePoints, AggregationDaily)
			if err != nil {
				continue
			}
			aggPoint.Time = day
			allAggPoints = append(allAggPoints, aggPoint)
		}
	}

	if len(allAggPoints) == 0 {
		return nil
	}

	// Write to storage
	if err := p.aggStorage.WriteDaily(worker.database, worker.collection, allAggPoints); err != nil {
		return fmt.Errorf("failed to write daily aggregates: %w", err)
	}

	p.logger.Info("Daily aggregation complete",
		"database", worker.database,
		"collection", worker.collection,
		"month", monthStart.Format("2006-01"),
		"points", len(allAggPoints))

	return nil
}

// aggregateMonthly aggregates daily data to monthly
func (p *AggregationWorkerPool) aggregateMonthly(worker *PartitionWorker) error {
	// Read daily data for the year
	yearStart := TruncateToYear(worker.timeKey.In(p.config.Timezone))
	yearEnd := yearStart.AddDate(1, 0, 0)

	// Iterate through each month
	var allAggPoints []*AggregatedPoint
	for month := yearStart; month.Before(yearEnd); month = month.AddDate(0, 1, 0) {
		dailyPoints, err := p.aggStorage.ReadDailyForMonth(worker.database, worker.collection, month)
		if err != nil {
			p.logger.Warn("Failed to read daily data for month",
				"database", worker.database,
				"collection", worker.collection,
				"month", month.Format("2006-01"),
				"error", err)
			continue
		}

		if len(dailyPoints) == 0 {
			continue
		}

		// Group by device
		byDevice := make(map[string][]*AggregatedPoint)
		for _, point := range dailyPoints {
			byDevice[point.DeviceID] = append(byDevice[point.DeviceID], point)
		}

		// Aggregate each device
		for deviceID, devicePoints := range byDevice {
			aggPoint, err := AggregateAggregatedPoints(deviceID, devicePoints, AggregationMonthly)
			if err != nil {
				continue
			}
			aggPoint.Time = month
			allAggPoints = append(allAggPoints, aggPoint)
		}
	}

	if len(allAggPoints) == 0 {
		return nil
	}

	// Write to storage
	if err := p.aggStorage.WriteMonthly(worker.database, worker.collection, allAggPoints); err != nil {
		return fmt.Errorf("failed to write monthly aggregates: %w", err)
	}

	p.logger.Info("Monthly aggregation complete",
		"database", worker.database,
		"collection", worker.collection,
		"year", yearStart.Format("2006"),
		"points", len(allAggPoints))

	return nil
}

// aggregateYearly aggregates monthly data to yearly
func (p *AggregationWorkerPool) aggregateYearly(worker *PartitionWorker) error {
	// Read all monthly data for the year
	monthlyPoints, err := p.aggStorage.ReadMonthlyForYear(worker.database, worker.collection, worker.timeKey)
	if err != nil {
		return fmt.Errorf("failed to read monthly data: %w", err)
	}

	if len(monthlyPoints) == 0 {
		return nil
	}

	// Group by device
	byDevice := make(map[string][]*AggregatedPoint)
	for _, point := range monthlyPoints {
		byDevice[point.DeviceID] = append(byDevice[point.DeviceID], point)
	}

	// Aggregate each device
	var aggPoints []*AggregatedPoint
	for deviceID, devicePoints := range byDevice {
		aggPoint, err := AggregateAggregatedPoints(deviceID, devicePoints, AggregationYearly)
		if err != nil {
			continue
		}
		aggPoint.Time = TruncateToYear(worker.timeKey)
		aggPoints = append(aggPoints, aggPoint)
	}

	// Write to storage
	if err := p.aggStorage.WriteYearly(worker.database, worker.collection, aggPoints); err != nil {
		return fmt.Errorf("failed to write yearly aggregates: %w", err)
	}

	p.logger.Info("Yearly aggregation complete",
		"database", worker.database,
		"collection", worker.collection,
		"points", len(aggPoints))

	return nil
}

// =============================================================================
// Stats
// =============================================================================

// Stats returns pool statistics
func (p *AggregationWorkerPool) Stats() map[string]interface{} {
	p.workersMu.RLock()
	totalWorkers := len(p.workers)
	runningWorkers := 0
	pendingWorkers := 0
	waitingForJobWorkers := 0
	totalPending := 0

	for _, worker := range p.workers {
		worker.mu.Lock()
		switch worker.state {
		case workerRunning:
			runningWorkers++
		case workerPending:
			pendingWorkers++
		case workerWaitingForJob:
			waitingForJobWorkers++
		}
		totalPending += worker.pendingCount
		worker.mu.Unlock()
	}
	p.workersMu.RUnlock()

	p.statsMu.RLock()
	totalProcessed := p.totalProcessed
	totalFailed := p.totalFailed
	p.statsMu.RUnlock()

	return map[string]interface{}{
		"max_active_workers":      p.config.MaxActiveWorkers,
		"total_workers":           totalWorkers,
		"running_workers":         runningWorkers,
		"pending_workers":         pendingWorkers,
		"waiting_for_job_workers": waitingForJobWorkers,
		"total_pending":           totalPending,
		"total_processed":         totalProcessed,
		"total_failed":            totalFailed,
	}
}
