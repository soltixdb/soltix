package aggregation

import (
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

// =============================================================================
// Pipeline - manages cascade of aggregation worker pools
// Hourly -> Daily -> Monthly -> Yearly
// =============================================================================

// PipelineConfig contains configuration for aggregation pipeline
type PipelineConfig struct {
	// WorkerPool config for each level
	HourlyConfig  WorkerPoolConfig
	DailyConfig   WorkerPoolConfig
	MonthlyConfig WorkerPoolConfig
	YearlyConfig  WorkerPoolConfig

	// QueueSize for flush event input
	FlushEventQueueSize int

	// Timezone for aggregation
	Timezone *time.Location
}

// DefaultPipelineConfig returns default configuration
func DefaultPipelineConfig() PipelineConfig {
	baseConfig := DefaultWorkerPoolConfig()

	return PipelineConfig{
		HourlyConfig: WorkerPoolConfig{
			MaxActiveWorkers:  100, // More partitions for hourly (most frequent)
			WorkerIdleTimeout: 5 * time.Minute,
			QueueSize:         100,
			BatchDelay:        2 * time.Second,
			Timezone:          time.UTC,
		},
		DailyConfig: WorkerPoolConfig{
			MaxActiveWorkers:  50,
			WorkerIdleTimeout: 10 * time.Minute,
			QueueSize:         100,
			BatchDelay:        2 * time.Second,
			Timezone:          time.UTC,
		},
		MonthlyConfig: WorkerPoolConfig{
			MaxActiveWorkers:  20,
			WorkerIdleTimeout: 15 * time.Minute,
			QueueSize:         50,
			BatchDelay:        5 * time.Second,
			Timezone:          time.UTC,
		},
		YearlyConfig: WorkerPoolConfig{
			MaxActiveWorkers:  10,
			WorkerIdleTimeout: 30 * time.Minute,
			QueueSize:         20,
			BatchDelay:        10 * time.Second,
			Timezone:          time.UTC,
		},
		FlushEventQueueSize: baseConfig.QueueSize,
		Timezone:            time.UTC,
	}
}

// Pipeline manages the cascade of aggregation pools
type Pipeline struct {
	config     PipelineConfig
	logger     *logging.Logger
	rawReader  RawDataReader
	aggStorage AggregationStorage

	// Worker pools for each level
	hourlyPool  *AggregationWorkerPool
	dailyPool   *AggregationWorkerPool
	monthlyPool *AggregationWorkerPool
	yearlyPool  *AggregationWorkerPool

	// Input channel for flush events
	flushEventCh chan FlushCompleteEvent

	stopCh chan struct{}
}

// NewPipeline creates a new cascade aggregation pipeline
func NewPipeline(
	config PipelineConfig,
	logger *logging.Logger,
	rawReader RawDataReader,
	aggStorage AggregationStorage,
) *Pipeline {
	// Ensure timezone is set
	if config.Timezone == nil {
		config.Timezone = time.UTC
	}

	// Set timezone for all pool configs
	config.HourlyConfig.Timezone = config.Timezone
	config.DailyConfig.Timezone = config.Timezone
	config.MonthlyConfig.Timezone = config.Timezone
	config.YearlyConfig.Timezone = config.Timezone

	p := &Pipeline{
		config:       config,
		logger:       logger,
		rawReader:    rawReader,
		aggStorage:   aggStorage,
		flushEventCh: make(chan FlushCompleteEvent, config.FlushEventQueueSize),
		stopCh:       make(chan struct{}),
	}

	// Create pools
	p.yearlyPool = NewAggregationWorkerPool(config.YearlyConfig, logger, rawReader, aggStorage)
	p.monthlyPool = NewAggregationWorkerPool(config.MonthlyConfig, logger, rawReader, aggStorage)
	p.dailyPool = NewAggregationWorkerPool(config.DailyConfig, logger, rawReader, aggStorage)
	p.hourlyPool = NewAggregationWorkerPool(config.HourlyConfig, logger, rawReader, aggStorage)

	// Set up cascade: Hourly -> Daily -> Monthly -> Yearly
	p.hourlyPool.SetNextLevelPool(AggregationDaily, p.dailyPool)
	p.dailyPool.SetNextLevelPool(AggregationMonthly, p.monthlyPool)
	p.monthlyPool.SetNextLevelPool(AggregationYearly, p.yearlyPool)

	return p
}

// Start starts the aggregation pipeline
func (p *Pipeline) Start() {
	// Start all pools
	p.hourlyPool.Start()
	p.dailyPool.Start()
	p.monthlyPool.Start()
	p.yearlyPool.Start()

	// Start flush event dispatcher
	go p.dispatchFlushEvents()

	p.logger.Info("Aggregation pipeline started")
}

// Stop stops the aggregation pipeline
func (p *Pipeline) Stop() {
	close(p.stopCh)

	// Stop pools in reverse order
	p.yearlyPool.Stop()
	p.monthlyPool.Stop()
	p.dailyPool.Stop()
	p.hourlyPool.Stop()

	p.logger.Info("Aggregation pipeline stopped")
}

// OnFlushComplete handles flush complete events from storage
// This is the entry point to trigger hourly aggregation
func (p *Pipeline) OnFlushComplete(event FlushCompleteEvent) {
	select {
	case p.flushEventCh <- event:
	default:
		p.logger.Warn("Flush event channel full",
			"database", event.Database,
			"collections", event.Collections,
			"date", event.Date.Format("2006-01-02"))
	}
}

// dispatchFlushEvents converts flush events to hourly aggregation notifications
func (p *Pipeline) dispatchFlushEvents() {
	for {
		select {
		case <-p.stopCh:
			return

		case event := <-p.flushEventCh:
			// Notify hourly pool for each collection
			for _, collection := range event.Collections {
				p.hourlyPool.Notify(AggregationNotification{
					Database:   event.Database,
					Collection: collection,
					Level:      AggregationHourly,
					TimeKey:    event.Date.In(p.config.Timezone),
				})
			}
		}
	}
}

// TriggerAggregation manually triggers aggregation for a specific partition
func (p *Pipeline) TriggerAggregation(database, collection string, level AggregationLevel, timeKey time.Time) {
	notif := AggregationNotification{
		Database:   database,
		Collection: collection,
		Level:      level,
		TimeKey:    timeKey.In(p.config.Timezone),
	}

	switch level {
	case AggregationHourly:
		p.hourlyPool.Notify(notif)
	case AggregationDaily:
		p.dailyPool.Notify(notif)
	case AggregationMonthly:
		p.monthlyPool.Notify(notif)
	case AggregationYearly:
		p.yearlyPool.Notify(notif)
	}
}

// Stats returns statistics for all pools
func (p *Pipeline) Stats() map[string]interface{} {
	return map[string]interface{}{
		"hourly":  p.hourlyPool.Stats(),
		"daily":   p.dailyPool.Stats(),
		"monthly": p.monthlyPool.Stats(),
		"yearly":  p.yearlyPool.Stats(),
	}
}

// GetTimezone returns the configured timezone
func (p *Pipeline) GetTimezone() *time.Location {
	return p.config.Timezone
}
