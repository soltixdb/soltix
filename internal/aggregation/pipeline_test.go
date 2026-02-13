package aggregation

import (
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

// mockRawDataReader is a mock implementation of RawDataReader for testing
type mockRawDataReader struct {
	queryFunc func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error)
}

func (m *mockRawDataReader) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
	if m.queryFunc != nil {
		return m.queryFunc(database, collection, deviceIDs, startTime, endTime, fields)
	}
	return nil, nil
}

// mockDataPoint is a mock implementation of DataPointInterface for testing
type mockDataPoint struct {
	id         string
	database   string
	collection string
	time       time.Time
	fields     map[string]interface{}
}

func (m *mockDataPoint) GetID() string                     { return m.id }
func (m *mockDataPoint) GetDatabase() string               { return m.database }
func (m *mockDataPoint) GetCollection() string             { return m.collection }
func (m *mockDataPoint) GetTime() time.Time                { return m.time }
func (m *mockDataPoint) GetFields() map[string]interface{} { return m.fields }

// mockAggregationStorage is a mock implementation of AggregationStorage for testing
type mockAggregationStorage struct {
	writeHourlyFunc        func(database, collection string, points []*AggregatedPoint) error
	writeDailyFunc         func(database, collection string, points []*AggregatedPoint) error
	writeMonthlyFunc       func(database, collection string, points []*AggregatedPoint) error
	writeYearlyFunc        func(database, collection string, points []*AggregatedPoint) error
	readHourlyFunc         func(database, collection, date string) ([]*AggregatedPoint, error)
	readHourlyForDayFunc   func(database, collection string, day time.Time) ([]*AggregatedPoint, error)
	readDailyForMonthFunc  func(database, collection string, month time.Time) ([]*AggregatedPoint, error)
	readMonthlyForYearFunc func(database, collection string, year time.Time) ([]*AggregatedPoint, error)
	readYearlyFunc         func(database, collection string) ([]*AggregatedPoint, error)
	timezone               *time.Location
}

func (m *mockAggregationStorage) WriteHourly(database, collection string, points []*AggregatedPoint) error {
	if m.writeHourlyFunc != nil {
		return m.writeHourlyFunc(database, collection, points)
	}
	return nil
}

func (m *mockAggregationStorage) WriteDaily(database, collection string, points []*AggregatedPoint) error {
	if m.writeDailyFunc != nil {
		return m.writeDailyFunc(database, collection, points)
	}
	return nil
}

func (m *mockAggregationStorage) WriteMonthly(database, collection string, points []*AggregatedPoint) error {
	if m.writeMonthlyFunc != nil {
		return m.writeMonthlyFunc(database, collection, points)
	}
	return nil
}

func (m *mockAggregationStorage) WriteYearly(database, collection string, points []*AggregatedPoint) error {
	if m.writeYearlyFunc != nil {
		return m.writeYearlyFunc(database, collection, points)
	}
	return nil
}

func (m *mockAggregationStorage) ReadHourly(database, collection, date string) ([]*AggregatedPoint, error) {
	if m.readHourlyFunc != nil {
		return m.readHourlyFunc(database, collection, date)
	}
	return nil, nil
}

func (m *mockAggregationStorage) ReadHourlyForDay(database, collection string, day time.Time) ([]*AggregatedPoint, error) {
	if m.readHourlyForDayFunc != nil {
		return m.readHourlyForDayFunc(database, collection, day)
	}
	return nil, nil
}

func (m *mockAggregationStorage) ReadDailyForMonth(database, collection string, month time.Time) ([]*AggregatedPoint, error) {
	if m.readDailyForMonthFunc != nil {
		return m.readDailyForMonthFunc(database, collection, month)
	}
	return nil, nil
}

func (m *mockAggregationStorage) ReadMonthlyForYear(database, collection string, year time.Time) ([]*AggregatedPoint, error) {
	if m.readMonthlyForYearFunc != nil {
		return m.readMonthlyForYearFunc(database, collection, year)
	}
	return nil, nil
}

func (m *mockAggregationStorage) ReadYearly(database, collection string) ([]*AggregatedPoint, error) {
	if m.readYearlyFunc != nil {
		return m.readYearlyFunc(database, collection)
	}
	return nil, nil
}

func (m *mockAggregationStorage) SetTimezone(tz *time.Location) {
	m.timezone = tz
}

func (m *mockAggregationStorage) GetTimezone() *time.Location {
	if m.timezone == nil {
		return time.UTC
	}
	return m.timezone
}

func TestDefaultPipelineConfig(t *testing.T) {
	config := DefaultPipelineConfig()

	// Test hourly config
	if config.HourlyConfig.MaxActiveWorkers != 100 {
		t.Errorf("HourlyConfig.MaxActiveWorkers = %d, expected 100", config.HourlyConfig.MaxActiveWorkers)
	}
	if config.HourlyConfig.WorkerIdleTimeout != 5*time.Minute {
		t.Errorf("HourlyConfig.WorkerIdleTimeout = %v, expected 5m", config.HourlyConfig.WorkerIdleTimeout)
	}
	if config.HourlyConfig.QueueSize != 100 {
		t.Errorf("HourlyConfig.QueueSize = %d, expected 100", config.HourlyConfig.QueueSize)
	}
	if config.HourlyConfig.BatchDelay != 2*time.Second {
		t.Errorf("HourlyConfig.BatchDelay = %v, expected 2s", config.HourlyConfig.BatchDelay)
	}

	// Test daily config
	if config.DailyConfig.MaxActiveWorkers != 50 {
		t.Errorf("DailyConfig.MaxActiveWorkers = %d, expected 50", config.DailyConfig.MaxActiveWorkers)
	}
	if config.DailyConfig.WorkerIdleTimeout != 10*time.Minute {
		t.Errorf("DailyConfig.WorkerIdleTimeout = %v, expected 10m", config.DailyConfig.WorkerIdleTimeout)
	}

	// Test monthly config
	if config.MonthlyConfig.MaxActiveWorkers != 20 {
		t.Errorf("MonthlyConfig.MaxActiveWorkers = %d, expected 20", config.MonthlyConfig.MaxActiveWorkers)
	}
	if config.MonthlyConfig.WorkerIdleTimeout != 15*time.Minute {
		t.Errorf("MonthlyConfig.WorkerIdleTimeout = %v, expected 15m", config.MonthlyConfig.WorkerIdleTimeout)
	}

	// Test yearly config
	if config.YearlyConfig.MaxActiveWorkers != 10 {
		t.Errorf("YearlyConfig.MaxActiveWorkers = %d, expected 10", config.YearlyConfig.MaxActiveWorkers)
	}
	if config.YearlyConfig.WorkerIdleTimeout != 30*time.Minute {
		t.Errorf("YearlyConfig.WorkerIdleTimeout = %v, expected 30m", config.YearlyConfig.WorkerIdleTimeout)
	}

	// Test timezone
	if config.Timezone != time.UTC {
		t.Errorf("Timezone = %v, expected UTC", config.Timezone)
	}
}

func TestNewPipeline(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultPipelineConfig()

	pipeline := NewPipeline(config, logger, rawReader, aggStorage)

	if pipeline == nil {
		t.Fatal("NewPipeline returned nil")
		return
	}

	// Verify pipeline components are initialized
	if pipeline.hourlyPool == nil {
		t.Error("hourlyPool is nil")
	}
	if pipeline.dailyPool == nil {
		t.Error("dailyPool is nil")
	}
	if pipeline.monthlyPool == nil {
		t.Error("monthlyPool is nil")
	}
	if pipeline.yearlyPool == nil {
		t.Error("yearlyPool is nil")
	}
	if pipeline.flushEventCh == nil {
		t.Error("flushEventCh is nil")
	}
	if pipeline.stopCh == nil {
		t.Error("stopCh is nil")
	}
}

func TestNewPipeline_NilTimezone(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}

	// Create config with nil timezone
	config := DefaultPipelineConfig()
	config.Timezone = nil

	pipeline := NewPipeline(config, logger, rawReader, aggStorage)

	// Should default to UTC
	if pipeline.config.Timezone != time.UTC {
		t.Errorf("Pipeline timezone = %v, expected UTC when nil provided", pipeline.config.Timezone)
	}
}

func TestPipeline_StartStop(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultPipelineConfig()

	pipeline := NewPipeline(config, logger, rawReader, aggStorage)

	// Start pipeline
	pipeline.Start()

	// Stop pipeline
	pipeline.Stop()

	// Should complete without hanging
}

func TestPipeline_OnFlushComplete(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultPipelineConfig()

	pipeline := NewPipeline(config, logger, rawReader, aggStorage)
	pipeline.Start()
	defer pipeline.Stop()

	// Send flush complete event
	event := FlushCompleteEvent{
		Database:    "testdb",
		Date:        time.Now(),
		Collections: []string{"col1", "col2"},
		PointCount:  100,
		FlushTime:   time.Now(),
	}

	// Should not block
	pipeline.OnFlushComplete(event)
}

func TestPipeline_TriggerAggregation(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultPipelineConfig()

	pipeline := NewPipeline(config, logger, rawReader, aggStorage)
	pipeline.Start()
	defer pipeline.Stop()

	now := time.Now()

	// Test triggering at each level
	pipeline.TriggerAggregation("testdb", "col1", AggregationHourly, now)
	pipeline.TriggerAggregation("testdb", "col1", AggregationDaily, now)
	pipeline.TriggerAggregation("testdb", "col1", AggregationMonthly, now)
	pipeline.TriggerAggregation("testdb", "col1", AggregationYearly, now)

	// Should not block or panic
}

func TestPipeline_Stats(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}
	config := DefaultPipelineConfig()

	pipeline := NewPipeline(config, logger, rawReader, aggStorage)
	pipeline.Start()
	defer pipeline.Stop()

	stats := pipeline.Stats()

	// Verify stats structure
	if _, ok := stats["hourly"]; !ok {
		t.Error("Stats missing 'hourly' key")
	}
	if _, ok := stats["daily"]; !ok {
		t.Error("Stats missing 'daily' key")
	}
	if _, ok := stats["monthly"]; !ok {
		t.Error("Stats missing 'monthly' key")
	}
	if _, ok := stats["yearly"]; !ok {
		t.Error("Stats missing 'yearly' key")
	}
}

func TestPipeline_GetTimezone(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}

	// Test with UTC
	config := DefaultPipelineConfig()
	config.Timezone = time.UTC
	pipeline := NewPipeline(config, logger, rawReader, aggStorage)

	if pipeline.GetTimezone() != time.UTC {
		t.Errorf("GetTimezone() = %v, expected UTC", pipeline.GetTimezone())
	}

	// Test with different timezone
	loc, _ := time.LoadLocation("Asia/Tokyo")
	config.Timezone = loc
	pipeline2 := NewPipeline(config, logger, rawReader, aggStorage)

	if pipeline2.GetTimezone() != loc {
		t.Errorf("GetTimezone() = %v, expected Asia/Tokyo", pipeline2.GetTimezone())
	}
}

func TestPipeline_FlushEventChannelFull(t *testing.T) {
	logger := logging.NewDevelopment()
	rawReader := &mockRawDataReader{}
	aggStorage := &mockAggregationStorage{}

	config := DefaultPipelineConfig()
	config.FlushEventQueueSize = 1 // Very small queue

	pipeline := NewPipeline(config, logger, rawReader, aggStorage)
	// Don't start pipeline so events aren't consumed

	event := FlushCompleteEvent{
		Database:    "testdb",
		Date:        time.Now(),
		Collections: []string{"col1"},
		PointCount:  100,
		FlushTime:   time.Now(),
	}

	// Fill the queue
	pipeline.OnFlushComplete(event)

	// This should not block, just warn
	pipeline.OnFlushComplete(event)
}
