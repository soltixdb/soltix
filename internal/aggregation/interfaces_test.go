package aggregation

import (
	"testing"
	"time"
)

func TestDataPointInterface(t *testing.T) {
	// Test that mockDataPoint implements DataPointInterface
	var _ DataPointInterface = &mockDataPoint{}

	now := time.Now()
	dp := &mockDataPoint{
		id:         "device123",
		database:   "testdb",
		collection: "metrics",
		time:       now,
		fields: map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60.0,
		},
	}

	if dp.GetID() != "device123" {
		t.Errorf("GetID() = %q, expected %q", dp.GetID(), "device123")
	}
	if dp.GetDatabase() != "testdb" {
		t.Errorf("GetDatabase() = %q, expected %q", dp.GetDatabase(), "testdb")
	}
	if dp.GetCollection() != "metrics" {
		t.Errorf("GetCollection() = %q, expected %q", dp.GetCollection(), "metrics")
	}
	if !dp.GetTime().Equal(now) {
		t.Errorf("GetTime() = %v, expected %v", dp.GetTime(), now)
	}
	if len(dp.GetFields()) != 2 {
		t.Errorf("GetFields() length = %d, expected 2", len(dp.GetFields()))
	}
}

func TestRawDataReader(t *testing.T) {
	// Test that mockRawDataReader implements RawDataReader
	var _ RawDataReader = &mockRawDataReader{}

	now := time.Now()
	queryCalled := false

	reader := &mockRawDataReader{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]DataPointInterface, error) {
			queryCalled = true
			if database != "testdb" {
				t.Errorf("database = %q, expected %q", database, "testdb")
			}
			if collection != "metrics" {
				t.Errorf("collection = %q, expected %q", collection, "metrics")
			}
			return []DataPointInterface{
				&mockDataPoint{
					id:         "device1",
					database:   database,
					collection: collection,
					time:       now,
					fields:     map[string]interface{}{"temp": 25.0},
				},
			}, nil
		},
	}

	result, err := reader.Query("testdb", "metrics", nil, now, now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if !queryCalled {
		t.Error("Query function was not called")
	}

	if len(result) != 1 {
		t.Errorf("Query returned %d results, expected 1", len(result))
	}
}

func TestRawDataReader_NilQueryFunc(t *testing.T) {
	reader := &mockRawDataReader{}

	// Should return nil, nil when queryFunc is nil
	result, err := reader.Query("testdb", "metrics", nil, time.Now(), time.Now().Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query with nil func should not error: %v", err)
	}
	if result != nil {
		t.Errorf("Query with nil func should return nil, got %v", result)
	}
}

func TestAggregationStorage(t *testing.T) {
	// Test that mockAggregationStorage implements AggregationStorage
	var _ AggregationStorage = &mockAggregationStorage{}

	storage := &mockAggregationStorage{}

	// Test SetTimezone/GetTimezone
	if storage.GetTimezone() != time.UTC {
		t.Errorf("Default timezone = %v, expected UTC", storage.GetTimezone())
	}

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	storage.SetTimezone(tokyo)
	if storage.GetTimezone() != tokyo {
		t.Errorf("Timezone after SetTimezone = %v, expected Asia/Tokyo", storage.GetTimezone())
	}
}

func TestAggregationStorage_WriteHourly(t *testing.T) {
	writeCalled := false
	storage := &mockAggregationStorage{
		writeHourlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeCalled = true
			if database != "testdb" {
				t.Errorf("database = %q, expected %q", database, "testdb")
			}
			if collection != "metrics" {
				t.Errorf("collection = %q, expected %q", collection, "metrics")
			}
			if len(points) != 1 {
				t.Errorf("points length = %d, expected 1", len(points))
			}
			return nil
		},
	}

	points := []*AggregatedPoint{
		{
			Time:     time.Now(),
			DeviceID: "device1",
			Fields:   map[string]*AggregatedField{},
		},
	}

	err := storage.WriteHourly("testdb", "metrics", points)
	if err != nil {
		t.Fatalf("WriteHourly failed: %v", err)
	}

	if !writeCalled {
		t.Error("WriteHourly was not called")
	}
}

func TestAggregationStorage_WriteDaily(t *testing.T) {
	writeCalled := false
	storage := &mockAggregationStorage{
		writeDailyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeCalled = true
			return nil
		},
	}

	err := storage.WriteDaily("testdb", "metrics", []*AggregatedPoint{})
	if err != nil {
		t.Fatalf("WriteDaily failed: %v", err)
	}

	if !writeCalled {
		t.Error("WriteDaily was not called")
	}
}

func TestAggregationStorage_WriteMonthly(t *testing.T) {
	writeCalled := false
	storage := &mockAggregationStorage{
		writeMonthlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeCalled = true
			return nil
		},
	}

	err := storage.WriteMonthly("testdb", "metrics", []*AggregatedPoint{})
	if err != nil {
		t.Fatalf("WriteMonthly failed: %v", err)
	}

	if !writeCalled {
		t.Error("WriteMonthly was not called")
	}
}

func TestAggregationStorage_WriteYearly(t *testing.T) {
	writeCalled := false
	storage := &mockAggregationStorage{
		writeYearlyFunc: func(database, collection string, points []*AggregatedPoint) error {
			writeCalled = true
			return nil
		},
	}

	err := storage.WriteYearly("testdb", "metrics", []*AggregatedPoint{})
	if err != nil {
		t.Fatalf("WriteYearly failed: %v", err)
	}

	if !writeCalled {
		t.Error("WriteYearly was not called")
	}
}

func TestAggregationStorage_ReadHourly(t *testing.T) {
	now := time.Now()
	storage := &mockAggregationStorage{
		readHourlyFunc: func(database, collection, date string) ([]*AggregatedPoint, error) {
			if date != "20240515" {
				t.Errorf("date = %q, expected %q", date, "20240515")
			}
			return []*AggregatedPoint{
				{Time: now, DeviceID: "device1"},
			}, nil
		},
	}

	result, err := storage.ReadHourly("testdb", "metrics", "20240515")
	if err != nil {
		t.Fatalf("ReadHourly failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("ReadHourly returned %d results, expected 1", len(result))
	}
}

func TestAggregationStorage_ReadHourlyForDay(t *testing.T) {
	day := time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC)
	storage := &mockAggregationStorage{
		readHourlyForDayFunc: func(database, collection string, d time.Time) ([]*AggregatedPoint, error) {
			if !d.Equal(day) {
				t.Errorf("day = %v, expected %v", d, day)
			}
			return []*AggregatedPoint{
				{Time: day, DeviceID: "device1"},
				{Time: day.Add(time.Hour), DeviceID: "device1"},
			}, nil
		},
	}

	result, err := storage.ReadHourlyForDay("testdb", "metrics", day)
	if err != nil {
		t.Fatalf("ReadHourlyForDay failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("ReadHourlyForDay returned %d results, expected 2", len(result))
	}
}

func TestAggregationStorage_ReadDailyForMonth(t *testing.T) {
	month := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	storage := &mockAggregationStorage{
		readDailyForMonthFunc: func(database, collection string, m time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{Time: month, DeviceID: "device1"},
			}, nil
		},
	}

	result, err := storage.ReadDailyForMonth("testdb", "metrics", month)
	if err != nil {
		t.Fatalf("ReadDailyForMonth failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("ReadDailyForMonth returned %d results, expected 1", len(result))
	}
}

func TestAggregationStorage_ReadMonthlyForYear(t *testing.T) {
	year := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	storage := &mockAggregationStorage{
		readMonthlyForYearFunc: func(database, collection string, y time.Time) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{Time: year, DeviceID: "device1"},
			}, nil
		},
	}

	result, err := storage.ReadMonthlyForYear("testdb", "metrics", year)
	if err != nil {
		t.Fatalf("ReadMonthlyForYear failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("ReadMonthlyForYear returned %d results, expected 1", len(result))
	}
}

func TestAggregationStorage_ReadYearly(t *testing.T) {
	storage := &mockAggregationStorage{
		readYearlyFunc: func(database, collection string) ([]*AggregatedPoint, error) {
			return []*AggregatedPoint{
				{Time: time.Now(), DeviceID: "device1"},
			}, nil
		},
	}

	result, err := storage.ReadYearly("testdb", "metrics")
	if err != nil {
		t.Fatalf("ReadYearly failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("ReadYearly returned %d results, expected 1", len(result))
	}
}

func TestAggregationStorage_NilFuncs(t *testing.T) {
	storage := &mockAggregationStorage{}

	// All methods should return nil, nil when funcs are not set
	if err := storage.WriteHourly("", "", nil); err != nil {
		t.Errorf("WriteHourly should return nil error")
	}
	if err := storage.WriteDaily("", "", nil); err != nil {
		t.Errorf("WriteDaily should return nil error")
	}
	if err := storage.WriteMonthly("", "", nil); err != nil {
		t.Errorf("WriteMonthly should return nil error")
	}
	if err := storage.WriteYearly("", "", nil); err != nil {
		t.Errorf("WriteYearly should return nil error")
	}

	if result, _ := storage.ReadHourly("", "", ""); result != nil {
		t.Errorf("ReadHourly should return nil result")
	}
	if result, _ := storage.ReadHourlyForDay("", "", time.Now()); result != nil {
		t.Errorf("ReadHourlyForDay should return nil result")
	}
	if result, _ := storage.ReadDailyForMonth("", "", time.Now()); result != nil {
		t.Errorf("ReadDailyForMonth should return nil result")
	}
	if result, _ := storage.ReadMonthlyForYear("", "", time.Now()); result != nil {
		t.Errorf("ReadMonthlyForYear should return nil result")
	}
	if result, _ := storage.ReadYearly("", ""); result != nil {
		t.Errorf("ReadYearly should return nil result")
	}
}
