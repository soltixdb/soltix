package storage

import (
	"testing"
	"time"
)

// mockMainStorage is a mock implementation of MainStorage for testing
type mockMainStorage struct {
	writeBatchFunc func(entries []*DataPoint) error
	queryFunc      func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error)
	timezone       *time.Location
}

func (m *mockMainStorage) WriteBatch(entries []*DataPoint) error {
	if m.writeBatchFunc != nil {
		return m.writeBatchFunc(entries)
	}
	return nil
}

func (m *mockMainStorage) Query(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
	if m.queryFunc != nil {
		return m.queryFunc(database, collection, deviceIDs, startTime, endTime, fields)
	}
	return nil, nil
}

func (m *mockMainStorage) SetTimezone(tz *time.Location) {
	m.timezone = tz
}

func TestNewRawDataReaderAdapter(t *testing.T) {
	storage := &mockMainStorage{}
	adapter := NewRawDataReaderAdapter(storage)

	if adapter == nil {
		t.Fatal("NewRawDataReaderAdapter returned nil")
		return
	}
	if adapter.storage == nil {
		t.Error("adapter.storage is nil")
	}
}

func TestRawDataReaderAdapter_Query(t *testing.T) {
	now := time.Now()
	storage := &mockMainStorage{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
			if database != "testdb" {
				t.Errorf("database = %q, expected %q", database, "testdb")
			}
			if collection != "metrics" {
				t.Errorf("collection = %q, expected %q", collection, "metrics")
			}
			return []*DataPoint{
				{
					ID:         "device1",
					Database:   database,
					Collection: collection,
					Time:       now,
					Fields:     map[string]interface{}{"temp": 25.5},
				},
				{
					ID:         "device2",
					Database:   database,
					Collection: collection,
					Time:       now,
					Fields:     map[string]interface{}{"temp": 26.0},
				},
			}, nil
		},
	}

	adapter := NewRawDataReaderAdapter(storage)
	result, err := adapter.Query("testdb", "metrics", nil, now, now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Query returned %d results, expected 2", len(result))
	}

	// Verify the results have expected data (result is already []aggregation.DataPointInterface)
	for _, dp := range result {
		if dp.GetID() == "" {
			t.Error("DataPoint has empty ID")
		}
	}
}

func TestRawDataReaderAdapter_QueryEmpty(t *testing.T) {
	storage := &mockMainStorage{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
			return []*DataPoint{}, nil
		},
	}

	adapter := NewRawDataReaderAdapter(storage)
	result, err := adapter.Query("testdb", "metrics", nil, time.Now(), time.Now().Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Query returned %d results, expected 0", len(result))
	}
}

func TestRawDataReaderAdapter_QueryWithDeviceFilter(t *testing.T) {
	now := time.Now()
	storage := &mockMainStorage{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
			if len(deviceIDs) != 2 {
				t.Errorf("deviceIDs length = %d, expected 2", len(deviceIDs))
			}
			return []*DataPoint{
				{
					ID:         deviceIDs[0],
					Database:   database,
					Collection: collection,
					Time:       now,
					Fields:     map[string]interface{}{"temp": 25.5},
				},
			}, nil
		},
	}

	adapter := NewRawDataReaderAdapter(storage)
	result, err := adapter.Query("testdb", "metrics", []string{"dev1", "dev2"}, now, now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Query returned %d results, expected 1", len(result))
	}
}

func TestRawDataReaderAdapter_QueryWithFieldFilter(t *testing.T) {
	now := time.Now()
	storage := &mockMainStorage{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
			if len(fields) != 1 {
				t.Errorf("fields length = %d, expected 1", len(fields))
			}
			if fields[0] != "temperature" {
				t.Errorf("fields[0] = %q, expected %q", fields[0], "temperature")
			}
			return []*DataPoint{
				{
					ID:         "device1",
					Database:   database,
					Collection: collection,
					Time:       now,
					Fields:     map[string]interface{}{"temperature": 25.5},
				},
			}, nil
		},
	}

	adapter := NewRawDataReaderAdapter(storage)
	result, err := adapter.Query("testdb", "metrics", nil, now, now.Add(time.Hour), []string{"temperature"})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Query returned %d results, expected 1", len(result))
	}
}

func TestRawDataReaderAdapter_QueryError(t *testing.T) {
	storage := &mockMainStorage{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
			return nil, &testError{msg: "query failed"}
		},
	}

	adapter := NewRawDataReaderAdapter(storage)
	result, err := adapter.Query("testdb", "metrics", nil, time.Now(), time.Now().Add(time.Hour), nil)

	if err == nil {
		t.Error("Expected error, got nil")
	}
	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}
}

// testError is a simple error type for testing
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
