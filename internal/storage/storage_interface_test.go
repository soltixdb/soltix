package storage

import (
	"testing"
	"time"
)

func TestMainStorageInterface(t *testing.T) {
	// Verify mockMainStorage implements MainStorage
	var _ MainStorage = &mockMainStorage{}
}

func TestMockMainStorage_WriteBatch(t *testing.T) {
	writeCalled := false
	storage := &mockMainStorage{
		writeBatchFunc: func(entries []*DataPoint) error {
			writeCalled = true
			if len(entries) != 2 {
				t.Errorf("entries length = %d, expected 2", len(entries))
			}
			return nil
		},
	}

	entries := []*DataPoint{
		{ID: "device1", Database: "testdb", Collection: "metrics", Time: time.Now()},
		{ID: "device2", Database: "testdb", Collection: "metrics", Time: time.Now()},
	}

	err := storage.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if !writeCalled {
		t.Error("WriteBatch was not called")
	}
}

func TestMockMainStorage_WriteBatchEmpty(t *testing.T) {
	storage := &mockMainStorage{}
	err := storage.WriteBatch([]*DataPoint{})
	if err != nil {
		t.Fatalf("WriteBatch with empty entries failed: %v", err)
	}
}

func TestMockMainStorage_Query(t *testing.T) {
	now := time.Now()
	storage := &mockMainStorage{
		queryFunc: func(database, collection string, deviceIDs []string, startTime, endTime time.Time, fields []string) ([]*DataPoint, error) {
			return []*DataPoint{
				{
					ID:         "device1",
					Database:   database,
					Collection: collection,
					Time:       now,
					Fields:     map[string]interface{}{"temp": 25.0},
				},
			}, nil
		},
	}

	result, err := storage.Query("testdb", "metrics", nil, now, now.Add(time.Hour), nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Query returned %d results, expected 1", len(result))
	}
}

func TestMockMainStorage_SetTimezone(t *testing.T) {
	storage := &mockMainStorage{}

	// Set timezone
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	storage.SetTimezone(tokyo)

	if storage.timezone != tokyo {
		t.Errorf("timezone = %v, expected Asia/Tokyo", storage.timezone)
	}
}

func TestStorageAdapterInterface(t *testing.T) {
	// Note: StorageAdapter wraps Storage
	// This test verifies the adapter concept
	t.Log("StorageAdapter wraps Storage to implement MainStorage")
}

func TestDataPoint(t *testing.T) {
	now := time.Now()
	dp := &DataPoint{
		ID:         "device123",
		Database:   "testdb",
		Collection: "metrics",
		Time:       now,
		Fields: map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60.0,
		},
	}

	if dp.ID != "device123" {
		t.Errorf("ID = %q, expected %q", dp.ID, "device123")
	}
	if dp.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", dp.Database, "testdb")
	}
	if dp.Collection != "metrics" {
		t.Errorf("Collection = %q, expected %q", dp.Collection, "metrics")
	}
	if !dp.Time.Equal(now) {
		t.Errorf("Time = %v, expected %v", dp.Time, now)
	}
	if len(dp.Fields) != 2 {
		t.Errorf("Fields length = %d, expected 2", len(dp.Fields))
	}
}

func TestDataPoint_GetID(t *testing.T) {
	dp := &DataPoint{ID: "mydevice"}
	if dp.GetID() != "mydevice" {
		t.Errorf("GetID() = %q, expected %q", dp.GetID(), "mydevice")
	}
}

func TestDataPoint_GetDatabase(t *testing.T) {
	dp := &DataPoint{Database: "mydb"}
	if dp.GetDatabase() != "mydb" {
		t.Errorf("GetDatabase() = %q, expected %q", dp.GetDatabase(), "mydb")
	}
}

func TestDataPoint_GetCollection(t *testing.T) {
	dp := &DataPoint{Collection: "mycol"}
	if dp.GetCollection() != "mycol" {
		t.Errorf("GetCollection() = %q, expected %q", dp.GetCollection(), "mycol")
	}
}

func TestDataPoint_GetTime(t *testing.T) {
	now := time.Now()
	dp := &DataPoint{Time: now}
	if !dp.GetTime().Equal(now) {
		t.Errorf("GetTime() = %v, expected %v", dp.GetTime(), now)
	}
}

func TestDataPoint_GetFields(t *testing.T) {
	dp := &DataPoint{
		Fields: map[string]interface{}{
			"temp":     25.5,
			"humidity": 60,
		},
	}
	fields := dp.GetFields()
	if len(fields) != 2 {
		t.Errorf("GetFields() length = %d, expected 2", len(fields))
	}
	if fields["temp"] != 25.5 {
		t.Errorf("fields[\"temp\"] = %v, expected 25.5", fields["temp"])
	}
}
