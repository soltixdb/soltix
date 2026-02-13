package processing

import (
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/logging"
)

func TestConvertTimezone(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Test times in UTC
	results := []QueryResult{
		{
			DeviceID: "device1",
			Times: []string{
				"2024-03-15T00:00:00Z",
				"2024-03-15T06:00:00Z",
				"2024-03-15T12:00:00Z",
			},
			Fields: map[string][]interface{}{
				"temp": {20.0, 22.0, 25.0},
			},
		},
	}

	// Convert to Asia/Tokyo (UTC+9)
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	converted := p.ConvertTimezone(results, tokyo)

	if len(converted) != 1 {
		t.Errorf("Expected 1 result, got %d", len(converted))
	}

	// Check that times are converted correctly
	expected := "2024-03-15T09:00:00+09:00"
	if converted[0].Times[0] != expected {
		t.Errorf("Expected %s, got %s", expected, converted[0].Times[0])
	}

	expected = "2024-03-15T15:00:00+09:00"
	if converted[0].Times[1] != expected {
		t.Errorf("Expected %s, got %s", expected, converted[0].Times[1])
	}

	expected = "2024-03-15T21:00:00+09:00"
	if converted[0].Times[2] != expected {
		t.Errorf("Expected %s, got %s", expected, converted[0].Times[2])
	}

	// Verify fields are preserved
	if len(converted[0].Fields["temp"]) != 3 {
		t.Error("Expected fields to be preserved")
	}
}

func TestConvertTimezone_NilLocation(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-03-15T00:00:00Z"},
		},
	}

	// nil location should return unchanged results
	converted := p.ConvertTimezone(results, nil)

	if converted[0].Times[0] != "2024-03-15T00:00:00Z" {
		t.Errorf("Expected unchanged time with nil location")
	}
}

func TestConvertTimezone_EmptyResults(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	results := []QueryResult{}

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	converted := p.ConvertTimezone(results, tokyo)

	if len(converted) != 0 {
		t.Errorf("Expected 0 results, got %d", len(converted))
	}
}

func TestConvertTimezone_SameTimezone(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Times already in Tokyo timezone
	results := []QueryResult{
		{
			DeviceID: "device1",
			Times: []string{
				"2024-03-15T09:00:00+09:00",
			},
		},
	}

	// Convert to same timezone - should skip conversion
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	converted := p.ConvertTimezone(results, tokyo)

	// Should return same results
	if converted[0].Times[0] != "2024-03-15T09:00:00+09:00" {
		t.Errorf("Expected unchanged time when already in target timezone")
	}
}

func TestConvertTimezone_InvalidTimeFormat(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Include an invalid time format
	results := []QueryResult{
		{
			DeviceID: "device1",
			Times: []string{
				"2024-03-15T00:00:00Z",
				"invalid-time-format",
				"2024-03-15T12:00:00Z",
			},
		},
	}

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	converted := p.ConvertTimezone(results, tokyo)

	// Valid times should be converted
	if converted[0].Times[0] != "2024-03-15T09:00:00+09:00" {
		t.Errorf("Expected first time to be converted")
	}

	// Invalid time should be preserved as-is
	if converted[0].Times[1] != "invalid-time-format" {
		t.Errorf("Expected invalid time to be preserved, got %s", converted[0].Times[1])
	}

	// Third time should be converted
	if converted[0].Times[2] != "2024-03-15T21:00:00+09:00" {
		t.Errorf("Expected third time to be converted")
	}
}

func TestConvertTimezone_MultipleDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-03-15T00:00:00Z"},
		},
		{
			DeviceID: "device2",
			Times:    []string{"2024-03-15T06:00:00Z"},
		},
	}

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	converted := p.ConvertTimezone(results, tokyo)

	if len(converted) != 2 {
		t.Errorf("Expected 2 devices, got %d", len(converted))
	}

	if converted[0].Times[0] != "2024-03-15T09:00:00+09:00" {
		t.Errorf("Device1 time not converted correctly")
	}

	if converted[1].Times[0] != "2024-03-15T15:00:00+09:00" {
		t.Errorf("Device2 time not converted correctly")
	}
}

func TestConvertAnomalyTimezone(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	anomalies := []AnomalyResult{
		{
			Time:     "2024-03-15T00:00:00Z",
			DeviceID: "device1",
			Field:    "temp",
			Value:    100.0,
			Score:    5.0,
		},
		{
			Time:     "2024-03-15T06:00:00Z",
			DeviceID: "device1",
			Field:    "temp",
			Value:    105.0,
			Score:    6.0,
		},
	}

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	converted := p.ConvertAnomalyTimezone(anomalies, tokyo)

	if len(converted) != 2 {
		t.Errorf("Expected 2 anomalies, got %d", len(converted))
	}

	if converted[0].Time != "2024-03-15T09:00:00+09:00" {
		t.Errorf("Expected first anomaly time to be converted, got %s", converted[0].Time)
	}

	if converted[1].Time != "2024-03-15T15:00:00+09:00" {
		t.Errorf("Expected second anomaly time to be converted, got %s", converted[1].Time)
	}

	// Check other fields preserved
	if converted[0].Value != 100.0 || converted[0].DeviceID != "device1" {
		t.Error("Expected other fields to be preserved")
	}
}

func TestConvertAnomalyTimezone_NilLocation(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	anomalies := []AnomalyResult{
		{
			Time:     "2024-03-15T00:00:00Z",
			DeviceID: "device1",
		},
	}

	converted := p.ConvertAnomalyTimezone(anomalies, nil)

	if converted[0].Time != "2024-03-15T00:00:00Z" {
		t.Errorf("Expected unchanged time with nil location")
	}
}

func TestConvertAnomalyTimezone_EmptyAnomalies(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	anomalies := []AnomalyResult{}

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	converted := p.ConvertAnomalyTimezone(anomalies, tokyo)

	if len(converted) != 0 {
		t.Errorf("Expected 0 anomalies, got %d", len(converted))
	}
}
