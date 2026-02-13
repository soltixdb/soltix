package processing

import (
	"testing"
	"time"

	"github.com/soltixdb/soltix/internal/analytics/anomaly"
	"github.com/soltixdb/soltix/internal/logging"
)

func TestDetectAnomalies_ZScore(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Create data with an obvious anomaly
	times := make([]string, 30)
	values := make([]interface{}, 30)
	for i := 0; i < 30; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		if i == 15 {
			values[i] = 100.0 // Anomaly
		} else {
			values[i] = 10.0 + float64(i%3) // Normal values around 10-12
		}
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temperature": values,
			},
		},
	}

	anomalies := p.DetectAnomalies(results, "zscore", 2.0, "temperature")

	if len(anomalies) == 0 {
		t.Error("Expected at least one anomaly to be detected")
	}

	// Check the anomaly is around index 15
	found := false
	for _, a := range anomalies {
		if a.Value == 100.0 {
			found = true
			if a.Algorithm != "zscore" {
				t.Errorf("Expected algorithm zscore, got %s", a.Algorithm)
			}
			if a.DeviceID != "device1" {
				t.Errorf("Expected device1, got %s", a.DeviceID)
			}
			if a.Field != "temperature" {
				t.Errorf("Expected temperature field, got %s", a.Field)
			}
		}
	}

	if !found {
		t.Error("Expected anomaly with value 100.0 to be detected")
	}
}

func TestDetectAnomalies_IQR(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Create data with an obvious anomaly
	times := make([]string, 30)
	values := make([]interface{}, 30)
	for i := 0; i < 30; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		if i == 15 {
			values[i] = 1000.0 // Anomaly
		} else {
			values[i] = 10.0 + float64(i%5)
		}
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temperature": values,
			},
		},
	}

	anomalies := p.DetectAnomalies(results, "iqr", 1.5, "temperature")

	if len(anomalies) == 0 {
		t.Error("Expected at least one anomaly to be detected")
	}
}

func TestDetectAnomalies_NoAlgorithm(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-01-01T00:00:00Z"},
			Fields: map[string][]interface{}{
				"temperature": {10.0},
			},
		},
	}

	// No algorithm - should return nil
	anomalies := p.DetectAnomalies(results, "", 2.0, "temperature")
	if anomalies != nil {
		t.Errorf("Expected nil anomalies with empty algorithm, got %d", len(anomalies))
	}

	// "none" algorithm - should return nil
	anomalies = p.DetectAnomalies(results, "none", 2.0, "temperature")
	if anomalies != nil {
		t.Errorf("Expected nil anomalies with 'none' algorithm, got %d", len(anomalies))
	}
}

func TestDetectAnomalies_EmptyResults(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	results := []QueryResult{}

	anomalies := p.DetectAnomalies(results, "zscore", 2.0, "temperature")
	if anomalies != nil {
		t.Errorf("Expected nil anomalies with empty results, got %d", len(anomalies))
	}
}

func TestDetectAnomalies_AllFields(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Create data with anomalies in multiple fields
	times := make([]string, 30)
	tempValues := make([]interface{}, 30)
	humidValues := make([]interface{}, 30)
	for i := 0; i < 30; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		if i == 15 {
			tempValues[i] = 100.0 // Anomaly in temperature
		} else {
			tempValues[i] = 20.0
		}
		if i == 20 {
			humidValues[i] = 200.0 // Anomaly in humidity
		} else {
			humidValues[i] = 50.0
		}
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temperature": tempValues,
				"humidity":    humidValues,
			},
		},
	}

	// Empty target field means analyze all fields
	anomalies := p.DetectAnomalies(results, "zscore", 2.0, "")

	if len(anomalies) < 2 {
		t.Errorf("Expected at least 2 anomalies (one per field), got %d", len(anomalies))
	}

	// Should have anomalies from both fields
	hasTemp := false
	hasHumid := false
	for _, a := range anomalies {
		if a.Field == "temperature" {
			hasTemp = true
		}
		if a.Field == "humidity" {
			hasHumid = true
		}
	}

	if !hasTemp {
		t.Error("Expected anomaly in temperature field")
	}
	if !hasHumid {
		t.Error("Expected anomaly in humidity field")
	}
}

func TestDetectAnomalies_MultipleDevices(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Create data for two devices with anomalies
	times := make([]string, 30)
	values1 := make([]interface{}, 30)
	values2 := make([]interface{}, 30)
	for i := 0; i < 30; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		if i == 15 {
			values1[i] = 100.0 // Anomaly in device1
		} else {
			values1[i] = 20.0
		}
		if i == 20 {
			values2[i] = 200.0 // Anomaly in device2
		} else {
			values2[i] = 50.0
		}
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temp": values1,
			},
		},
		{
			DeviceID: "device2",
			Times:    times,
			Fields: map[string][]interface{}{
				"temp": values2,
			},
		},
	}

	anomalies := p.DetectAnomalies(results, "zscore", 2.0, "temp")

	if len(anomalies) < 2 {
		t.Errorf("Expected at least 2 anomalies (one per device), got %d", len(anomalies))
	}

	// Should have anomalies from both devices
	hasDevice1 := false
	hasDevice2 := false
	for _, a := range anomalies {
		if a.DeviceID == "device1" {
			hasDevice1 = true
		}
		if a.DeviceID == "device2" {
			hasDevice2 = true
		}
	}

	if !hasDevice1 {
		t.Error("Expected anomaly from device1")
	}
	if !hasDevice2 {
		t.Error("Expected anomaly from device2")
	}
}

func TestDetectAnomalies_Expected(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	// Create data with an obvious anomaly
	times := make([]string, 30)
	values := make([]interface{}, 30)
	for i := 0; i < 30; i++ {
		tm := time.Date(2024, 1, 1, 0, i, 0, 0, time.UTC)
		times[i] = tm.Format(time.RFC3339)
		if i == 15 {
			values[i] = 100.0 // Anomaly
		} else {
			values[i] = 10.0
		}
	}

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    times,
			Fields: map[string][]interface{}{
				"temperature": values,
			},
		},
	}

	anomalies := p.DetectAnomalies(results, "zscore", 2.0, "temperature")

	if len(anomalies) == 0 {
		t.Error("Expected at least one anomaly")
		return
	}

	// Check that anomaly has expected range
	for _, a := range anomalies {
		if a.Value == 100.0 && a.Expected != nil {
			if a.Expected.Min >= a.Expected.Max {
				t.Error("Expected range Min should be less than Max")
			}
			// The expected range should be around 10.0 (the normal value)
			if a.Expected.Min > 20 || a.Expected.Max < 0 {
				t.Error("Expected range seems wrong")
			}
			return
		}
	}
}

func TestDetectAnomalies_UnknownAlgorithm(t *testing.T) {
	logger := logging.NewDevelopment()
	p := NewProcessor(logger)

	results := []QueryResult{
		{
			DeviceID: "device1",
			Times:    []string{"2024-01-01T00:00:00Z"},
			Fields: map[string][]interface{}{
				"temperature": {10.0},
			},
		},
	}

	// Unknown algorithm should return nil (logs warning)
	anomalies := p.DetectAnomalies(results, "unknown_algo", 2.0, "temperature")
	if anomalies != nil {
		t.Errorf("Expected nil anomalies with unknown algorithm, got %d", len(anomalies))
	}
}

func TestAnomalyResult_Type(t *testing.T) {
	// Test that AnomalyResult can hold different anomaly types
	result := AnomalyResult{
		Time:      "2024-01-01T00:00:00Z",
		DeviceID:  "device1",
		Field:     "temp",
		Value:     100.0,
		Score:     5.0,
		Type:      anomaly.AnomalyTypeSpike,
		Algorithm: "zscore",
	}

	if result.Type != anomaly.AnomalyTypeSpike {
		t.Errorf("Expected AnomalyTypeSpike, got %v", result.Type)
	}

	result.Type = anomaly.AnomalyTypeDrop
	if result.Type != anomaly.AnomalyTypeDrop {
		t.Errorf("Expected AnomalyTypeDrop, got %v", result.Type)
	}
}
