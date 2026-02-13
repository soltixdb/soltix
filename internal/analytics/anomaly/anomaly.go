package anomaly

import (
	"fmt"

	"github.com/soltixdb/soltix/internal/analytics"
)

// AnomalyType represents the type of anomaly detected
type AnomalyType string

const (
	AnomalyTypeSpike    AnomalyType = "spike"    // Sudden increase
	AnomalyTypeDrop     AnomalyType = "drop"     // Sudden decrease
	AnomalyTypeOutlier  AnomalyType = "outlier"  // Value outside normal range
	AnomalyTypeFlatline AnomalyType = "flatline" // No variation (possibly sensor failure)
)

// Anomaly represents a detected anomaly in time-series data
type Anomaly struct {
	Time      string      `json:"time"`
	DeviceID  string      `json:"device_id"`
	Field     string      `json:"field"`
	Value     float64     `json:"value"`
	Expected  *Range      `json:"expected,omitempty"`
	Score     float64     `json:"score"`     // How anomalous (higher = more abnormal)
	Type      AnomalyType `json:"type"`      // Type of anomaly
	Algorithm string      `json:"algorithm"` // Which algorithm detected it
}

// Range represents expected value range
type Range struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

// DataPoint is an alias to the shared analytics.TimeSeriesPoint type.
// This maintains backward compatibility while eliminating code duplication.
type DataPoint = analytics.TimeSeriesPoint

// DetectorConfig holds configuration for anomaly detection
type DetectorConfig struct {
	// Threshold for detection sensitivity (e.g., number of std deviations for Z-Score)
	Threshold float64

	// WindowSize for moving average/window-based algorithms
	WindowSize int

	// MinDataPoints minimum number of points required for detection
	MinDataPoints int
}

// DefaultConfig returns default detector configuration
func DefaultConfig() DetectorConfig {
	return DetectorConfig{
		Threshold:     3.0, // 3 standard deviations
		WindowSize:    10,  // 10-point moving window
		MinDataPoints: 10,  // Need at least 10 points
	}
}

// AnomalyDetector interface for all anomaly detection algorithms
type AnomalyDetector interface {
	// Name returns the algorithm name
	Name() string

	// Detect finds anomalies in the given data points
	// Returns list of indices that are anomalies and their scores
	Detect(data []DataPoint, config DetectorConfig) []AnomalyResult
}

// AnomalyResult contains detection result for a single point
type AnomalyResult struct {
	Index    int         // Index in original data
	Score    float64     // Anomaly score
	Type     AnomalyType // Type of anomaly
	Expected *Range      // Expected range
}

// Registry holds available anomaly detectors
var detectorRegistry = make(map[string]AnomalyDetector)

// RegisterDetector adds a detector to the registry
func RegisterDetector(name string, detector AnomalyDetector) {
	detectorRegistry[name] = detector
}

// GetDetector returns a detector by name
func GetDetector(name string) (AnomalyDetector, error) {
	if detector, ok := detectorRegistry[name]; ok {
		return detector, nil
	}
	return nil, fmt.Errorf("unknown anomaly detector: %s", name)
}

// ListDetectors returns list of available detector names
func ListDetectors() []string {
	names := make([]string, 0, len(detectorRegistry))
	for name := range detectorRegistry {
		names = append(names, name)
	}
	return names
}

// DetectAnomalies is a helper function to detect anomalies using specified algorithm
func DetectAnomalies(algorithm string, data []DataPoint, config DetectorConfig) ([]AnomalyResult, error) {
	detector, err := GetDetector(algorithm)
	if err != nil {
		return nil, err
	}
	return detector.Detect(data, config), nil
}
