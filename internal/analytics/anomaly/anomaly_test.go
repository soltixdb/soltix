package anomaly

import (
	"math"
	"testing"
	"time"
)

func createTestDataPoints(values []float64) []DataPoint {
	points := make([]DataPoint, len(values))
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, v := range values {
		points[i] = DataPoint{
			Time:  baseTime.Add(time.Duration(i) * time.Minute),
			Value: v,
		}
	}
	return points
}

func TestZScoreDetector_DetectSpike(t *testing.T) {
	detector := &ZScoreDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5
	config.Threshold = 2.0 // Lower threshold for easier detection

	// Create data where spike is clearly anomalous (5 std deviations away)
	values := []float64{10, 10, 10, 10, 10, 10, 100, 10, 10, 10}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) == 0 {
		t.Fatal("Expected to detect at least one anomaly (the spike at index 6)")
	}

	foundSpike := false
	for _, r := range results {
		if r.Index == 6 {
			foundSpike = true
			if r.Type != AnomalyTypeSpike {
				t.Errorf("Expected anomaly type Spike, got %s", r.Type)
			}
			break
		}
	}

	if !foundSpike {
		t.Error("Expected to detect spike at index 6")
	}
}

func TestZScoreDetector_DetectDrop(t *testing.T) {
	detector := &ZScoreDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5
	config.Threshold = 2.0 // Lower threshold for easier detection

	// Create data where drop is clearly anomalous
	values := []float64{50, 50, 50, 50, 50, 50, 0, 50, 50, 50}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) == 0 {
		t.Fatal("Expected to detect at least one anomaly (the drop at index 6)")
	}

	foundDrop := false
	for _, r := range results {
		if r.Index == 6 {
			foundDrop = true
			if r.Type != AnomalyTypeDrop {
				t.Errorf("Expected anomaly type Drop, got %s", r.Type)
			}
			break
		}
	}

	if !foundDrop {
		t.Error("Expected to detect drop at index 6")
	}
}

func TestZScoreDetector_NoAnomalies(t *testing.T) {
	detector := &ZScoreDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5

	values := []float64{10, 11, 10, 12, 11, 10, 11, 11, 10, 12}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) != 0 {
		t.Errorf("Expected no anomalies in normal data, got %d", len(results))
	}
}

func TestZScoreDetector_Flatline(t *testing.T) {
	detector := &ZScoreDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5

	values := []float64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) != len(values) {
		t.Errorf("Expected all points flagged as flatline, got %d", len(results))
	}

	for _, r := range results {
		if r.Type != AnomalyTypeFlatline {
			t.Errorf("Expected anomaly type Flatline, got %s", r.Type)
		}
	}
}

func TestZScoreDetector_InsufficientData(t *testing.T) {
	detector := &ZScoreDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 10

	values := []float64{10, 50, 10}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) != 0 {
		t.Errorf("Expected no results with insufficient data, got %d", len(results))
	}
}

func TestCalculateMeanStdDev(t *testing.T) {
	values := []float64{2, 4, 4, 4, 5, 5, 7, 9}

	mean, stdDev := CalculateMeanStdDev(values)

	expectedMean := 5.0
	if math.Abs(mean-expectedMean) > 0.01 {
		t.Errorf("Expected mean %f, got %f", expectedMean, mean)
	}

	if stdDev < 1.5 || stdDev > 2.5 {
		t.Errorf("Expected stdDev around 2.0, got %f", stdDev)
	}
}

func TestIQRDetector_DetectOutliers(t *testing.T) {
	detector := &IQRDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5
	config.Threshold = 1.5

	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 100}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) == 0 {
		t.Fatal("Expected to detect outlier at index 9 (value 100)")
	}

	foundOutlier := false
	for _, r := range results {
		if r.Index == 9 {
			foundOutlier = true
			if r.Type != AnomalyTypeSpike {
				t.Errorf("Expected anomaly type Spike, got %s", r.Type)
			}
			break
		}
	}

	if !foundOutlier {
		t.Error("Expected to detect outlier at index 9")
	}
}

func TestIQRDetector_NoAnomalies(t *testing.T) {
	detector := &IQRDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5
	config.Threshold = 1.5

	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) != 0 {
		t.Errorf("Expected no anomalies, got %d", len(results))
	}
}

func TestCalculateIQR(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}

	q1, q3, iqr := CalculateIQR(values)

	if q1 < 2 || q1 > 3 {
		t.Errorf("Expected Q1 around 2.5, got %f", q1)
	}

	if q3 < 7 || q3 > 8 {
		t.Errorf("Expected Q3 around 7.5, got %f", q3)
	}

	expectedIQR := q3 - q1
	if math.Abs(iqr-expectedIQR) > 0.01 {
		t.Errorf("Expected IQR %f, got %f", expectedIQR, iqr)
	}
}

func TestMovingAverageDetector_DetectSuddenChange(t *testing.T) {
	detector := &MovingAverageDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5
	config.WindowSize = 5

	values := []float64{10, 10, 10, 10, 10, 10, 50, 10, 10, 10, 10, 10}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) == 0 {
		t.Fatal("Expected to detect sudden change at index 6")
	}

	foundSpike := false
	for _, r := range results {
		if r.Index == 6 {
			foundSpike = true
			break
		}
	}

	if !foundSpike {
		t.Error("Expected to detect change at index 6")
	}
}

func TestCalculateMovingAverage(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}
	windowSize := 3

	result := CalculateMovingAverage(values, windowSize)

	if len(result) != len(values) {
		t.Errorf("Expected result length %d, got %d", len(values), len(result))
	}

	if math.Abs(result[2]-3) > 0.01 {
		t.Errorf("Expected moving average at index 2 to be 3, got %f", result[2])
	}
}

func TestAutoDetector_SelectsAlgorithm(t *testing.T) {
	detector := &AutoDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5

	values := []float64{10, 11, 10, 12, 11, 10, 100, 11, 10, 12}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) == 0 {
		t.Fatal("Expected auto detector to find anomalies")
	}
}

func TestAnalyzeData_TrendingData(t *testing.T) {
	values := make([]float64, 50)
	for i := range values {
		values[i] = float64(i) * 10
	}
	data := createTestDataPoints(values)

	chars := AnalyzeData(data)

	if !chars.HasTrend {
		t.Error("Expected to detect trend in trending data")
	}

	if chars.TrendStrength <= 0 {
		t.Errorf("Expected positive trend strength, got %f", chars.TrendStrength)
	}
}

func TestAnalyzeData_DataWithOutliers(t *testing.T) {
	values := make([]float64, 100)
	for i := range values {
		values[i] = 50
		if i%10 == 0 {
			values[i] = 500
		}
	}
	data := createTestDataPoints(values)

	chars := AnalyzeData(data)

	if !chars.HasOutliers {
		t.Error("Expected to detect outliers")
	}

	if chars.OutlierPercentage < 5 {
		t.Errorf("Expected >5%% outliers, got %f%%", chars.OutlierPercentage)
	}

	if chars.SelectedAlgorithm != "iqr" {
		t.Errorf("Expected IQR for data with outliers, got %s", chars.SelectedAlgorithm)
	}
}

func TestDetectorRegistry(t *testing.T) {
	detectors := ListDetectors()

	if len(detectors) < 4 {
		t.Errorf("Expected at least 4 detectors, got %d", len(detectors))
	}

	expectedDetectors := []string{"zscore", "iqr", "moving_avg", "auto"}
	for _, name := range expectedDetectors {
		detector, err := GetDetector(name)
		if err != nil {
			t.Errorf("Expected detector %s to exist, got error: %v", name, err)
		}
		if detector == nil {
			t.Errorf("Expected detector %s to not be nil", name)
		}
	}
}

func TestGetDetector_Unknown(t *testing.T) {
	_, err := GetDetector("unknown_detector")

	if err == nil {
		t.Error("Expected error for unknown detector")
	}
}

func TestEmptyData(t *testing.T) {
	detectors := []AnomalyDetector{
		&ZScoreDetector{},
		&IQRDetector{},
		&MovingAverageDetector{},
		&AutoDetector{},
	}

	config := DefaultConfig()
	data := []DataPoint{}

	for _, detector := range detectors {
		results := detector.Detect(data, config)
		if len(results) != 0 {
			t.Errorf("%s: Expected no results for empty data", detector.Name())
		}
	}
}

func TestNegativeValues(t *testing.T) {
	detector := &ZScoreDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5
	config.Threshold = 2.0 // Lower threshold

	// Negative values with a clear anomaly
	values := []float64{-10, -10, -10, -10, -10, -10, -100, -10, -10, -10}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) == 0 {
		t.Error("Expected to detect anomaly in negative values")
	}
}

func TestVeryHighThreshold(t *testing.T) {
	detector := &ZScoreDetector{}
	config := DefaultConfig()
	config.MinDataPoints = 5
	config.Threshold = 100

	values := []float64{10, 11, 10, 12, 11, 10, 50, 11, 10, 12}
	data := createTestDataPoints(values)

	results := detector.Detect(data, config)

	if len(results) != 0 {
		t.Errorf("Expected no anomalies with high threshold, got %d", len(results))
	}
}
