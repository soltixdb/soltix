package downsampling

import (
	"fmt"
	"math"

	"github.com/soltixdb/soltix/internal/utils"
)

// Mode represents the downsampling mode
type Mode string

const (
	// ModeNone means no downsampling
	ModeNone Mode = "none"
	// ModeAuto automatically determines if downsampling is needed
	ModeAuto Mode = "auto"
	// ModeLTTB uses Largest-Triangle-Three-Buckets algorithm
	ModeLTTB Mode = "lttb"
	// ModeMinMax keeps min and max values per bucket (preserves peaks/spikes)
	ModeMinMax Mode = "minmax"
	// ModeAverage uses average value per bucket
	ModeAverage Mode = "avg"
	// ModeM4 keeps First, Min, Max, Last per bucket (4 points per bucket)
	ModeM4 Mode = "m4"
)

// DefaultAutoThreshold is the default threshold for auto mode
const DefaultAutoThreshold = 1000

// MinLTTBThreshold is the minimum threshold for LTTB algorithm
const MinLTTBThreshold = 100

// ValidModes returns all valid downsampling modes
func ValidModes() []Mode {
	return []Mode{ModeNone, ModeAuto, ModeLTTB, ModeMinMax, ModeAverage, ModeM4}
}

// IsValid checks if a mode string is valid
func IsValid(mode string) bool {
	for _, m := range ValidModes() {
		if string(m) == mode {
			return true
		}
	}
	return false
}

// Point represents a data point with time index and value
type Point struct {
	TimeIndex int
	Value     float64
}

// ApplyDownsampling applies downsampling to time-series data
// timeValues: array of time strings (ISO format)
// fieldValues: array of numeric values corresponding to times
// mode: downsampling mode
// threshold: target number of points (for auto/lttb modes)
func ApplyDownsampling(timeValues []string, fieldValues []interface{}, mode Mode, threshold int) ([]string, []interface{}, error) {
	if mode == ModeNone {
		return timeValues, fieldValues, nil
	}

	if len(timeValues) != len(fieldValues) {
		return nil, nil, fmt.Errorf("time and value arrays must have same length")
	}

	if len(timeValues) == 0 {
		return timeValues, fieldValues, nil
	}

	// Convert to numeric points, keeping original indices
	// We need to track both the index in the filtered points array and the original index
	points := make([]Point, 0, len(fieldValues))
	originalIndices := make([]int, 0, len(fieldValues))

	for i, v := range fieldValues {
		if v == nil {
			continue
		}
		// Convert to float64 using shared utility
		floatVal, ok := utils.ToFloat64(v)
		if !ok {
			// Skip non-numeric values
			continue
		}
		// TimeIndex here is the index within the filtered array
		points = append(points, Point{TimeIndex: len(points), Value: floatVal})
		// Store the original index in the input arrays
		originalIndices = append(originalIndices, i)
	}

	if len(points) == 0 {
		return timeValues, fieldValues, nil
	}

	// Determine if downsampling is needed
	if mode == ModeAuto {
		if threshold <= 0 {
			threshold = DefaultAutoThreshold
		}
		// Only downsample if we have more points than threshold
		if len(points) <= threshold {
			return timeValues, fieldValues, nil
		}
		// Auto-detect best algorithm based on data characteristics
		mode = detectBestAlgorithm(points)
	}

	// Check threshold
	if threshold < 2 {
		threshold = 2
	}
	if len(points) <= threshold {
		// No need to downsample
		return timeValues, fieldValues, nil
	}

	var sampledIndices []int

	switch mode {
	case ModeLTTB:
		if threshold < MinLTTBThreshold {
			threshold = MinLTTBThreshold
		}
		sampledIndices = lttb(points, threshold)

	case ModeMinMax:
		sampledIndices = minmax(points, threshold)

	case ModeAverage:
		// Average returns computed values, not indices
		return averageDownsample(timeValues, fieldValues, points, originalIndices, threshold)

	case ModeM4:
		sampledIndices = m4(points, threshold)

	default:
		return timeValues, fieldValues, fmt.Errorf("unknown downsampling mode: %s", mode)
	}

	// Build downsampled arrays - map back to original indices
	downsampledTimes := make([]string, len(sampledIndices))
	downsampledValues := make([]interface{}, len(sampledIndices))
	for i, filteredIdx := range sampledIndices {
		originalIdx := originalIndices[filteredIdx]
		downsampledTimes[i] = timeValues[originalIdx]
		downsampledValues[i] = fieldValues[originalIdx]
	}

	return downsampledTimes, downsampledValues, nil
}

// detectBestAlgorithm analyzes data characteristics and selects the most appropriate algorithm
// Returns the recommended downsampling mode based on:
// - Spikiness: high variance/outliers → MinMax to preserve peaks
// - Smoothness: low variance → LTTB for visual accuracy
// - Large datasets: very high point count → Average for performance
func detectBestAlgorithm(points []Point) Mode {
	n := len(points)

	// For very large datasets, prefer Average for performance
	if n > 100000 {
		// But check if data has significant spikes first
		spikiness := calculateSpikiness(points)
		if spikiness > 0.2 {
			return ModeMinMax // Preserve important peaks
		}
		return ModeAverage // Fast for large smooth data
	}

	// Calculate data characteristics
	spikiness := calculateSpikiness(points)

	// High spikiness (many outliers/peaks) → MinMax to preserve peaks
	if spikiness > 0.2 {
		return ModeMinMax
	}

	// Medium spikiness → M4 for balanced visual representation
	if spikiness > 0.1 {
		return ModeM4
	}

	// Low spikiness (smooth data) → LTTB for best visual quality
	return ModeLTTB
}

// calculateSpikiness measures how "spiky" the data is
// Returns a value between 0 (smooth) and 1 (very spiky)
// Based on the ratio of points that deviate significantly from local trend
func calculateSpikiness(points []Point) float64 {
	if len(points) < 10 {
		return 0 // Not enough data to determine
	}

	// Calculate mean and standard deviation
	sum := 0.0
	for _, p := range points {
		sum += p.Value
	}
	mean := sum / float64(len(points))

	// Calculate variance
	variance := 0.0
	for _, p := range points {
		diff := p.Value - mean
		variance += diff * diff
	}
	variance /= float64(len(points))
	stdDev := math.Sqrt(variance)

	if stdDev == 0 {
		return 0 // All values are the same
	}

	// Count "spikes" - points that deviate more than 2 standard deviations from mean
	// Also consider local changes (derivative spikes)
	spikeCount := 0
	derivativeSpikeCount := 0

	for i, p := range points {
		// Check absolute deviation
		if math.Abs(p.Value-mean) > 2*stdDev {
			spikeCount++
		}

		// Check derivative (rate of change)
		if i > 0 {
			change := math.Abs(p.Value - points[i-1].Value)
			if change > stdDev {
				derivativeSpikeCount++
			}
		}
	}

	// Combine both measures
	absoluteSpikiness := float64(spikeCount) / float64(len(points))
	derivativeSpikiness := float64(derivativeSpikeCount) / float64(len(points)-1)

	// Weight derivative changes slightly higher (they're more visually important)
	spikiness := (absoluteSpikiness + 1.5*derivativeSpikiness) / 2.5

	// Clamp to [0, 1]
	if spikiness > 1 {
		spikiness = 1
	}

	return spikiness
}

// lttb implements the Largest-Triangle-Three-Buckets algorithm
// Returns indices of selected points in the original array
func lttb(data []Point, threshold int) []int {
	if len(data) <= threshold {
		// Return all indices
		indices := make([]int, len(data))
		for i := range data {
			indices[i] = data[i].TimeIndex
		}
		return indices
	}

	if threshold <= 2 {
		// Edge case: return first and last
		if len(data) >= 2 {
			return []int{data[0].TimeIndex, data[len(data)-1].TimeIndex}
		}
		return []int{data[0].TimeIndex}
	}

	sampled := make([]int, 0, threshold)

	// Always include first point
	sampled = append(sampled, data[0].TimeIndex)

	// Bucket size (excluding first and last points)
	bucketSize := float64(len(data)-2) / float64(threshold-2)

	// Index of the point in the previous bucket
	a := 0

	for i := 0; i < threshold-2; i++ {
		// Calculate point average for next bucket
		avgRangeStart := int(math.Floor(float64(i+1)*bucketSize)) + 1
		avgRangeEnd := int(math.Floor(float64(i+2)*bucketSize)) + 1
		if avgRangeEnd >= len(data) {
			avgRangeEnd = len(data)
		}

		avgX := 0.0
		avgY := 0.0
		avgRangeLength := avgRangeEnd - avgRangeStart

		for ; avgRangeStart < avgRangeEnd; avgRangeStart++ {
			avgX += float64(avgRangeStart)
			avgY += data[avgRangeStart].Value
		}
		avgX /= float64(avgRangeLength)
		avgY /= float64(avgRangeLength)

		// Get the range for this bucket
		rangeOffs := int(math.Floor(float64(i)*bucketSize)) + 1
		rangeTo := int(math.Floor(float64(i+1)*bucketSize)) + 1

		// Point a (previous selected point)
		pointAX := float64(a)
		pointAY := data[a].Value

		maxArea := -1.0
		var maxAreaPoint int

		for ; rangeOffs < rangeTo; rangeOffs++ {
			// Calculate triangle area over three buckets
			area := math.Abs((pointAX-avgX)*(data[rangeOffs].Value-pointAY)-
				(pointAX-float64(rangeOffs))*(avgY-pointAY)) * 0.5

			if area > maxArea {
				maxArea = area
				maxAreaPoint = rangeOffs
			}
		}

		sampled = append(sampled, data[maxAreaPoint].TimeIndex)
		a = maxAreaPoint
	}

	// Always include last point
	sampled = append(sampled, data[len(data)-1].TimeIndex)

	return sampled
}

// minmax implements Min-Max downsampling algorithm
// Returns both min and max indices for each bucket to preserve peaks and valleys
// Output size: ~2 * numBuckets (min + max per bucket)
func minmax(data []Point, threshold int) []int {
	if len(data) <= threshold {
		indices := make([]int, len(data))
		for i := range data {
			indices[i] = data[i].TimeIndex
		}
		return indices
	}

	// Calculate number of buckets (we return 2 points per bucket: min and max)
	numBuckets := threshold / 2
	if numBuckets < 1 {
		numBuckets = 1
	}

	bucketSize := float64(len(data)) / float64(numBuckets)
	sampled := make([]int, 0, numBuckets*2)

	for i := 0; i < numBuckets; i++ {
		bucketStart := int(float64(i) * bucketSize)
		bucketEnd := int(float64(i+1) * bucketSize)
		if bucketEnd > len(data) {
			bucketEnd = len(data)
		}
		if bucketStart >= bucketEnd {
			continue
		}

		// Find min and max in this bucket
		minIdx := bucketStart
		maxIdx := bucketStart
		minVal := data[bucketStart].Value
		maxVal := data[bucketStart].Value

		for j := bucketStart + 1; j < bucketEnd; j++ {
			if data[j].Value < minVal {
				minVal = data[j].Value
				minIdx = j
			}
			if data[j].Value > maxVal {
				maxVal = data[j].Value
				maxIdx = j
			}
		}

		// Add in time order (min first if it comes before max)
		if minIdx <= maxIdx {
			sampled = append(sampled, data[minIdx].TimeIndex)
			if minIdx != maxIdx {
				sampled = append(sampled, data[maxIdx].TimeIndex)
			}
		} else {
			sampled = append(sampled, data[maxIdx].TimeIndex)
			sampled = append(sampled, data[minIdx].TimeIndex)
		}
	}

	return sampled
}

// averageDownsample computes average values per bucket
// Returns new time/value arrays with computed averages (not original values)
func averageDownsample(timeValues []string, fieldValues []interface{}, points []Point, originalIndices []int, threshold int) ([]string, []interface{}, error) {
	if len(points) <= threshold {
		return timeValues, fieldValues, nil
	}

	numBuckets := threshold
	bucketSize := float64(len(points)) / float64(numBuckets)

	downsampledTimes := make([]string, 0, numBuckets)
	downsampledValues := make([]interface{}, 0, numBuckets)

	for i := 0; i < numBuckets; i++ {
		bucketStart := int(float64(i) * bucketSize)
		bucketEnd := int(float64(i+1) * bucketSize)
		if bucketEnd > len(points) {
			bucketEnd = len(points)
		}
		if bucketStart >= bucketEnd {
			continue
		}

		// Calculate average value in this bucket
		sum := 0.0
		count := 0
		for j := bucketStart; j < bucketEnd; j++ {
			sum += points[j].Value
			count++
		}

		if count > 0 {
			avg := sum / float64(count)
			// Use the middle point's time as representative
			midIdx := bucketStart + count/2
			originalIdx := originalIndices[midIdx]
			downsampledTimes = append(downsampledTimes, timeValues[originalIdx])
			downsampledValues = append(downsampledValues, avg)
		}
	}

	return downsampledTimes, downsampledValues, nil
}

// m4 implements the M4 downsampling algorithm
// Returns First, Min, Max, Last indices for each bucket (up to 4 points per bucket)
// This preserves the visual shape better than simple min/max
func m4(data []Point, threshold int) []int {
	if len(data) <= threshold {
		indices := make([]int, len(data))
		for i := range data {
			indices[i] = data[i].TimeIndex
		}
		return indices
	}

	// Calculate number of buckets (we return up to 4 points per bucket)
	numBuckets := threshold / 4
	if numBuckets < 1 {
		numBuckets = 1
	}

	bucketSize := float64(len(data)) / float64(numBuckets)
	sampled := make([]int, 0, numBuckets*4)

	for i := 0; i < numBuckets; i++ {
		bucketStart := int(float64(i) * bucketSize)
		bucketEnd := int(float64(i+1) * bucketSize)
		if bucketEnd > len(data) {
			bucketEnd = len(data)
		}
		if bucketStart >= bucketEnd {
			continue
		}

		firstIdx := bucketStart
		lastIdx := bucketEnd - 1
		minIdx := bucketStart
		maxIdx := bucketStart
		minVal := data[bucketStart].Value
		maxVal := data[bucketStart].Value

		// Find min and max in this bucket
		for j := bucketStart + 1; j < bucketEnd; j++ {
			if data[j].Value < minVal {
				minVal = data[j].Value
				minIdx = j
			}
			if data[j].Value > maxVal {
				maxVal = data[j].Value
				maxIdx = j
			}
		}

		// Collect unique indices in time order
		// Order: first, then min/max (in time order), then last
		indicesSet := make(map[int]bool)
		orderedIndices := make([]int, 0, 4)

		// Add first
		if !indicesSet[firstIdx] {
			orderedIndices = append(orderedIndices, firstIdx)
			indicesSet[firstIdx] = true
		}

		// Add min and max in time order (between first and last)
		midPoints := []int{}
		if minIdx != firstIdx && minIdx != lastIdx && !indicesSet[minIdx] {
			midPoints = append(midPoints, minIdx)
		}
		if maxIdx != firstIdx && maxIdx != lastIdx && !indicesSet[maxIdx] {
			midPoints = append(midPoints, maxIdx)
		}
		// Sort midPoints by index (time order)
		if len(midPoints) == 2 && midPoints[0] > midPoints[1] {
			midPoints[0], midPoints[1] = midPoints[1], midPoints[0]
		}
		for _, idx := range midPoints {
			if !indicesSet[idx] {
				orderedIndices = append(orderedIndices, idx)
				indicesSet[idx] = true
			}
		}

		// Add last
		if !indicesSet[lastIdx] {
			orderedIndices = append(orderedIndices, lastIdx)
			indicesSet[lastIdx] = true
		}

		// Map to TimeIndex and add to result
		for _, idx := range orderedIndices {
			sampled = append(sampled, data[idx].TimeIndex)
		}
	}

	return sampled
}
