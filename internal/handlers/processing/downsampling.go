package processing

import (
	"sort"

	"github.com/soltixdb/soltix/internal/downsampling"
)

// ApplyDownsampling applies downsampling to query results
// All fields for a device share the same downsampled time points (based on first numeric field)
// If threshold <= 0, it will be auto-calculated based on data size
func (p *Processor) ApplyDownsampling(results []QueryResult, mode string, threshold int) []QueryResult {
	if mode == "" || mode == "none" {
		return results
	}

	// Convert mode string to downsampling.Mode
	var dsMode downsampling.Mode
	switch mode {
	case "auto":
		dsMode = downsampling.ModeAuto
	case "lttb":
		dsMode = downsampling.ModeLTTB
	case "minmax":
		dsMode = downsampling.ModeMinMax
	case "avg":
		dsMode = downsampling.ModeAverage
	case "m4":
		dsMode = downsampling.ModeM4
	default:
		p.logger.Warn("Unknown downsampling mode, skipping", "mode", mode)
		return results
	}

	// Auto-calculate threshold if not provided
	if threshold <= 0 {
		threshold = p.CalculateAutoThreshold(results)
		p.logger.Debug("Auto-calculated downsampling threshold", "threshold", threshold)
	}

	downsampled := make([]QueryResult, 0, len(results))

	for _, result := range results {
		if len(result.Times) == 0 {
			downsampled = append(downsampled, result)
			continue
		}

		// Determine which time points to keep based on the first numeric field
		// Create a deterministic field order to avoid map iteration non-determinism
		fieldNames := make([]string, 0, len(result.Fields))
		for fieldName := range result.Fields {
			fieldNames = append(fieldNames, fieldName)
		}
		sort.Strings(fieldNames) // Sort to get deterministic order

		var downsampledTimes []string

		// Find first numeric field to use as basis for downsampling
		for _, fieldName := range fieldNames {
			fieldValues := result.Fields[fieldName]
			dsTime, _, err := downsampling.ApplyDownsampling(result.Times, fieldValues, dsMode, threshold)
			if err != nil {
				continue // Try next field
			}

			// Successfully downsampled - use this field's times as the master
			downsampledTimes = dsTime

			p.logger.Debug("Using field for downsampling basis",
				"device", result.DeviceID,
				"field", fieldName,
				"original_points", len(result.Times),
				"downsampled_points", len(dsTime))
			break
		}

		// If downsampling failed for all fields, keep original data
		if len(downsampledTimes) == 0 {
			p.logger.Warn("Downsampling failed for all fields, keeping original data",
				"device", result.DeviceID)
			downsampled = append(downsampled, result)
			continue
		}

		// Build a map from downsampled time to index for lookup
		downsampledFields := make(map[string][]interface{})
		timeToIndex := make(map[string][]int, len(result.Times)) // time -> list of indices (for duplicate times)

		for i, t := range result.Times {
			timeToIndex[t] = append(timeToIndex[t], i)
		}

		// For each field, extract values at the downsampled time points
		for fieldName, fieldValues := range result.Fields {
			dsValues := make([]interface{}, len(downsampledTimes))
			for i, t := range downsampledTimes {
				if indices, exists := timeToIndex[t]; exists && len(indices) > 0 {
					// Use first index for this time (most common case)
					dsValues[i] = fieldValues[indices[0]]
				} else {
					dsValues[i] = nil
				}
			}
			downsampledFields[fieldName] = dsValues
		}

		downsampled = append(downsampled, QueryResult{
			DeviceID: result.DeviceID,
			Times:    downsampledTimes,
			Fields:   downsampledFields,
		})
	}

	p.logger.Debug("Applied downsampling",
		"mode", mode,
		"threshold", threshold,
		"before", CountPoints(results),
		"after", CountPoints(downsampled))

	return downsampled
}

// CalculateAutoThreshold automatically calculates an appropriate downsampling threshold
// based on the total number of data points across all devices.
// Strategy:
// - For small datasets (<1000 points): no downsampling needed, return total
// - For medium datasets (1000-10000): target ~1000 points
// - For large datasets (10000-100000): target ~2000 points
// - For very large datasets (>100000): target ~5000 points or 5% of total, whichever is smaller
// This ensures good visualization quality while managing data transfer size
func (p *Processor) CalculateAutoThreshold(results []QueryResult) int {
	totalPoints := CountPoints(results)

	// Small datasets: no downsampling needed
	if totalPoints <= 1000 {
		return totalPoints
	}

	// Medium datasets: target ~1000 points
	if totalPoints <= 10000 {
		return 1000
	}

	// Large datasets: target ~2000 points
	if totalPoints <= 100000 {
		return 2000
	}

	// Very large datasets: 5% of total or 5000, whichever is smaller
	calculated := totalPoints / 20 // 5%
	if calculated > 5000 {
		return 5000
	}
	return calculated
}
