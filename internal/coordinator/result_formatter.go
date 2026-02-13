package coordinator

import (
	"sort"
)

// ResultFormatter handles formatting and aggregation of query results
type ResultFormatter struct{}

// NewResultFormatter creates a new ResultFormatter
func NewResultFormatter() *ResultFormatter {
	return &ResultFormatter{}
}

// MergeAndFormat merges query results from multiple nodes and formats them
// into the time-series format (times array + field arrays)
func (f *ResultFormatter) MergeAndFormat(results []QueryResult, requestedFields []string) []FormattedQueryResult {
	// Group by device ID and build time-series format directly
	type timeSeriesBuilder struct {
		timeMap map[string]map[string]interface{} // time -> field -> value
	}

	deviceMap := make(map[string]*timeSeriesBuilder)

	// Collect all data points grouped by device
	for _, result := range results {
		builder, exists := deviceMap[result.DeviceID]
		if !exists {
			builder = &timeSeriesBuilder{
				timeMap: make(map[string]map[string]interface{}),
			}
			deviceMap[result.DeviceID] = builder
		}

		for _, dp := range result.DataPoints {
			if _, exists := builder.timeMap[dp.Time]; !exists {
				builder.timeMap[dp.Time] = make(map[string]interface{})
			}
			// Merge fields (later values overwrite earlier ones)
			for fieldName, fieldValue := range dp.Fields {
				builder.timeMap[dp.Time][fieldName] = fieldValue
			}
		}
	}

	// Build formatted results
	formatted := make([]FormattedQueryResult, 0, len(deviceMap))

	for deviceID, builder := range deviceMap {
		// Sort times
		times := make([]string, 0, len(builder.timeMap))
		for t := range builder.timeMap {
			times = append(times, t)
		}
		sort.Strings(times)

		// Collect all field names
		fieldNames := make(map[string]bool)
		if len(requestedFields) > 0 {
			for _, f := range requestedFields {
				fieldNames[f] = true
			}
		} else {
			// Collect all unique fields from all time points
			for _, fields := range builder.timeMap {
				for fieldName := range fields {
					fieldNames[fieldName] = true
				}
			}
		}

		// Build field arrays
		fieldArrays := make(map[string][]interface{})
		for fieldName := range fieldNames {
			values := make([]interface{}, len(times))
			for i, t := range times {
				if val, ok := builder.timeMap[t][fieldName]; ok {
					values[i] = val
				} else {
					values[i] = nil
				}
			}
			fieldArrays[fieldName] = values
		}

		if len(times) > 0 {
			formatted = append(formatted, FormattedQueryResult{
				DeviceID: deviceID,
				Times:    times,
				Fields:   fieldArrays,
			})
		}
	}

	return formatted
}

// ApplyLimit applies a limit across all devices' results
// If limit <= 0, it means unlimited and returns all results
func (f *ResultFormatter) ApplyLimit(results []FormattedQueryResult, limit int64) []FormattedQueryResult {
	if limit <= 0 {
		return results
	}

	limited := make([]FormattedQueryResult, 0, len(results))
	remaining := limit

	for _, result := range results {
		if remaining <= 0 {
			break
		}

		if int64(len(result.Times)) <= remaining {
			// Take all points
			limited = append(limited, result)
			remaining -= int64(len(result.Times))
		} else {
			// Take partial points - slice all arrays to match limit
			truncatedFields := make(map[string][]interface{})
			for fieldName, values := range result.Fields {
				truncatedFields[fieldName] = values[:remaining]
			}
			limited = append(limited, FormattedQueryResult{
				DeviceID: result.DeviceID,
				Times:    result.Times[:remaining],
				Fields:   truncatedFields,
			})
			remaining = 0
		}
	}

	return limited
}

// CountPoints counts total data points across all devices
func (f *ResultFormatter) CountPoints(results []FormattedQueryResult) int {
	total := 0
	for _, result := range results {
		total += len(result.Times)
	}
	return total
}
