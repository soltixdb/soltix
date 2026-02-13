package processing

import (
	"time"
)

// ConvertTimezone converts all times in results to the specified timezone
// Times are expected to be in RFC3339 format
func (p *Processor) ConvertTimezone(results []QueryResult, loc *time.Location) []QueryResult {
	if loc == nil || len(results) == 0 {
		return results
	}

	// Check if conversion is needed by sampling the first time
	// If already in the target timezone, skip conversion
	if len(results[0].Times) > 0 {
		sampleTime, err := time.Parse(time.RFC3339, results[0].Times[0])
		if err == nil {
			// Compare timezone offsets at this point in time
			_, inputOffset := sampleTime.Zone()
			_, outputOffset := sampleTime.In(loc).Zone()
			if inputOffset == outputOffset {
				p.logger.Debug("Skipping timezone conversion - already in target timezone",
					"timezone", loc.String())
				return results
			}
		}
	}

	converted := make([]QueryResult, len(results))
	for i, result := range results {
		convertedTimes := make([]string, len(result.Times))
		for j, timeStr := range result.Times {
			// Parse the time string
			t, err := time.Parse(time.RFC3339, timeStr)
			if err != nil {
				// If parsing fails, keep original
				convertedTimes[j] = timeStr
				continue
			}
			// Convert to target timezone and format
			convertedTimes[j] = t.In(loc).Format(time.RFC3339)
		}
		converted[i] = QueryResult{
			DeviceID: result.DeviceID,
			Times:    convertedTimes,
			Fields:   result.Fields,
		}
	}

	p.logger.Debug("Converted times to timezone",
		"timezone", loc.String(),
		"results", len(results))

	return converted
}

// ConvertAnomalyTimezone converts anomaly times to the specified timezone
func (p *Processor) ConvertAnomalyTimezone(anomalies []AnomalyResult, loc *time.Location) []AnomalyResult {
	if loc == nil || len(anomalies) == 0 {
		return anomalies
	}

	result := make([]AnomalyResult, len(anomalies))
	for i, a := range anomalies {
		result[i] = a

		// Parse and convert time
		t, err := time.Parse(time.RFC3339, a.Time)
		if err != nil {
			continue
		}
		result[i].Time = t.In(loc).Format(time.RFC3339)
	}

	return result
}
