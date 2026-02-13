package processing

import (
	"time"

	"github.com/soltixdb/soltix/internal/analytics/anomaly"
	"github.com/soltixdb/soltix/internal/utils"
)

// DetectAnomalies runs anomaly detection on query results
func (p *Processor) DetectAnomalies(
	results []QueryResult,
	algorithm string,
	threshold float64,
	targetField string,
) []AnomalyResult {
	if algorithm == "" || algorithm == "none" {
		return nil
	}

	if len(results) == 0 {
		return nil
	}

	detector, err := anomaly.GetDetector(algorithm)
	if err != nil {
		p.logger.Warn("Unknown anomaly detection algorithm, skipping",
			"algorithm", algorithm,
			"error", err)
		return nil
	}

	config := anomaly.DefaultConfig()
	config.Threshold = threshold

	var allAnomalies []AnomalyResult

	for _, result := range results {
		// Determine which fields to analyze
		fieldsToAnalyze := make([]string, 0)
		if targetField != "" {
			// Only analyze specific field
			if _, ok := result.Fields[targetField]; ok {
				fieldsToAnalyze = append(fieldsToAnalyze, targetField)
			}
		} else {
			// Analyze all numeric fields
			for fieldName := range result.Fields {
				fieldsToAnalyze = append(fieldsToAnalyze, fieldName)
			}
		}

		for _, fieldName := range fieldsToAnalyze {
			fieldValues := result.Fields[fieldName]

			// Convert to DataPoints
			dataPoints := make([]anomaly.DataPoint, 0, len(result.Times))
			for i, timeStr := range result.Times {
				if i >= len(fieldValues) {
					break
				}

				value := fieldValues[i]
				if value == nil {
					continue
				}

				// Convert value to float64 using shared utility
				floatVal, ok := utils.ToFloat64(value)
				if !ok {
					continue // Skip non-numeric values
				}

				// Parse time
				t, err := time.Parse(time.RFC3339, timeStr)
				if err != nil {
					continue
				}

				dataPoints = append(dataPoints, anomaly.DataPoint{
					Time:  t,
					Value: floatVal,
				})
			}

			if len(dataPoints) < config.MinDataPoints {
				continue
			}

			// Run detection
			anomalyResults := detector.Detect(dataPoints, config)

			// Convert results
			for _, ar := range anomalyResults {
				if ar.Index < 0 || ar.Index >= len(result.Times) {
					continue
				}

				anomalyResult := AnomalyResult{
					Time:      result.Times[ar.Index],
					DeviceID:  result.DeviceID,
					Field:     fieldName,
					Value:     dataPoints[ar.Index].Value,
					Score:     ar.Score,
					Type:      ar.Type,
					Algorithm: detector.Name(),
				}

				if ar.Expected != nil {
					anomalyResult.Expected = ar.Expected
				}

				allAnomalies = append(allAnomalies, anomalyResult)
			}
		}
	}

	if len(allAnomalies) > 0 {
		p.logger.Debug("Anomaly detection completed",
			"algorithm", algorithm,
			"threshold", threshold,
			"anomalies_found", len(allAnomalies))
	}

	return allAnomalies
}
