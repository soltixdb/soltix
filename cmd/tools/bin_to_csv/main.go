package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/soltixdb/soltix/internal/aggregation"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
)

func main() {
	// Command line flags
	database := flag.String("database", "shirokuma", "Database name")
	collection := flag.String("collection", "plant_power", "Collection name")
	interval := flag.String("interval", "raw", "Aggregation interval (raw, 1h, 1d, 1mo, 1y)")
	date := flag.String("date", "", "Date in yyyymmdd format")
	dataDir := flag.String("data-dir", "./data/data", "Base raw data directory")
	aggDir := flag.String("agg-dir", "./data/agg", "Base aggregated data directory")
	output := flag.String("output", "./data/csv", "Output CSV directory")
	timezone := flag.String("timezone", "Asia/Tokyo", "Timezone for timestamp conversion")
	deviceID := flag.String("device", "", "Filter by device ID (optional)")

	flag.Parse()

	// Validate required parameters
	if *date == "" {
		log.Fatal("Error: -date parameter is required (format: yyyymmdd)")
	}

	// Parse date
	parsedDate, err := time.Parse("20060102", *date)
	if err != nil {
		log.Fatalf("Error: Invalid date format '%s'. Expected yyyymmdd (e.g., 20260113)\n", *date)
	}

	// Load timezone
	loc, err := time.LoadLocation(*timezone)
	if err != nil {
		log.Fatalf("Error: Invalid timezone '%s': %v\n", *timezone, err)
	}

	// Read and parse data
	var points []DataPoint

	if *interval == "raw" {
		// Use V3 Columnar storage for raw data
		points, err = readStorageData(*dataDir, *database, *collection, parsedDate, loc, *deviceID)
	} else {
		// Aggregated data still uses old format
		points, err = readAggregateData(*aggDir, *database, *collection, *interval, parsedDate, loc, *deviceID)
	}

	if err != nil {
		log.Fatalf("Error reading data: %v\n", err)
	}

	if len(points) == 0 {
		log.Printf("Warning: No data points found\n")
		return
	}

	fmt.Printf("Found %d data points\n", len(points))

	// Ensure output directory exists
	if err := os.MkdirAll(*output, 0o755); err != nil {
		log.Fatalf("Error creating output directory: %v\n", err)
	}

	// Determine output file name
	outputFile := filepath.Join(*output, fmt.Sprintf("%s_%s_%s_%s.csv", *database, *collection, *interval, *date))

	// Export to CSV
	if err := exportToCSV(outputFile, points); err != nil {
		log.Fatalf("Error exporting to CSV: %v\n", err)
	}

	fmt.Printf("Successfully exported to: %s\n", outputFile)
}

// readStorageData reads raw data using TieredStorage (group-aware)
// Directory layout: {dataDir}/group_XXXX/{db}/{collection}/yyyy/mm/yyyymmdd/dg_XXXX/*.bin
func readStorageData(dataDir, database, collection string, date time.Time, timezone *time.Location, deviceFilter string) ([]DataPoint, error) {
	logger := logging.NewDevelopment()

	// Initialize TieredStorage — auto-discovers group_XXXX directories
	config := storage.DefaultGroupStorageConfig(dataDir)
	config.Timezone = timezone
	tieredStorage := storage.NewTieredStorage(config, logger)

	// Query for the entire day
	startTime := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, timezone)
	endTime := startTime.Add(24 * time.Hour)

	// Build device filter
	var deviceIDs []string
	if deviceFilter != "" {
		deviceIDs = []string{deviceFilter}
	}

	// List discovered groups for logging
	groupIDs, _ := tieredStorage.ListGroupIDs()
	fmt.Printf("Discovered %d groups in %s\n", len(groupIDs), dataDir)
	fmt.Printf("Querying TieredStorage: %s/%s from %s to %s\n", database, collection, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

	// Query data across all groups
	storagePoints, err := tieredStorage.Query(database, collection, deviceIDs, startTime, endTime, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query storage: %w", err)
	}

	// Convert to local DataPoint format
	var points []DataPoint
	for _, sp := range storagePoints {
		point := DataPoint{
			Timestamp: sp.Time.In(timezone),
			ID:        sp.ID,
			Values:    make(map[string]interface{}),
		}

		for k, v := range sp.Fields {
			point.Values[k] = v
		}

		points = append(points, point)
	}

	// Sort by timestamp
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp.Before(points[j].Timestamp)
	})

	return points, nil
}

// readAggregateData reads aggregated data (still uses old format)
func readAggregateData(aggDir, database, collection, interval string, date time.Time, timezone *time.Location, deviceFilter string) ([]DataPoint, error) {
	logger := logging.NewDevelopment()
	aggStorage := aggregation.NewStorage(aggDir, logger)
	aggStorage.SetTimezone(timezone)

	var level aggregation.AggregationLevel
	var startTime, endTime time.Time

	switch interval {
	case "1h":
		level = aggregation.AggregationHourly
		startTime = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, timezone)
		endTime = startTime.Add(24 * time.Hour)
	case "1d":
		level = aggregation.AggregationDaily
		startTime = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, timezone)
		endTime = startTime.AddDate(0, 1, 0)
	case "1mo", "1m":
		level = aggregation.AggregationMonthly
		startTime = time.Date(date.Year(), time.January, 1, 0, 0, 0, 0, timezone)
		endTime = startTime.AddDate(1, 0, 0)
	case "1y":
		level = aggregation.AggregationYearly
		startTime = time.Date(date.Year(), time.January, 1, 0, 0, 0, 0, timezone)
		endTime = startTime.AddDate(1, 0, 0)
	default:
		return nil, fmt.Errorf("invalid interval '%s'. Valid options: raw, 1h, 1d, 1mo (or 1m), 1y", interval)
	}

	var deviceIDs []string
	if deviceFilter != "" {
		deviceIDs = []string{deviceFilter}
	}

	opts := aggregation.ReadOptions{
		StartTime: startTime,
		EndTime:   endTime,
		DeviceIDs: deviceIDs,
	}

	aggPoints, err := aggStorage.ReadAggregatedPoints(level, database, collection, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to read aggregated data: %w", err)
	}

	points := make([]DataPoint, 0, len(aggPoints))
	for _, ap := range aggPoints {
		point := DataPoint{
			Timestamp: ap.Time.In(timezone),
			ID:        ap.DeviceID,
			Values:    make(map[string]interface{}),
		}

		for fieldName, fieldValue := range ap.Fields {
			if fieldValue == nil {
				continue
			}
			fieldStats := map[string]interface{}{
				"count": fieldValue.Count,
				"sum":   fieldValue.Sum,
				"min":   fieldValue.Min,
				"max":   fieldValue.Max,
				"avg":   fieldValue.Avg,
			}
			point.Values[fieldName] = fieldStats
		}

		points = append(points, point)
	}

	// Sort by timestamp
	sort.Slice(points, func(i, j int) bool {
		if points[i].Timestamp.Equal(points[j].Timestamp) {
			return points[i].ID < points[j].ID
		}
		return points[i].Timestamp.Before(points[j].Timestamp)
	})

	return points, nil
}

// DataPoint represents a single data point
type DataPoint struct {
	Timestamp time.Time
	ID        string
	Values    map[string]interface{}
}

// exportToCSV exports data points to CSV file
func exportToCSV(filename string, points []DataPoint) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if len(points) == 0 {
		return nil
	}

	// Build header
	header := []string{"timestamp", "id"}

	// Get field names from first point
	var fieldNames []string
	for fieldName := range points[0].Values {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames) // Ensure consistent ordering

	// Check if data is aggregated (contains stats) or raw (simple values)
	isAggregated := false
	if len(points) > 0 && len(points[0].Values) > 0 {
		// Check first field to determine if it's aggregated
		for _, value := range points[0].Values {
			if _, ok := value.(map[string]interface{}); ok {
				isAggregated = true
				break
			}
		}
	}

	// Add field columns
	if isAggregated {
		// Aggregated data: add statistics columns
		for _, fieldName := range fieldNames {
			header = append(header,
				fieldName+"_count",
				fieldName+"_sum",
				fieldName+"_min",
				fieldName+"_max",
				fieldName+"_avg",
			)
		}
	} else {
		// Raw data: add simple value columns
		header = append(header, fieldNames...)
	}

	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write data rows
	for _, point := range points {
		row := []string{
			point.Timestamp.Format(time.RFC3339),
			point.ID,
		}

		for _, fieldName := range fieldNames {
			if fieldData, ok := point.Values[fieldName]; ok {
				if isAggregated {
					// Handle aggregated data with statistics
					if stats, ok := fieldData.(map[string]interface{}); ok {
						row = append(row,
							fmt.Sprintf("%v", stats["count"]),
							fmt.Sprintf("%v", stats["sum"]),
							fmt.Sprintf("%v", stats["min"]),
							fmt.Sprintf("%v", stats["max"]),
							fmt.Sprintf("%v", stats["avg"]),
						)
					} else {
						row = append(row, "", "", "", "", "")
					}
				} else {
					// Handle raw data with simple values
					row = append(row, fmt.Sprintf("%v", fieldData))
				}
			} else {
				if isAggregated {
					row = append(row, "", "", "", "", "")
				} else {
					row = append(row, "")
				}
			}
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	return nil
}
