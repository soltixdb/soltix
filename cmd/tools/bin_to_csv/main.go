package main

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/soltixdb/soltix/internal/compression"
	"github.com/soltixdb/soltix/internal/logging"
	"github.com/soltixdb/soltix/internal/storage"
	pb "github.com/soltixdb/soltix/proto/storage/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	// Command line flags
	database := flag.String("database", "shirokuma", "Database name")
	collection := flag.String("collection", "plant_power", "Collection name")
	interval := flag.String("interval", "raw", "Aggregation interval (raw, 1h, 1d, 1mo, 1y)")
	date := flag.String("date", "", "Date in yyyymmdd format")
	dataDir := flag.String("data-dir", "./data/data", "Base data directory")
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
		points, err = readAggregateData(*dataDir, *database, *collection, *interval, parsedDate, loc)
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

// readStorageData reads raw data using V3 Columnar storage
func readStorageData(dataDir, database, collection string, date time.Time, timezone *time.Location, deviceFilter string) ([]DataPoint, error) {
	logger := logging.NewDevelopment()

	// Initialize Columnar storage
	columnarStorage := storage.NewStorage(dataDir, compression.Snappy, logger)
	columnarStorage.SetTimezone(timezone)

	// Query for the entire day
	startTime := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, timezone)
	endTime := startTime.Add(24 * time.Hour)

	// Build device filter
	var deviceIDs []string
	if deviceFilter != "" {
		deviceIDs = []string{deviceFilter}
	}

	fmt.Printf("Querying Columnar data: %s/%s from %s to %s\n", database, collection, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

	// Query data
	storagePoints, err := columnarStorage.Query(database, collection, deviceIDs, startTime, endTime, nil)
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
func readAggregateData(dataDir, database, collection, interval string, date time.Time, timezone *time.Location) ([]DataPoint, error) {
	// Construct file path based on interval
	year := date.Format("2006")
	month := date.Format("01")
	yearMonth := date.Format("200601")
	dateStr := date.Format("20060102")

	var filePath string
	switch interval {
	case "1h":
		filePath = filepath.Join(dataDir, "agg_1h", database, collection, year, month, dateStr+".bin")
	case "1d":
		filePath = filepath.Join(dataDir, "agg_1d", database, collection, year, yearMonth+".bin")
	case "1mo", "1m":
		filePath = filepath.Join(dataDir, "agg_1M", database, collection, year+".bin")
	case "1y":
		filePath = filepath.Join(dataDir, "agg_1y", database, collection, "all.bin")
	default:
		return nil, fmt.Errorf("invalid interval '%s'. Valid options: raw, 1h, 1d, 1mo (or 1m), 1y", interval)
	}

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("file not found: %s", filePath)
	}

	fmt.Printf("Reading aggregate file: %s\n", filePath)
	return readAggregateFile(filePath, timezone)
}

// readAggregateFile reads and decompresses an aggregate bin file
func readAggregateFile(filePath string, timezone *time.Location) ([]DataPoint, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Read header (256 bytes)
	headerBytes := make([]byte, 256)
	if _, err := file.Read(headerBytes); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Parse header
	header := parseAggFileHeader(headerBytes)

	// Validate magic (AggFileMagic = 0x53585441 "ATXS")
	if header.Magic != 0x53585441 {
		return nil, fmt.Errorf("invalid magic number: 0x%X (expected 0x53585441)", header.Magic)
	}

	// Read index at the end of file
	if _, err := file.Seek(header.IndexOffset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to index: %w", err)
	}

	// Read index size
	var indexSize uint32
	if err := binary.Read(file, binary.LittleEndian, &indexSize); err != nil {
		return nil, fmt.Errorf("failed to read index size: %w", err)
	}

	// Read compressed index data
	indexData := make([]byte, indexSize)
	if _, err := file.Read(indexData); err != nil {
		return nil, fmt.Errorf("failed to read index: %w", err)
	}

	// Decompress index
	compressor := compression.NewSnappyCompressor()
	decompressedIndex, err := compressor.Decompress(indexData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress index: %w", err)
	}

	// Parse index as JSON
	var fileIndex storage.FileIndex
	if err := json.Unmarshal(decompressedIndex, &fileIndex); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index: %w", err)
	}

	// Read all device blocks
	var allPoints []DataPoint

	for _, entry := range fileIndex.DeviceEntries {
		if _, err := file.Seek(entry.BlockOffset, 0); err != nil {
			return nil, fmt.Errorf("failed to seek to block: %w", err)
		}

		compressed := make([]byte, entry.BlockSize)
		if _, err := file.Read(compressed); err != nil {
			return nil, fmt.Errorf("failed to read block: %w", err)
		}

		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress block: %w", err)
		}

		var aggBlock pb.AggregatedDeviceBlock
		if err := proto.Unmarshal(decompressed, &aggBlock); err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
		}

		for i := 0; i < len(aggBlock.Timestamps); i++ {
			timestamp := time.Unix(0, aggBlock.Timestamps[i]).In(timezone)

			point := DataPoint{
				Timestamp: timestamp,
				ID:        aggBlock.DeviceId,
				Values:    make(map[string]interface{}),
			}

			if i < len(aggBlock.Fields) {
				fieldsEntry := aggBlock.Fields[i]
				for fieldName, fieldValue := range fieldsEntry.Fields {
					fieldStats := make(map[string]interface{})
					fieldStats["count"] = fieldValue.Count
					fieldStats["sum"] = fieldValue.Sum
					fieldStats["min"] = fieldValue.Min
					fieldStats["max"] = fieldValue.Max
					fieldStats["avg"] = fieldValue.Avg
					point.Values[fieldName] = fieldStats
				}
			}

			allPoints = append(allPoints, point)
		}
	}

	return allPoints, nil
}

// AggFileHeader represents the header of an aggregate file
type AggFileHeader struct {
	Magic       uint32
	IndexOffset int64
	IndexSize   uint32
}

func parseAggFileHeader(data []byte) AggFileHeader {
	return AggFileHeader{
		Magic:       binary.LittleEndian.Uint32(data[0:4]),
		IndexOffset: int64(binary.LittleEndian.Uint64(data[36:44])),
		IndexSize:   binary.LittleEndian.Uint32(data[44:48]),
	}
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
