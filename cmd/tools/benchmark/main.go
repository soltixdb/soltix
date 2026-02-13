package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	url2 "net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// BenchmarkConfig holds benchmark configuration
type BenchmarkConfig struct {
	BaseURL       string
	Database      string
	Collection    string
	NumDevices    int
	NumFields     int
	Duration      time.Duration
	WriteWorkers  int
	QueryWorkers  int
	BatchSize     int
	QueryInterval time.Duration
	SkipSetup     bool
	DataTimeRange time.Duration // How far back in time to spread data
	APIKey        string
	HTTPClient    *http.Client // Shared HTTP client for connection pooling
}

// Metrics holds benchmark metrics
type Metrics struct {
	WriteLatencies  []float64
	QueryLatencies  []float64
	WriteErrors     int64
	QueryErrors     int64
	WriteSuccess    int64
	QuerySuccess    int64
	TotalWrites     int64
	TotalQueries    int64
	FirstWriteError string
	FirstQueryError string
	mu              sync.Mutex
}

// Result represents benchmark results
type Result struct {
	Operation  string
	TotalOps   int64
	SuccessOps int64
	ErrorOps   int64
	Duration   time.Duration
	Throughput float64 // ops/sec
	AvgLatency float64 // ms
	MinLatency float64 // ms
	MaxLatency float64 // ms
	P50Latency float64 // ms
	P95Latency float64 // ms
	P99Latency float64 // ms
	ErrorMsg   string  // First error message
}

func main() {
	// Parse flags
	config := BenchmarkConfig{}
	flag.StringVar(&config.BaseURL, "url", "http://127.0.0.1:5555", "Base URL of the API")
	flag.StringVar(&config.Database, "db", "bench_db", "Database name")
	flag.StringVar(&config.Collection, "coll", "bench_coll", "Collection name")
	flag.IntVar(&config.NumDevices, "devices", 100, "Number of devices")
	flag.IntVar(&config.NumFields, "fields", 10, "Number of fields per record")
	flag.DurationVar(&config.Duration, "duration", 60*time.Second, "Benchmark duration")
	flag.IntVar(&config.WriteWorkers, "write-workers", 10, "Number of concurrent write workers")
	flag.IntVar(&config.QueryWorkers, "query-workers", 5, "Number of concurrent query workers")
	flag.IntVar(&config.BatchSize, "batch-size", 1, "Batch write size")
	flag.DurationVar(&config.QueryInterval, "query-interval", 10*time.Millisecond, "Interval between queries per worker")
	flag.DurationVar(&config.DataTimeRange, "time-range", 24*time.Hour, "Time range to spread data across")
	flag.StringVar(&config.APIKey, "api-key", "", "API key for authentication")
	flag.BoolVar(&config.SkipSetup, "skip-setup", false, "Skip database/collection creation")
	flag.Parse()
	// Create shared HTTP client with connection pooling
	config.HTTPClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	fmt.Printf("=== Soltix Benchmark Tool ===\n")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  URL: %s\n", config.BaseURL)
	fmt.Printf("  Database: %s\n", config.Database)
	fmt.Printf("  Collection: %s\n", config.Collection)
	fmt.Printf("  Devices: %d\n", config.NumDevices)
	fmt.Printf("  Fields: %d\n", config.NumFields)
	fmt.Printf("  Duration: %s\n", config.Duration)
	fmt.Printf("  Write Workers: %d\n", config.WriteWorkers)
	fmt.Printf("  Query Workers: %d\n", config.QueryWorkers)
	fmt.Printf("  Batch Size: %d\n", config.BatchSize)
	fmt.Printf("  Query Interval: %s\n", config.QueryInterval)
	fmt.Printf("  Data Time Range: %s\n", config.DataTimeRange)
	fmt.Printf("\n")

	// Create database and collection (unless skipped)
	if !config.SkipSetup {
		if err := setupDatabaseAndCollection(config); err != nil {
			fmt.Printf("Warning: Failed to setup database/collection: %v\n", err)
			fmt.Printf("Continuing with existing database/collection...\n")
		}
	} else {
		fmt.Printf("Skipping database/collection setup (using existing)\n")
	}

	// Run benchmark
	metrics := runBenchmark(config)

	// Calculate and display results
	writeResult := calculateResult("Write", metrics.WriteLatencies, metrics.WriteSuccess, metrics.WriteErrors, config.Duration, metrics.FirstWriteError)
	queryResult := calculateResult("Query", metrics.QueryLatencies, metrics.QuerySuccess, metrics.QueryErrors, config.Duration, metrics.FirstQueryError)

	fmt.Printf("\n=== Benchmark Results ===\n\n")
	displayResult(writeResult)
	fmt.Println()
	displayResult(queryResult)

	// Save results to file
	saveResults(config, writeResult, queryResult)
}

func setupDatabaseAndCollection(config BenchmarkConfig) error {
	// Create database (ignore if already exists)
	dbURL := fmt.Sprintf("%s/v1/databases", config.BaseURL)
	dbData := map[string]interface{}{
		"name":        config.Database,
		"description": "Benchmark database",
		"timezone":    "Asia/Tokyo",
	}
	if err := makeRequest(config, "POST", dbURL, dbData); err != nil {
		// Ignore if database already exists
		if !isConflictError(err) {
			return fmt.Errorf("create database: %w", err)
		}
		fmt.Printf("Database '%s' already exists, using existing one\n", config.Database)
	} else {
		fmt.Printf("Created database '%s'\n", config.Database)
	}

	// Create collection (ignore if already exists)
	collURL := fmt.Sprintf("%s/v1/databases/%s/collections", config.BaseURL, config.Database)
	collData := map[string]interface{}{
		"name":        config.Collection,
		"description": "Benchmark collection",
		"fields": []map[string]interface{}{
			{
				"name":     "time",
				"type":     "timestamp",
				"required": true,
			},
			{
				"name":     "id",
				"type":     "string",
				"required": true,
			},
		},
	}
	if err := makeRequest(config, "POST", collURL, collData); err != nil {
		// Ignore if collection already exists
		if !isConflictError(err) {
			return fmt.Errorf("create collection: %w", err)
		}
		fmt.Printf("Collection '%s' already exists, using existing one\n", config.Collection)
	} else {
		fmt.Printf("Created collection '%s'\n", config.Collection)
	}

	fmt.Printf("Database and collection setup completed\n")
	return nil
}

// isConflictError checks if error is a 409 conflict
func isConflictError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return len(errStr) >= 8 && errStr[:8] == "HTTP 409"
}

func runBenchmark(config BenchmarkConfig) *Metrics {
	metrics := &Metrics{
		WriteLatencies: make([]float64, 0, 10000),
		QueryLatencies: make([]float64, 0, 1000),
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	startTime := time.Now()

	// Start write workers
	for i := 0; i < config.WriteWorkers; i++ {
		wg.Add(1)
		go writeWorker(i, config, metrics, stopCh, &wg)
	}

	// Start query workers
	for i := 0; i < config.QueryWorkers; i++ {
		wg.Add(1)
		go queryWorker(i, config, metrics, stopCh, &wg)
	}

	// Progress reporter
	go progressReporter(metrics, config.Duration, startTime)

	// Wait for duration
	time.Sleep(config.Duration)
	close(stopCh)
	wg.Wait()

	return metrics
}

func writeWorker(id int, config BenchmarkConfig, metrics *Metrics, stopCh chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	baseTime := time.Now().Add(-config.DataTimeRange) // Start from past
	deviceID := id % config.NumDevices
	counter := 0

	for {
		select {
		case <-stopCh:
			return
		default:
			// Collect batch of points
			var points []map[string]interface{}
			for i := 0; i < config.BatchSize; i++ {
				timestamp := baseTime.Add(time.Duration(counter) * time.Second)

				// Create data point
				point := map[string]interface{}{
					"time": timestamp.Format(time.RFC3339Nano),
					"id":   fmt.Sprintf("device-%04d", deviceID),
				}

				// Add dynamic fields
				fields := generateFields(config.NumFields)
				for k, v := range fields {
					point[k] = v
				}

				points = append(points, point)
				counter++
				deviceID = (deviceID + 1) % config.NumDevices
			}

			// Send write request (batch or single)
			var url string
			var payload interface{}
			if config.BatchSize > 1 {
				// Use batch API
				url = fmt.Sprintf("%s/v1/databases/%s/collections/%s/write/batch",
					config.BaseURL, config.Database, config.Collection)
				payload = map[string]interface{}{"points": points}
			} else {
				// Use single write API
				url = fmt.Sprintf("%s/v1/databases/%s/collections/%s/write",
					config.BaseURL, config.Database, config.Collection)
				payload = points[0]
			}

			start := time.Now()
			err := makeRequest(config, "POST", url, payload)
			latency := time.Since(start).Seconds() * 1000 // ms

			metrics.mu.Lock()
			metrics.WriteLatencies = append(metrics.WriteLatencies, latency)
			metrics.mu.Unlock()

			if err != nil {
				atomic.AddInt64(&metrics.WriteErrors, 1)
				metrics.mu.Lock()
				if metrics.FirstWriteError == "" {
					metrics.FirstWriteError = err.Error()
				}
				metrics.mu.Unlock()
			} else {
				// Count successful points, not requests
				atomic.AddInt64(&metrics.WriteSuccess, int64(config.BatchSize))
			}
			atomic.AddInt64(&metrics.TotalWrites, int64(config.BatchSize))
		}
	}
}

func queryWorker(id int, config BenchmarkConfig, metrics *Metrics, stopCh chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(config.QueryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			// Query recent data
			endTime := time.Now()
			startTime := endTime.Add(-5 * time.Minute)

			url := fmt.Sprintf("%s/v1/databases/%s/collections/%s/query?start_time=%s&end_time=%s",
				config.BaseURL, config.Database, config.Collection,
				url2.QueryEscape(startTime.Format(time.RFC3339)),
				url2.QueryEscape(endTime.Format(time.RFC3339)))

			start := time.Now()
			err := makeRequest(config, "GET", url, nil)
			latency := time.Since(start).Seconds() * 1000 // ms

			metrics.mu.Lock()
			metrics.QueryLatencies = append(metrics.QueryLatencies, latency)
			metrics.mu.Unlock()

			if err != nil {
				atomic.AddInt64(&metrics.QueryErrors, 1)
				metrics.mu.Lock()
				if metrics.FirstQueryError == "" {
					metrics.FirstQueryError = err.Error()
				}
				metrics.mu.Unlock()
			} else {
				atomic.AddInt64(&metrics.QuerySuccess, 1)
			}
			atomic.AddInt64(&metrics.TotalQueries, 1)
		}
	}
}

func progressReporter(metrics *Metrics, duration time.Duration, startTime time.Time) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		elapsed := time.Since(startTime)
		if elapsed >= duration {
			return
		}

		writes := atomic.LoadInt64(&metrics.WriteSuccess)
		queries := atomic.LoadInt64(&metrics.QuerySuccess)
		writeErrors := atomic.LoadInt64(&metrics.WriteErrors)
		queryErrors := atomic.LoadInt64(&metrics.QueryErrors)

		writeThroughput := float64(writes) / elapsed.Seconds()
		queryThroughput := float64(queries) / elapsed.Seconds()

		remaining := duration - elapsed
		fmt.Printf("[%s remaining] Writes: %d (%.0f/s, %d errors) | Queries: %d (%.0f/s, %d errors)\n",
			remaining.Round(time.Second), writes, writeThroughput, writeErrors,
			queries, queryThroughput, queryErrors)
	}
}

func makeRequest(config BenchmarkConfig, method, url string, data interface{}) error {
	var body io.Reader
	if data != nil {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}
		body = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "keep-alive")
	if config.APIKey != "" {
		req.Header.Set("X-API-Key", config.APIKey)
	}

	resp, err := config.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	// Read and discard body to reuse connection
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return nil
}

func generateFields(numFields int) map[string]interface{} {
	fields := make(map[string]interface{})
	for i := 0; i < numFields; i++ {
		fields[fmt.Sprintf("field_%d", i)] = rand.Float64() * 10000000
	}
	return fields
}

func calculateResult(operation string, latencies []float64, success, errors int64, duration time.Duration, errorMsg string) Result {
	if len(latencies) == 0 {
		return Result{
			Operation: operation,
			TotalOps:  success + errors,
			ErrorMsg:  errorMsg,
		}
	}

	// Sort for percentiles
	sort.Float64s(latencies)

	result := Result{
		Operation:  operation,
		TotalOps:   success + errors,
		SuccessOps: success,
		ErrorOps:   errors,
		Duration:   duration,
		Throughput: float64(success) / duration.Seconds(),
		MinLatency: latencies[0],
		MaxLatency: latencies[len(latencies)-1],
		P50Latency: percentile(latencies, 50),
		P95Latency: percentile(latencies, 95),
		P99Latency: percentile(latencies, 99),
		ErrorMsg:   errorMsg,
	}

	// Calculate average
	var sum float64
	for _, lat := range latencies {
		sum += lat
	}
	result.AvgLatency = sum / float64(len(latencies))

	return result
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(sorted)) * p / 100.0))
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func displayResult(r Result) {
	fmt.Printf("=== %s Operations ===\n", r.Operation)
	fmt.Printf("Total Operations: %d\n", r.TotalOps)
	fmt.Printf("Success:          %d (%.2f%%)\n", r.SuccessOps, float64(r.SuccessOps)/float64(r.TotalOps)*100)
	fmt.Printf("Errors:           %d (%.2f%%)\n", r.ErrorOps, float64(r.ErrorOps)/float64(r.TotalOps)*100)
	fmt.Printf("Duration:         %s\n", r.Duration)
	fmt.Printf("Throughput:       %.2f ops/sec\n", r.Throughput)
	if r.ErrorOps > 0 && len(r.ErrorMsg) > 0 {
		fmt.Printf("First Error:      %s\n", r.ErrorMsg)
	}
	fmt.Printf("\nLatency (ms):\n")
	fmt.Printf("  Min:  %.2f\n", r.MinLatency)
	fmt.Printf("  Avg:  %.2f\n", r.AvgLatency)
	fmt.Printf("  P50:  %.2f\n", r.P50Latency)
	fmt.Printf("  P95:  %.2f\n", r.P95Latency)
	fmt.Printf("  P99:  %.2f\n", r.P99Latency)
	fmt.Printf("  Max:  %.2f\n", r.MaxLatency)
}

func saveResults(config BenchmarkConfig, writeResult, queryResult Result) {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("benchmark_results/api_benchmark_%s.txt", timestamp)

	f, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Failed to create result file: %v\n", err)
		return
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, "=== Soltix API Benchmark Results ===\n")
	_, _ = fmt.Fprintf(f, "Date: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))
	_, _ = fmt.Fprintf(f, "Configuration:\n")
	_, _ = fmt.Fprintf(f, "  URL: %s\n", config.BaseURL)
	_, _ = fmt.Fprintf(f, "  Database: %s\n", config.Database)
	_, _ = fmt.Fprintf(f, "  Collection: %s\n", config.Collection)
	_, _ = fmt.Fprintf(f, "  Devices: %d\n", config.NumDevices)
	_, _ = fmt.Fprintf(f, "  Fields: %d\n", config.NumFields)
	_, _ = fmt.Fprintf(f, "  Duration: %s\n", config.Duration)
	_, _ = fmt.Fprintf(f, "  Write Workers: %d\n", config.WriteWorkers)
	_, _ = fmt.Fprintf(f, "  Query Workers: %d\n", config.QueryWorkers)
	_, _ = fmt.Fprintf(f, "  Batch Size: %d\n", config.BatchSize)
	_, _ = fmt.Fprintf(f, "\n")

	writeResultToFile(f, "Write", writeResult)
	_, _ = fmt.Fprintf(f, "\n")
	writeResultToFile(f, "Query", queryResult)

	fmt.Printf("\nResults saved to: %s\n", filename)
}

func writeResultToFile(f *os.File, name string, r Result) {
	_, _ = fmt.Fprintf(f, "=== %s Operations ===\n", name)
	_, _ = fmt.Fprintf(f, "Total Operations: %d\n", r.TotalOps)
	_, _ = fmt.Fprintf(f, "Success:          %d (%.2f%%)\n", r.SuccessOps, float64(r.SuccessOps)/float64(r.TotalOps)*100)
	_, _ = fmt.Fprintf(f, "Errors:           %d (%.2f%%)\n", r.ErrorOps, float64(r.ErrorOps)/float64(r.TotalOps)*100)
	_, _ = fmt.Fprintf(f, "Duration:         %s\n", r.Duration)
	_, _ = fmt.Fprintf(f, "Throughput:       %.2f ops/sec\n", r.Throughput)
	_, _ = fmt.Fprintf(f, "\nLatency (ms):\n")
	_, _ = fmt.Fprintf(f, "  Min:  %.2f\n", r.MinLatency)
	_, _ = fmt.Fprintf(f, "  Avg:  %.2f\n", r.AvgLatency)
	_, _ = fmt.Fprintf(f, "  P50:  %.2f\n", r.P50Latency)
	_, _ = fmt.Fprintf(f, "  P95:  %.2f\n", r.P95Latency)
	_, _ = fmt.Fprintf(f, "  P99:  %.2f\n", r.P99Latency)
	_, _ = fmt.Fprintf(f, "  Max:  %.2f\n", r.MaxLatency)
}
