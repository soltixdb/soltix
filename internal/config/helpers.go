package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// EnsureDirectories ensures all required directories exist
func (c *Config) EnsureDirectories() error {
	dirs := []string{
		c.Storage.DataDir,
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	return nil
}

// GetDataPath returns the full path for a data file
func (c *Config) GetDataPath(filename string) string {
	return filepath.Join(c.Storage.DataDir, filename)
}

// IsDevelopment returns true if running in development mode
func (c *Config) IsDevelopment() bool {
	return c.Logging.Level == "debug" && c.Logging.Format == "console"
}

// IsProduction returns true if running in production mode
func (c *Config) IsProduction() bool {
	return c.Logging.Level == "info" && c.Logging.Format == "json"
}

// GetServerAddress returns the HTTP server address
func (c *Config) GetServerAddress() string {
	return ":" + string(rune(c.Server.HTTPPort))
}

// GetGRPCAddress returns the gRPC server address
func (c *Config) GetGRPCAddress() string {
	return ":" + string(rune(c.Server.GRPCPort))
}

// GetStorageTimezone returns the configured timezone for storage
// Returns UTC if not configured or invalid
// Supports formats:
//   - IANA timezone names: "Asia/Tokyo", "America/New_York", "UTC"
//   - Offset format: "+09:00", "-05:00", "+00:00"
func (c *StorageConfig) GetStorageTimezone() *time.Location {
	if c.Timezone == "" {
		return time.UTC
	}

	// Try parsing as IANA timezone name first
	loc, err := time.LoadLocation(c.Timezone)
	if err == nil {
		return loc
	}

	// Try parsing as offset format (+09:00, -05:00, etc.)
	loc, err = parseOffsetTimezone(c.Timezone)
	if err == nil {
		return loc
	}

	// Default to UTC if parsing fails
	return time.UTC
}

// parseOffsetTimezone parses timezone offset format like "+09:00", "-05:00"
func parseOffsetTimezone(offset string) (*time.Location, error) {
	// Match patterns like +09:00, -05:00, +00:00
	re := regexp.MustCompile(`^([+-])(\d{2}):(\d{2})$`)
	matches := re.FindStringSubmatch(offset)
	if len(matches) != 4 {
		return nil, fmt.Errorf("invalid offset format: %s", offset)
	}

	sign := 1
	if matches[1] == "-" {
		sign = -1
	}

	hours, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, fmt.Errorf("invalid hours: %s", matches[2])
	}

	minutes, err := strconv.Atoi(matches[3])
	if err != nil {
		return nil, fmt.Errorf("invalid minutes: %s", matches[3])
	}

	offsetSeconds := sign * (hours*3600 + minutes*60)
	return time.FixedZone(offset, offsetSeconds), nil
}

// ConvertToStorageTimezone converts a time to the storage timezone
func (c *StorageConfig) ConvertToStorageTimezone(t time.Time) time.Time {
	return t.In(c.GetStorageTimezone())
}

// GetDateInStorageTimezone returns the date string (YYYYMMDD) for the given time in storage timezone
func (c *StorageConfig) GetDateInStorageTimezone(t time.Time) string {
	return c.ConvertToStorageTimezone(t).Format("20060102")
}

// GetMonthInStorageTimezone returns the month string (YYYYMM) for the given time in storage timezone
func (c *StorageConfig) GetMonthInStorageTimezone(t time.Time) string {
	return c.ConvertToStorageTimezone(t).Format("200601")
}

// GetYearInStorageTimezone returns the year string (YYYY) for the given time in storage timezone
func (c *StorageConfig) GetYearInStorageTimezone(t time.Time) string {
	return c.ConvertToStorageTimezone(t).Format("2006")
}
