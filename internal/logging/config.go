package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"github.com/soltixdb/soltix/internal/config"
)

// NewFromConfig creates a logger from configuration
func NewFromConfig(cfg config.LoggingConfig) (*Logger, error) {
	// Parse level
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}

	// Determine output path
	outputPath := cfg.OutputPath

	// Configure output writer
	var output io.Writer
	switch outputPath {
	case "stdout", "":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		// File output - ensure parent directory exists
		logDir := filepath.Dir(outputPath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
		}

		file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", outputPath, err)
		}
		output = file
	}

	// Configure format
	if cfg.Format == "console" || cfg.Format == "pretty" {
		output = zerolog.ConsoleWriter{
			Out:        output,
			TimeFormat: getTimeFormat(cfg.TimeFormat),
		}
	}

	// Create logger
	zl := zerolog.New(output).
		Level(level).
		With().
		Timestamp().
		Logger()

	return &Logger{zl: zl}, nil
}

// getTimeFormat converts string to time format
func getTimeFormat(format string) string {
	switch format {
	case "RFC3339":
		return time.RFC3339
	case "Unix":
		return time.UnixDate
	case "Kitchen":
		return time.Kitchen
	default:
		return time.RFC3339
	}
}
