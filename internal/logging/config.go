package logging

import (
	"io"
	"os"
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

	// Configure output writer
	var output io.Writer
	switch cfg.OutputPath {
	case "stdout", "":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		// File output
		file, err := os.OpenFile(cfg.OutputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
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
