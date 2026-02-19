package logging

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Logger wraps zerolog.Logger with convenience methods
type Logger struct {
	zl     zerolog.Logger
	fields map[string]interface{} // Store fields for With()
}

var (
	// Global logger instance
	global *Logger
)

func init() {
	// Initialize with default development logger
	logger := NewDevelopment()
	global = logger
}

// NewProduction creates a production logger with JSON output
func NewProduction() *Logger {
	zl := zerolog.New(os.Stdout).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Logger()

	return &Logger{
		zl:     zl,
		fields: make(map[string]interface{}),
	}
}

// NewDevelopment creates a development logger with pretty console output
func NewDevelopment() *Logger {
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	zl := zerolog.New(output).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Logger()

	return &Logger{
		zl:     zl,
		fields: make(map[string]interface{}),
	}
}

// NewWithWriter creates a logger with custom writer
func NewWithWriter(w io.Writer, level zerolog.Level) *Logger {
	zl := zerolog.New(w).
		Level(level).
		With().
		Timestamp().
		Logger()

	return &Logger{
		zl:     zl,
		fields: make(map[string]interface{}),
	}
}

// SetGlobal sets the global logger instance
func SetGlobal(logger *Logger) {
	global = logger
}

// Global returns the global logger instance
func Global() *Logger {
	return global
}

// applyStoredFields applies stored fields to an event
func (l *Logger) applyStoredFields(e *zerolog.Event) {
	for k, v := range l.fields {
		e.Interface(k, v)
	}
}

// Debug logs a debug message
func (l *Logger) Debug(msg string, fields ...interface{}) {
	e := l.zl.Debug()
	l.applyStoredFields(e)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			e.Interface(fields[i].(string), fields[i+1])
		}
	}
	e.Msg(msg)
}

// Info logs an info message
func (l *Logger) Info(msg string, fields ...interface{}) {
	e := l.zl.Info()
	l.applyStoredFields(e)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			e.Interface(fields[i].(string), fields[i+1])
		}
	}
	e.Msg(msg)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string, fields ...interface{}) {
	e := l.zl.Warn()
	l.applyStoredFields(e)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fields[i].(string)
			value := fields[i+1]
			// Special handling for error type
			if key == "error" {
				if err, ok := value.(error); ok {
					e.Str("error", err.Error())
				} else {
					e.Interface(key, value)
				}
			} else {
				e.Interface(key, value)
			}
		}
	}
	e.Msg(msg)
}

// Error logs an error message
func (l *Logger) Error(msg string, fields ...interface{}) {
	e := l.zl.Error()
	l.applyStoredFields(e)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fields[i].(string)
			value := fields[i+1]
			// Special handling for error type
			if key == "error" {
				if err, ok := value.(error); ok {
					e.Str("error", err.Error())
				} else {
					e.Interface(key, value)
				}
			} else {
				e.Interface(key, value)
			}
		}
	}
	e.Msg(msg)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(msg string, fields ...interface{}) {
	e := l.zl.Fatal()
	l.applyStoredFields(e)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fields[i].(string)
			value := fields[i+1]
			// Special handling for error type to ensure it's logged correctly
			if key == "error" {
				if err, ok := value.(error); ok {
					e.Str("error", err.Error())
				} else {
					e.Interface(key, value)
				}
			} else {
				e.Interface(key, value)
			}
		}
	}
	e.Msg(msg)
}

// Panic logs a panic message and panics
func (l *Logger) Panic(msg string, fields ...interface{}) {
	e := l.zl.Panic()
	l.applyStoredFields(e)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			e.Interface(fields[i].(string), fields[i+1])
		}
	}
	e.Msg(msg)
}

// With creates a child logger with additional fields
func (l *Logger) With(fields ...interface{}) *Logger {
	newFields := make(map[string]interface{})

	// Copy existing fields
	for k, v := range l.fields {
		newFields[k] = v
	}

	// Add new fields (key-value pairs)
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			newFields[fields[i].(string)] = fields[i+1]
		}
	}

	return &Logger{
		zl:     l.zl,
		fields: newFields,
	}
}

// WithContext returns a logger with context fields
func (l *Logger) WithContext(ctx context.Context) *Logger {
	fields := extractContextFields(ctx)
	if len(fields) == 0 {
		return l
	}
	return l.With(fields...)
}

// Sync flushes any buffered log entries (no-op for zerolog)
func (l *Logger) Sync() error {
	// Zerolog writes directly, no buffering
	return nil
}

// Global convenience functions

// Debug logs a debug message using global logger
func Debug(msg string, fields ...interface{}) {
	global.Debug(msg, fields...)
}

// Info logs an info message using global logger
func Info(msg string, fields ...interface{}) {
	global.Info(msg, fields...)
}

// Warn logs a warning message using global logger
func Warn(msg string, fields ...interface{}) {
	global.Warn(msg, fields...)
}

// Error logs an error message using global logger
func Error(msg string, fields ...interface{}) {
	global.Error(msg, fields...)
}

// Fatal logs a fatal message and exits using global logger
func Fatal(msg string, fields ...interface{}) {
	global.Fatal(msg, fields...)
}

// With creates a child logger with additional fields using global logger
func With(fields ...interface{}) *Logger {
	return global.With(fields...)
}

// Sync flushes the global logger
func Sync() error {
	return global.Sync()
}

// Common field constructors - return key-value pairs

// String creates a string field (returns key, value)
func String(key, val string) (string, interface{}) {
	return key, val
}

// Int creates an int field
func Int(key string, val int) (string, interface{}) {
	return key, val
}

// Int64 creates an int64 field
func Int64(key string, val int64) (string, interface{}) {
	return key, val
}

// Float64 creates a float64 field
func Float64(key string, val float64) (string, interface{}) {
	return key, val
}

// Bool creates a bool field
func Bool(key string, val bool) (string, interface{}) {
	return key, val
}

// Err creates an error field
func Err(err error) (string, interface{}) {
	return "error", err
}

// Duration creates a duration field
func Duration(key string, val time.Duration) (string, interface{}) {
	return key, val
}

// Any creates a field with any type
func Any(key string, val interface{}) (string, interface{}) {
	return key, val
}

// Namespace creates a namespace for fields (returns prefix)
func Namespace(key string) (string, interface{}) {
	return key + ".", nil
}
