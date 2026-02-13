package logging

import (
	"context"
)

type contextKey string

const (
	loggerKey    contextKey = "logger"
	requestIDKey contextKey = "request_id"
	userIDKey    contextKey = "user_id"
	traceIDKey   contextKey = "trace_id"
)

// WithLogger adds a logger to the context
func WithLogger(ctx context.Context, logger *Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// FromContext extracts the logger from context, falls back to global
func FromContext(ctx context.Context) *Logger {
	if logger, ok := ctx.Value(loggerKey).(*Logger); ok {
		return logger
	}
	return global
}

// WithRequestID adds a request ID to the context
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// WithUserID adds a user ID to the context
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// WithTraceID adds a trace ID to the context
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// extractContextFields extracts logging fields from context
func extractContextFields(ctx context.Context) []interface{} {
	var fields []interface{}

	if requestID, ok := ctx.Value(requestIDKey).(string); ok && requestID != "" {
		k, v := String("request_id", requestID)
		fields = append(fields, k, v)
	}

	if userID, ok := ctx.Value(userIDKey).(string); ok && userID != "" {
		k, v := String("user_id", userID)
		fields = append(fields, k, v)
	}

	if traceID, ok := ctx.Value(traceIDKey).(string); ok && traceID != "" {
		k, v := String("trace_id", traceID)
		fields = append(fields, k, v)
	}

	return fields
}

// ContextLogger provides convenience methods for context-aware logging

// DebugCtx logs a debug message with context
func DebugCtx(ctx context.Context, msg string, fields ...interface{}) {
	FromContext(ctx).WithContext(ctx).Debug(msg, fields...)
}

// InfoCtx logs an info message with context
func InfoCtx(ctx context.Context, msg string, fields ...interface{}) {
	FromContext(ctx).WithContext(ctx).Info(msg, fields...)
}

// WarnCtx logs a warning message with context
func WarnCtx(ctx context.Context, msg string, fields ...interface{}) {
	FromContext(ctx).WithContext(ctx).Warn(msg, fields...)
}

// ErrorCtx logs an error message with context
func ErrorCtx(ctx context.Context, msg string, fields ...interface{}) {
	FromContext(ctx).WithContext(ctx).Error(msg, fields...)
}

// FatalCtx logs a fatal message with context and exits
func FatalCtx(ctx context.Context, msg string, fields ...interface{}) {
	FromContext(ctx).WithContext(ctx).Fatal(msg, fields...)
}
