package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/ThreeDotsLabs/watermill"
)

// Logger defines the interface for structured logging that callers should provide.
// This matches the logger interface from hyperfleet-adapter/pkg/logger.
type Logger interface {
	// Debug logs at debug level
	Debug(ctx context.Context, message string)
	// Debugf logs at debug level with formatting
	Debugf(ctx context.Context, format string, args ...interface{})
	// Info logs at info level
	Info(ctx context.Context, message string)
	// Infof logs at info level with formatting
	Infof(ctx context.Context, format string, args ...interface{})
	// Warn logs at warn level
	Warn(ctx context.Context, message string)
	// Warnf logs at warn level with formatting
	Warnf(ctx context.Context, format string, args ...interface{})
	// Error logs at error level
	Error(ctx context.Context, message string)
	// Errorf logs at error level with formatting
	Errorf(ctx context.Context, format string, args ...interface{})
}

// OutputFormat defines the log output format
type OutputFormat int

const (
	FormatText OutputFormat = iota // Default text format
	FormatJSON                     // JSON format
)

// TestLoggerOption configures the test logger
type TestLoggerOption func(*testLoggerOptions)

type testLoggerOptions struct {
	format OutputFormat
	level  slog.Level
}

// WithFormat sets the output format for the test logger.
func WithFormat(format OutputFormat) TestLoggerOption {
	return func(o *testLoggerOptions) {
		o.format = format
	}
}

// WithLevel sets the minimum log level for the test logger.
func WithLevel(level slog.Level) TestLoggerOption {
	return func(o *testLoggerOptions) {
		o.level = level
	}
}

// testLogger provides a simple logger implementation using slog with HyperFleet format
type testLogger struct {
	logger *slog.Logger
}

// NewTestLogger creates a logger for testing and examples.
// Production applications should implement the Logger interface with their own logging infrastructure.
// Defaults to text format at INFO level. Use WithFormat and WithLevel to customize.
func NewTestLogger(opts ...TestLoggerOption) Logger {
	options := &testLoggerOptions{
		format: FormatText,
		level:  slog.LevelInfo,
	}
	for _, opt := range opts {
		opt(options)
	}

	var handler slog.Handler

	switch options.format {
	case FormatJSON:
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: options.level,
		})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: options.level,
		})
	}

	return &testLogger{
		logger: slog.New(handler),
	}
}

func (l *testLogger) Debug(ctx context.Context, message string) {
	l.logger.DebugContext(ctx, message)
}

func (l *testLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	l.logger.DebugContext(ctx, fmt.Sprintf(format, args...))
}

func (l *testLogger) Info(ctx context.Context, message string) {
	l.logger.InfoContext(ctx, message)
}

func (l *testLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	l.logger.InfoContext(ctx, fmt.Sprintf(format, args...))
}

func (l *testLogger) Warn(ctx context.Context, message string) {
	l.logger.WarnContext(ctx, message)
}

func (l *testLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	l.logger.WarnContext(ctx, fmt.Sprintf(format, args...))
}

func (l *testLogger) Error(ctx context.Context, message string) {
	l.logger.ErrorContext(ctx, message)
}

func (l *testLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	l.logger.ErrorContext(ctx, fmt.Sprintf(format, args...))
}

// WatermillLoggerAdapter adapts the caller's Logger interface to Watermill's LoggerAdapter interface.
// It stores the context from broker operations and uses it when Watermill calls logging methods.
type WatermillLoggerAdapter struct {
	logger Logger
	ctx    context.Context
	fields watermill.LogFields // Bound fields from With() calls
}

// NewWatermillLoggerAdapter creates a new adapter that converts the caller's Logger
// to Watermill's LoggerAdapter interface, preserving the provided context.
// The context is immutable after construction to ensure thread safety.
func NewWatermillLoggerAdapter(logger Logger, ctx context.Context) *WatermillLoggerAdapter {
	return &WatermillLoggerAdapter{
		logger: logger,
		ctx:    ctx,
	}
}

// getSlogLogger returns the underlying slog.Logger and converted attributes if the
// adapter wraps a testLogger and there are fields to log. Returns nil, nil otherwise.
func (w *WatermillLoggerAdapter) getSlogLogger(fields watermill.LogFields) (*slog.Logger, []any) {
	allFields := mergeFields(w.fields, fields)
	if dl, ok := w.logger.(*testLogger); ok && len(allFields) > 0 {
		attrs := make([]any, 0, len(allFields)*2)
		for k, v := range allFields {
			attrs = append(attrs, k, v)
		}
		return dl.logger, attrs
	}
	return nil, nil
}

// Error logs an error message with optional fields.
// Implements watermill.LoggerAdapter interface.
func (w *WatermillLoggerAdapter) Error(msg string, err error, fields watermill.LogFields) {
	errorMsg := msg
	if err != nil {
		errorMsg = fmt.Sprintf("%s: %v", msg, err)
	}

	if slogger, attrs := w.getSlogLogger(fields); slogger != nil {
		slogger.ErrorContext(w.ctx, errorMsg, attrs...)
		return
	}
	w.logger.Error(w.ctx, errorMsg)
}

// Info logs an info message with optional fields.
// Implements watermill.LoggerAdapter interface.
func (w *WatermillLoggerAdapter) Info(msg string, fields watermill.LogFields) {
	if slogger, attrs := w.getSlogLogger(fields); slogger != nil {
		slogger.InfoContext(w.ctx, msg, attrs...)
		return
	}
	w.logger.Info(w.ctx, msg)
}

// Debug logs a debug message with optional fields.
// Implements watermill.LoggerAdapter interface.
func (w *WatermillLoggerAdapter) Debug(msg string, fields watermill.LogFields) {
	if slogger, attrs := w.getSlogLogger(fields); slogger != nil {
		slogger.DebugContext(w.ctx, msg, attrs...)
		return
	}
	w.logger.Debug(w.ctx, msg)
}

// Trace logs a trace message with optional fields.
// Implements watermill.LoggerAdapter interface.
// Note: Since the caller's Logger interface doesn't have Trace level,
// we map this to Debug level.
func (w *WatermillLoggerAdapter) Trace(msg string, fields watermill.LogFields) {
	if slogger, attrs := w.getSlogLogger(fields); slogger != nil {
		slogger.DebugContext(w.ctx, msg, attrs...)
		return
	}
	w.logger.Debug(w.ctx, msg)
}

// mergeFields merges two LogFields maps, with overrideFields taking precedence on key collision.
// Returns a new map without modifying either input.
func mergeFields(baseFields, overrideFields watermill.LogFields) watermill.LogFields {
	if len(baseFields) == 0 && len(overrideFields) == 0 {
		return nil
	}
	merged := make(watermill.LogFields, len(baseFields)+len(overrideFields))
	for k, v := range baseFields {
		merged[k] = v
	}
	for k, v := range overrideFields {
		merged[k] = v
	}
	return merged
}

// With creates a new logger adapter with additional fields.
// Implements watermill.LoggerAdapter interface.
// Returns a new adapter that preserves and merges the incoming fields with existing fields.
// Incoming fields override existing fields on key collision.
// The original adapter is not modified, ensuring concurrency safety.
func (w *WatermillLoggerAdapter) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &WatermillLoggerAdapter{
		logger: w.logger,
		ctx:    w.ctx,
		fields: mergeFields(w.fields, fields),
	}
}
