package watermilladapter

import (
	"context"
	"log/slog"
	"maps"

	"github.com/ThreeDotsLabs/watermill"
)

// Adapter bridges *slog.Logger to Watermill's LoggerAdapter interface.
type Adapter struct {
	logger *slog.Logger
	ctx    context.Context
	fields watermill.LogFields
}

// New creates an Adapter that forwards Watermill log calls to the given slog.Logger.
func New(logger *slog.Logger, ctx context.Context) *Adapter {
	return &Adapter{
		logger: logger,
		ctx:    ctx,
	}
}

func fieldsToAttrs(fields watermill.LogFields) []any {
	if len(fields) == 0 {
		return nil
	}
	attrs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		attrs = append(attrs, k, v)
	}
	return attrs
}

// allAttrs merges stored With() fields with per-call fields.
// Per-call fields override stored fields on key collision.
func (a *Adapter) allAttrs(callFields watermill.LogFields) []any {
	if len(a.fields) == 0 {
		return fieldsToAttrs(callFields)
	}
	if len(callFields) == 0 {
		return fieldsToAttrs(a.fields)
	}
	merged := make(watermill.LogFields, len(a.fields)+len(callFields))
	maps.Copy(merged, a.fields)
	maps.Copy(merged, callFields)
	return fieldsToAttrs(merged)
}

func (a *Adapter) Error(msg string, err error, fields watermill.LogFields) {
	attrs := a.allAttrs(fields)
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	a.logger.ErrorContext(a.ctx, msg, attrs...)
}

func (a *Adapter) Info(msg string, fields watermill.LogFields) {
	a.logger.InfoContext(a.ctx, msg, a.allAttrs(fields)...)
}

func (a *Adapter) Debug(msg string, fields watermill.LogFields) {
	a.logger.DebugContext(a.ctx, msg, a.allAttrs(fields)...)
}

// Trace maps to Debug since slog has no Trace level.
func (a *Adapter) Trace(msg string, fields watermill.LogFields) {
	a.logger.DebugContext(a.ctx, msg, a.allAttrs(fields)...)
}

// With returns a new Adapter with fields merged (new override old).
func (a *Adapter) With(fields watermill.LogFields) watermill.LoggerAdapter {
	merged := make(watermill.LogFields, len(a.fields)+len(fields))
	maps.Copy(merged, a.fields)
	maps.Copy(merged, fields)
	return &Adapter{
		logger: a.logger,
		ctx:    a.ctx,
		fields: merged,
	}
}
