package watermilladapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
)

// logRecord is the JSON structure emitted by slog.JSONHandler.
type logRecord struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
	// Remaining fields (watermill fields, etc.) are captured here.
	Extra map[string]any `json:"-"`
}

// parseLogRecord decodes a single JSON log line from buf and returns the
// parsed record with all extra keys available in Extra.
func parseLogRecord(t *testing.T, buf *bytes.Buffer) logRecord {
	t.Helper()

	raw := buf.Bytes()
	if len(raw) == 0 {
		t.Fatal("expected log output but buffer is empty")
	}

	var rec logRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("failed to parse log JSON: %v\nraw: %s", err, raw)
	}

	var all map[string]any
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatalf("failed to parse log JSON (map pass): %v", err)
	}
	delete(all, "time")
	delete(all, "level")
	delete(all, "msg")
	rec.Extra = all

	return rec
}

func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

func TestAdapter_InfoLogsAtInfoLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)
	adapter.Info("test message", nil)

	rec := parseLogRecord(t, &buf)

	if rec.Level != "INFO" {
		t.Errorf("expected level INFO, got %q", rec.Level)
	}
	if rec.Message != "test message" {
		t.Errorf("expected message %q, got %q", "test message", rec.Message)
	}
}

func TestAdapter_InfoIncludesWatermillFieldsAsSlogAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)
	adapter.Info("routed", watermill.LogFields{
		"topic":           "orders",
		"subscription_id": "sub-1",
	})

	rec := parseLogRecord(t, &buf)

	topic, ok := rec.Extra["topic"]
	if !ok {
		t.Fatal("expected 'topic' field in log output, not found")
	}
	if topic != "orders" {
		t.Errorf("expected topic=%q, got %v", "orders", topic)
	}

	subID, ok := rec.Extra["subscription_id"]
	if !ok {
		t.Fatal("expected 'subscription_id' field in log output, not found")
	}
	if subID != "sub-1" {
		t.Errorf("expected subscription_id=%q, got %v", "sub-1", subID)
	}
}

func TestAdapter_ErrorLogsErrAsStructuredAttr(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)
	testErr := errors.New("connection refused")
	adapter.Error("publish failed", testErr, nil)

	rec := parseLogRecord(t, &buf)

	if rec.Level != "ERROR" {
		t.Errorf("expected level ERROR, got %q", rec.Level)
	}
	if rec.Message != "publish failed" {
		t.Errorf("expected message %q, got %q", "publish failed", rec.Message)
	}
	errVal, ok := rec.Extra["error"]
	if !ok {
		t.Fatal("expected 'error' field in log output, not found")
	}
	if errVal != "connection refused" {
		t.Errorf("expected error=%q, got %v", "connection refused", errVal)
	}
}

func TestAdapter_ErrorWithNilErrLogsMessageOnly(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)
	adapter.Error("something happened", nil, nil)

	rec := parseLogRecord(t, &buf)

	if rec.Level != "ERROR" {
		t.Errorf("expected level ERROR, got %q", rec.Level)
	}
	if rec.Message != "something happened" {
		t.Errorf("expected message %q, got %q", "something happened", rec.Message)
	}
}

func TestAdapter_ErrorIncludesWatermillFieldsAsSlogAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)
	testErr := errors.New("timeout")
	adapter.Error("consume failed", testErr, watermill.LogFields{
		"topic":    "events",
		"retry_no": 3,
	})

	rec := parseLogRecord(t, &buf)

	topic, ok := rec.Extra["topic"]
	if !ok {
		t.Fatal("expected 'topic' field in log output, not found")
	}
	if topic != "events" {
		t.Errorf("expected topic=%q, got %v", "events", topic)
	}

	retryNo, ok := rec.Extra["retry_no"]
	if !ok {
		t.Fatal("expected 'retry_no' field in log output, not found")
	}
	if retryNo != float64(3) {
		t.Errorf("expected retry_no=3, got %v", retryNo)
	}

	errVal, ok := rec.Extra["error"]
	if !ok {
		t.Fatal("expected 'error' field in log output, not found")
	}
	if errVal != "timeout" {
		t.Errorf("expected error=%q, got %v", "timeout", errVal)
	}
}

func TestAdapter_DebugLogsAtDebugLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)
	adapter.Debug("checking handler", watermill.LogFields{
		"handler": "order_created",
	})

	rec := parseLogRecord(t, &buf)

	if rec.Level != "DEBUG" {
		t.Errorf("expected level DEBUG, got %q", rec.Level)
	}
	if rec.Message != "checking handler" {
		t.Errorf("expected message %q, got %q", "checking handler", rec.Message)
	}

	handler, ok := rec.Extra["handler"]
	if !ok {
		t.Fatal("expected 'handler' field in log output, not found")
	}
	if handler != "order_created" {
		t.Errorf("expected handler=%q, got %v", "order_created", handler)
	}
}

func TestAdapter_TraceMapsToDebugLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)
	adapter.Trace("detailed trace", watermill.LogFields{
		"trace_id": "abc-123",
	})

	rec := parseLogRecord(t, &buf)

	if rec.Level != "DEBUG" {
		t.Errorf("expected level DEBUG (trace mapped to debug), got %q", rec.Level)
	}
	if rec.Message != "detailed trace" {
		t.Errorf("expected message %q, got %q", "detailed trace", rec.Message)
	}

	traceID, ok := rec.Extra["trace_id"]
	if !ok {
		t.Fatal("expected 'trace_id' field in log output, not found")
	}
	if traceID != "abc-123" {
		t.Errorf("expected trace_id=%q, got %v", "abc-123", traceID)
	}
}

func TestAdapter_WithCreatesNewAdapterOriginalUntouched(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	original := New(logger, ctx)
	derived := original.With(watermill.LogFields{
		"component": "broker",
	})

	derived.Info("from derived", nil)
	recDerived := parseLogRecord(t, &buf)

	comp, ok := recDerived.Extra["component"]
	if !ok {
		t.Fatal("expected 'component' field from derived adapter, not found")
	}
	if comp != "broker" {
		t.Errorf("expected component=%q, got %v", "broker", comp)
	}

	buf.Reset()
	original.Info("from original", nil)
	recOriginal := parseLogRecord(t, &buf)

	if _, ok := recOriginal.Extra["component"]; ok {
		t.Error("original adapter must not contain 'component' field after With() on a derived adapter")
	}
}

func TestAdapter_WithMergesFieldsNewOverridesOld(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)

	first := adapter.With(watermill.LogFields{
		"env":     "staging",
		"version": "v1",
	})
	second := first.With(watermill.LogFields{
		"version": "v2",
		"region":  "us-east-1",
	})

	second.Info("merged", nil)

	raw := buf.Bytes()
	if count := bytes.Count(raw, []byte(`"version":`)); count != 1 {
		t.Errorf("expected exactly 1 occurrence of \"version:\" in JSON, got %d: %s", count, raw)
	}

	rec := parseLogRecord(t, &buf)

	env, ok := rec.Extra["env"]
	if !ok {
		t.Fatal("expected 'env' field, not found")
	}
	if env != "staging" {
		t.Errorf("expected env=%q, got %v", "staging", env)
	}

	version, ok := rec.Extra["version"]
	if !ok {
		t.Fatal("expected 'version' field, not found")
	}
	if version != "v2" {
		t.Errorf("expected version=%q (overridden), got %v", "v2", version)
	}

	region, ok := rec.Extra["region"]
	if !ok {
		t.Fatal("expected 'region' field, not found")
	}
	if region != "us-east-1" {
		t.Errorf("expected region=%q, got %v", "us-east-1", region)
	}
}

func TestAdapter_WithFieldsAndCallFieldsBothPresent(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ctx := context.Background()

	adapter := New(logger, ctx)
	derived := adapter.With(watermill.LogFields{
		"component": "subscriber",
	})

	derived.Info("processing", watermill.LogFields{
		"topic":      "orders",
		"message_id": "msg-42",
	})

	rec := parseLogRecord(t, &buf)

	comp, ok := rec.Extra["component"]
	if !ok {
		t.Fatal("expected 'component' (With field) in log output, not found")
	}
	if comp != "subscriber" {
		t.Errorf("expected component=%q, got %v", "subscriber", comp)
	}

	topic, ok := rec.Extra["topic"]
	if !ok {
		t.Fatal("expected 'topic' (call field) in log output, not found")
	}
	if topic != "orders" {
		t.Errorf("expected topic=%q, got %v", "orders", topic)
	}

	msgID, ok := rec.Extra["message_id"]
	if !ok {
		t.Fatal("expected 'message_id' (call field) in log output, not found")
	}
	if msgID != "msg-42" {
		t.Errorf("expected message_id=%q, got %v", "msg-42", msgID)
	}
}

type contextCapturingHandler struct {
	slog.Handler
	captured context.Context
}

func (h *contextCapturingHandler) Handle(ctx context.Context, r slog.Record) error {
	h.captured = ctx
	return h.Handler.Handle(ctx, r)
}

func (h *contextCapturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextCapturingHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextCapturingHandler) WithGroup(name string) slog.Handler {
	return &contextCapturingHandler{Handler: h.Handler.WithGroup(name)}
}

type ctxKey struct{}

func TestAdapter_ContextIsPreserved(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	capturing := &contextCapturingHandler{Handler: inner}
	logger := slog.New(capturing)

	ctx := context.WithValue(context.Background(), ctxKey{}, "request-789")

	adapter := New(logger, ctx)
	adapter.Info("with context", nil)

	if capturing.captured == nil {
		t.Fatal("expected context to be passed to handler, got nil")
	}

	val, ok := capturing.captured.Value(ctxKey{}).(string)
	if !ok || val != "request-789" {
		t.Errorf("expected context value %q, got %v", "request-789", capturing.captured.Value(ctxKey{}))
	}
}
