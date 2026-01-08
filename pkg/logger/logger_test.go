package logger

import (
	"context"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
)

func TestWatermillLoggerAdapter_Info(t *testing.T) {
	mockLogger := NewMockLogger()
	ctx := context.Background()

	adapter := NewWatermillLoggerAdapter(mockLogger, ctx)

	// Test Info method with MockLogger (falls back to old behavior since it's not defaultLogger)
	adapter.Info("test message", watermill.LogFields{
		"topic":           "test-topic",
		"subscription_id": "test-sub",
	})

	logs := mockLogger.GetLogs()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	log := logs[0]
	if log.Level != "info" {
		t.Errorf("Expected level 'info', got '%s'", log.Level)
	}

	// Since this uses MockLogger (not defaultLogger), it should fall back to simple message
	if log.Message != "test message" {
		t.Errorf("Expected message 'test message', got '%s'", log.Message)
	}
}

func TestWatermillLoggerAdapter_Error(t *testing.T) {
	mockLogger := NewMockLogger()
	ctx := context.Background()

	adapter := NewWatermillLoggerAdapter(mockLogger, ctx)

	// Test Error method with MockLogger (falls back to old behavior since it's not defaultLogger)
	testErr := errors.New("test error")
	adapter.Error("error occurred", testErr, watermill.LogFields{
		"topic": "error-topic",
	})

	logs := mockLogger.GetLogs()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	log := logs[0]
	if log.Level != "error" {
		t.Errorf("Expected level 'error', got '%s'", log.Level)
	}

	expectedMessage := "error occurred: test error"
	if log.Message != expectedMessage {
		t.Errorf("Expected message '%s', got '%s'", expectedMessage, log.Message)
	}
}

func TestWatermillLoggerAdapter_With(t *testing.T) {
	mockLogger := NewMockLogger()
	ctx := context.Background()

	adapter := NewWatermillLoggerAdapter(mockLogger, ctx)

	// Test With method
	newAdapter := adapter.With(watermill.LogFields{
		"component": "test-component",
		"version":   "v1.0.0",
	})

	newAdapter.Info("test with fields", nil)

	logs := mockLogger.GetLogs()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	log := logs[0]
	// Since our simplified implementation doesn't capture fields separately,
	// the With method doesn't affect subsequent logging.
	// This test just verifies that With() returns a valid adapter.
	if log.Message != "test with fields" {
		t.Errorf("Expected message 'test with fields', got '%s'", log.Message)
	}
}

func TestWatermillLoggerAdapter_Trace(t *testing.T) {
	mockLogger := NewMockLogger()
	ctx := context.Background()

	adapter := NewWatermillLoggerAdapter(mockLogger, ctx)

	// Test Trace method (should map to Debug) with MockLogger (falls back to old behavior)
	adapter.Trace("trace message", watermill.LogFields{
		"trace_id": "123",
	})

	logs := mockLogger.GetLogs()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	log := logs[0]
	if log.Level != "debug" {
		t.Errorf("Expected level 'debug' (trace mapped to debug), got '%s'", log.Level)
	}

	expectedMessage := "trace message"
	if log.Message != expectedMessage {
		t.Errorf("Expected message '%s', got '%s'", expectedMessage, log.Message)
	}
}
