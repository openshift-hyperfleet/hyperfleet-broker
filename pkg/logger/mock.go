package logger

import (
	"context"
	"fmt"
	"sync"
)

// MockLogger implements the Logger interface for testing
type MockLogger struct {
	mu   sync.RWMutex
	logs []LogEntry
}

type LogEntry struct {
	Level   string
	Message string
	Context context.Context
}

func NewMockLogger() *MockLogger {
	return &MockLogger{
		logs: make([]LogEntry, 0),
	}
}

func (m *MockLogger) Debug(ctx context.Context, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, LogEntry{
		Level:   "debug",
		Message: message,
		Context: ctx,
	})
}

func (m *MockLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	m.Debug(ctx, fmt.Sprintf(format, args...))
}

func (m *MockLogger) Info(ctx context.Context, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, LogEntry{
		Level:   "info",
		Message: message,
		Context: ctx,
	})
}

func (m *MockLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	m.Info(ctx, fmt.Sprintf(format, args...))
}

func (m *MockLogger) Warn(ctx context.Context, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, LogEntry{
		Level:   "warn",
		Message: message,
		Context: ctx,
	})
}

func (m *MockLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	m.Warn(ctx, fmt.Sprintf(format, args...))
}

func (m *MockLogger) Error(ctx context.Context, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, LogEntry{
		Level:   "error",
		Message: message,
		Context: ctx,
	})
}

func (m *MockLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	m.Error(ctx, fmt.Sprintf(format, args...))
}

func (m *MockLogger) GetLogs() []LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Return a copy to avoid callers mutating internal state
	logsCopy := make([]LogEntry, len(m.logs))
	copy(logsCopy, m.logs)
	return logsCopy
}
