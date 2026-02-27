package broker

import (
	"testing"

	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

// These tests verify parameter validation only.
// Success cases that require a running broker are covered by integration tests.

func TestNewPublisherValidation(t *testing.T) {
	configMap := map[string]string{
		"broker.type":         "rabbitmq",
		"broker.rabbitmq.url": "amqp://guest:guest@localhost:5672/",
	}

	t.Run("publisher with nil logger returns error", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		metrics := NewMetricsRecorder("test", "v0.1.0", reg)
		pub, err := NewPublisher(nil, metrics, configMap)
		assert.Error(t, err)
		assert.Nil(t, pub)
		assert.Contains(t, err.Error(), "logger is required")
	})

	t.Run("publisher with nil metrics returns error", func(t *testing.T) {
		mockLogger := logger.NewMockLogger()
		pub, err := NewPublisher(mockLogger, nil, configMap)
		assert.Error(t, err)
		assert.Nil(t, pub)
		assert.Contains(t, err.Error(), "metrics is required")
	})
}

func TestNewSubscriberValidation(t *testing.T) {
	configMap := map[string]string{
		"broker.type":         "rabbitmq",
		"broker.rabbitmq.url": "amqp://guest:guest@localhost:5672/",
	}

	t.Run("subscriber with nil logger returns error", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		metrics := NewMetricsRecorder("test", "v0.1.0", reg)
		sub, err := NewSubscriber(nil, "test-sub", metrics, configMap)
		assert.Error(t, err)
		assert.Nil(t, sub)
		assert.Contains(t, err.Error(), "logger is required")
	})

	t.Run("subscriber with empty subscription ID returns error", func(t *testing.T) {
		mockLogger := logger.NewMockLogger()
		reg := prometheus.NewRegistry()
		metrics := NewMetricsRecorder("test", "v0.1.0", reg)
		sub, err := NewSubscriber(mockLogger, "", metrics, configMap)
		assert.Error(t, err)
		assert.Nil(t, sub)
		assert.Contains(t, err.Error(), "subscriptionID is required")
	})

	t.Run("subscriber with nil metrics returns error", func(t *testing.T) {
		mockLogger := logger.NewMockLogger()
		sub, err := NewSubscriber(mockLogger, "test-sub", nil, configMap)
		assert.Error(t, err)
		assert.Nil(t, sub)
		assert.Contains(t, err.Error(), "metrics is required")
	})
}
