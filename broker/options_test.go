package broker

import (
	"testing"

	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/stretchr/testify/assert"
)

// These tests verify parameter validation only.
// Success cases that require a running broker are covered by integration tests.

func TestNewPublisherWithRequiredLogger(t *testing.T) {
	configMap := map[string]string{
		"broker.type":         "rabbitmq",
		"broker.rabbitmq.url": "amqp://guest:guest@localhost:5672/",
	}

	t.Run("publisher with nil logger returns error", func(t *testing.T) {
		pub, err := NewPublisher(nil, configMap)
		assert.Error(t, err)
		assert.Nil(t, pub)
		assert.Contains(t, err.Error(), "logger is required")
	})
}

func TestNewSubscriberWithRequiredLogger(t *testing.T) {
	configMap := map[string]string{
		"broker.type":         "rabbitmq",
		"broker.rabbitmq.url": "amqp://guest:guest@localhost:5672/",
	}

	t.Run("subscriber with nil logger returns error", func(t *testing.T) {
		sub, err := NewSubscriber(nil, "test-sub", configMap)
		assert.Error(t, err)
		assert.Nil(t, sub)
		assert.Contains(t, err.Error(), "logger is required")
	})

	t.Run("subscriber with empty subscription ID returns error", func(t *testing.T) {
		mockLogger := logger.NewMockLogger()
		sub, err := NewSubscriber(mockLogger, "", configMap)
		assert.Error(t, err)
		assert.Nil(t, sub)
		assert.Contains(t, err.Error(), "subscriptionID is required")
	})
}
