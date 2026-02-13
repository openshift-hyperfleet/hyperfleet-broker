package broker

import (
	"context"
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestPublisherHealthWithCustomHealthCheck(t *testing.T) {
	mockLogger := logger.NewMockLogger()

	t.Run("healthy publisher returns nil", func(t *testing.T) {
		p := &publisher{
			logger: mockLogger,
			healthCheck: func(_ context.Context) error {
				return nil
			},
		}
		err := p.Health(context.Background())
		assert.NoError(t, err)
	})

	t.Run("unhealthy publisher returns error", func(t *testing.T) {
		p := &publisher{
			logger: mockLogger,
			healthCheck: func(_ context.Context) error {
				return fmt.Errorf("connection lost")
			},
		}
		err := p.Health(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection lost")
	})
}

func TestNewRabbitMQHealthCheck(t *testing.T) {
	t.Run("returns error for non-AMQP publisher", func(t *testing.T) {
		// Pass a fakeWatermillPublisher (non-AMQP) to newRabbitMQHealthCheck
		// to trigger the AMQP type assertion failure
		hc := newRabbitMQHealthCheck(&fakeWatermillPublisher{})
		err := hc(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected publisher type")
	})
}

// fakeWatermillPublisher implements message.Publisher for testing type assertion failures
type fakeWatermillPublisher struct{}

func (f *fakeWatermillPublisher) Publish(_ string, _ ...*message.Message) error {
	return nil
}

func (f *fakeWatermillPublisher) Close() error {
	return nil
}
