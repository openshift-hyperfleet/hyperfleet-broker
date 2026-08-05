package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cloudevents/sdk-go/v2/event"
)

// Publisher defines the interface for publishing CloudEvents
type Publisher interface {
	// Publish publishes a CloudEvent to the specified topic with context
	Publish(ctx context.Context, topic string, event *event.Event) error
	// Health checks if the underlying broker connection is healthy.
	// Returns nil if healthy, or an error describing the failure.
	// The provided context controls the deadline/cancellation of the check.
	Health(ctx context.Context) error
	// Close closes the underlying publisher
	Close() error
	// BrokerType returns the configured broker type (e.g. "rabbitmq", "googlepubsub")
	BrokerType() string
}

// healthCheckFunc is a function that checks broker connectivity.
// Returns nil if healthy, or an error describing the failure.
// The provided context controls the deadline/cancellation of the check.
type healthCheckFunc func(ctx context.Context) error

// publisher wraps a Watermill publisher and provides a simplified interface
type publisher struct {
	pub          message.Publisher
	logger       *slog.Logger
	healthCheck  healthCheckFunc
	healthCloser io.Closer // optional resource to close with publisher (e.g. Pub/Sub health check client)
	metrics      *MetricsRecorder
	brokerType   string
}

// Publish publishes a CloudEvent to the specified topic with context
func (p *publisher) Publish(ctx context.Context, topic string, event *event.Event) error {
	p.logger.InfoContext(ctx, "publishing event", "event_id", event.ID(), "topic", topic)

	// Convert CloudEvent to Watermill message
	msg, err := eventToMessage(event)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to convert CloudEvent to message", "error", err)
		p.metrics.RecordError(topic, "conversion")
		return err
	}

	// Publish the message
	err = p.pub.Publish(topic, msg)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to publish message to topic", "error", err)
		p.metrics.RecordError(topic, "publish")
		return err
	}

	p.metrics.RecordPublished(topic)
	p.logger.DebugContext(ctx, "successfully published event", "event_id", event.ID(), "topic", topic)
	return nil
}

// Health checks if the underlying broker connection is healthy.
// Returns nil if healthy, or an error describing the failure.
// The provided context controls the deadline/cancellation of the check.
func (p *publisher) Health(ctx context.Context) error {
	if p == nil || p.healthCheck == nil {
		return fmt.Errorf("health check not configured")
	}
	return p.healthCheck(ctx)
}

// BrokerType returns the configured broker type
func (p *publisher) BrokerType() string {
	return p.brokerType
}

// Close closes the underlying publisher and any health check resources.
func (p *publisher) Close() error {
	p.logger.InfoContext(context.Background(), "closing publisher")

	err := p.pub.Close()
	if err != nil {
		p.logger.ErrorContext(context.Background(), "failed to close publisher", "error", err)
	}

	if p.healthCloser != nil {
		if closeErr := p.healthCloser.Close(); closeErr != nil {
			p.logger.ErrorContext(context.Background(), "failed to close health check client", "error", closeErr)
			if err == nil {
				err = closeErr
			}
		}
	}

	return err
}
