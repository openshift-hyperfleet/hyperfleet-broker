package broker

import (
	"context"
	"fmt"
	"io"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
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
}

// healthCheckFunc is a function that checks broker connectivity.
// Returns nil if healthy, or an error describing the failure.
// The provided context controls the deadline/cancellation of the check.
type healthCheckFunc func(ctx context.Context) error

// publisher wraps a Watermill publisher and provides a simplified interface
type publisher struct {
	pub          message.Publisher
	logger       logger.Logger // Caller's logger (always present - default logger if not provided)
	healthCheck  healthCheckFunc
	healthCloser io.Closer          // optional resource to close with publisher (e.g. Pub/Sub health check client)
	metrics      *MetricsRecorder
}

// Publish publishes a CloudEvent to the specified topic with context
func (p *publisher) Publish(ctx context.Context, topic string, event *event.Event) error {
	// Log the publish operation - logger is guaranteed non-nil
	p.logger.Infof(ctx, "Publishing event %s to topic %s", event.ID(), topic)

	// Convert CloudEvent to Watermill message
	msg, err := eventToMessage(event)
	if err != nil {
		p.logger.Errorf(ctx, "Failed to convert CloudEvent to message: %v", err)
		p.metrics.RecordError(topic, "conversion")
		return err
	}

	// Publish the message
	err = p.pub.Publish(topic, msg)
	if err != nil {
		p.logger.Errorf(ctx, "Failed to publish message to topic: %v", err)
		p.metrics.RecordError(topic, "publish")
		return err
	}

	p.metrics.RecordPublished(topic)
	p.logger.Debugf(ctx, "Successfully published event %s to topic %s", event.ID(), topic)
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

// Close closes the underlying publisher and any health check resources.
func (p *publisher) Close() error {
	p.logger.Info(context.Background(), "Closing publisher")

	err := p.pub.Close()
	if err != nil {
		p.logger.Errorf(context.Background(), "Failed to close publisher: %v", err)
	}

	if p.healthCloser != nil {
		if closeErr := p.healthCloser.Close(); closeErr != nil {
			p.logger.Errorf(context.Background(), "Failed to close health check client: %v", closeErr)
			if err == nil {
				err = closeErr
			}
		}
	}

	return err
}
