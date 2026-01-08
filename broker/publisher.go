package broker

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
)

// Publisher defines the interface for publishing CloudEvents
type Publisher interface {
	// Publish publishes a CloudEvent to the specified topic with context
	Publish(ctx context.Context, topic string, event *event.Event) error
	// Close closes the underlying publisher
	Close() error
}

// publisher wraps a Watermill publisher and provides a simplified interface
type publisher struct {
	pub    message.Publisher
	logger logger.Logger // Caller's logger (always present - default logger if not provided)
}

// Publish publishes a CloudEvent to the specified topic with context
func (p *publisher) Publish(ctx context.Context, topic string, event *event.Event) error {
	// Log the publish operation - logger is guaranteed non-nil
	p.logger.Infof(ctx, "Publishing event %s to topic %s", event.ID(), topic)

	// Convert CloudEvent to Watermill message
	msg, err := eventToMessage(event)
	if err != nil {
		p.logger.Errorf(ctx, "Failed to convert CloudEvent to message: %v", err)
		return err
	}

	// Publish the message
	err = p.pub.Publish(topic, msg)
	if err != nil {
		p.logger.Errorf(ctx, "Failed to publish message to topic: %v", err)
		return err
	}

	p.logger.Debugf(ctx, "Successfully published event %s to topic %s", event.ID(), topic)
	return nil
}

// Close closes the underlying publisher
func (p *publisher) Close() error {
	p.logger.Info(context.Background(), "Closing publisher")

	err := p.pub.Close()
	if err != nil {
		p.logger.Errorf(context.Background(), "Failed to close publisher: %v", err)
	}

	return err
}
