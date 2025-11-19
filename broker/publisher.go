package broker

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cloudevents/sdk-go/v2/event"
)

// Publisher defines the interface for publishing CloudEvents
type Publisher interface {
	// Publish publishes a CloudEvent to the specified topic
	Publish(topic string, event *event.Event) error
	// Close closes the underlying publisher
	Close() error
}

// publisher wraps a Watermill publisher and provides a simplified interface
type publisher struct {
	pub message.Publisher
}

// Publish publishes a CloudEvent to the specified topic
func (p *publisher) Publish(topic string, event *event.Event) error {
	// Convert CloudEvent to Watermill message
	msg, err := eventToMessage(event)
	if err != nil {
		return err
	}

	// Publish the message
	return p.pub.Publish(topic, msg)
}

// Close closes the underlying publisher
func (p *publisher) Close() error {
	return p.pub.Close()
}
