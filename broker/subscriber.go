package broker

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cloudevents/sdk-go/v2/event"
)

// HandlerFunc is a function that handles a CloudEvent
type HandlerFunc func(ctx context.Context, event *event.Event) error

// Subscriber defines the interface for subscribing to CloudEvents
type Subscriber interface {
	// Subscribe subscribes to a topic and processes messages with the provided handler
	Subscribe(ctx context.Context, topic string, handler HandlerFunc) error
	// Close closes the underlying subscriber
	Close() error
}

// subscriber wraps a Watermill subscriber and provides worker pools for parallel message processing
type subscriber struct {
	sub            message.Subscriber
	parallelism    int
	subscriptionID string
	logger         watermill.LoggerAdapter
	wg             sync.WaitGroup
}

// Subscribe subscribes to a topic and processes messages with the provided handler
// The subscriptionID stored in the subscriber struct determines whether subscribers share messages
// (same ID = shared, different IDs = separate)
func (s *subscriber) Subscribe(ctx context.Context, topic string, handler HandlerFunc) error {
	if handler == nil {
		return fmt.Errorf("handler must be provided")
	}

	// Subscribe to the original topic name
	// The subscription ID is handled by the broker-specific configuration:
	// - RabbitMQ: Queue name generator uses subscription ID
	// - Google Pub/Sub: Subscription name generator uses subscription ID
	// This ensures publishers and subscribers use the same topic/exchange name
	messages, err := s.sub.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	// Create a channel to distribute messages to workers
	messageChan := make(chan *message.Message, s.parallelism)

	// Start worker pool
	//var wg sync.WaitGroup
	for i := 0; i < s.parallelism; i++ {
		s.wg.Go(func() {
			s.worker(ctx, messageChan, handler)
		})
	}

	// Distribute messages to workers
	go func() {
		defer close(messageChan)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				select {
				case messageChan <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return nil
}

// worker processes messages from the message channel
func (s *subscriber) worker(ctx context.Context, messages <-chan *message.Message, handler HandlerFunc) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("Worker panicked - recovered",
				fmt.Errorf("panic: %v", r), watermill.LogFields{
					"subscription_id": s.subscriptionID,
					"stack":           string(debug.Stack()),
				})
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			// Convert Watermill message to CloudEvent
			evt, err := messageToEvent(msg)
			if err != nil {
				s.logger.Error("Error converting message to CloudEvent", err, watermill.LogFields{
					"uuid":            msg.UUID,
					"subscription_id": s.subscriptionID,
					"message":         msg.Payload,
					"metadata":        msg.Metadata,
				})
				msg.Nack()
				continue
			}

			// Process the event with the handler
			if err := handler(ctx, evt); err != nil {
				msg.Nack()
				continue
			}

			// Acknowledge the message
			msg.Ack()
		}
	}
}

// Close closes the underlying subscriber
func (s *subscriber) Close() error {
	err := s.sub.Close()
	if err != nil {
		return err
	}
	s.wg.Wait()
	return nil
}
