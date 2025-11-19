package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
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

	// Create a new router for this subscription
	router, err := message.NewRouter(message.RouterConfig{}, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
	}

	// Add standard middleware
	router.AddMiddleware(
		middleware.Recoverer,
	)

	// Create the Watermill handler function
	h := func(msg *message.Message) error {
		// Convert Watermill message to CloudEvent
		evt, err := messageToEvent(msg)
		if err != nil {
			// If conversion fails, we return error which triggers Nack/Retry
			// If it's a permanent error (malformed), Retry middleware will give up after MaxRetries
			// and message will be Nacked (or sent to PoisonQueue if configured, but here just Nacked)
			return fmt.Errorf("failed to convert message to CloudEvent: %w", err)
		}

		// Process the event with the handler
		// IMPORTANT: Pass msg.Context() to preserve tracing/metadata
		return handler(msg.Context(), evt)
	}

	// Register handler multiple times to achieve parallelism
	// Watermill Router processes each handler in a separate goroutine
	for i := 0; i < s.parallelism; i++ {
		handlerName := fmt.Sprintf("%s-%d", topic, i)
		router.AddConsumerHandler(
			handlerName,
			topic,
			s.sub,
			h,
		)
	}

	// Run the router in the background
	s.wg.Go(func() {
		if err := router.Run(ctx); err != nil {
			s.logger.Error("Router stopped with error", err, watermill.LogFields{
				"topic":           topic,
				"subscription_id": s.subscriptionID,
			})
		}
	})

	return nil
}

// Close closes the underlying subscriber
func (s *subscriber) Close() error {
	// Closing the subscriber will stop all routers receiving messages
	err := s.sub.Close()
	if err != nil {
		return err
	}
	s.wg.Wait()
	return nil
}
