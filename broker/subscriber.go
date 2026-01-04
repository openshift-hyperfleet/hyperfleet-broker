package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
)

// HandlerFunc is a function that handles a CloudEvent
type HandlerFunc func(ctx context.Context, event *event.Event) error

// Subscriber defines the interface for subscribing to CloudEvents
type Subscriber interface {
	// Subscribe subscribes to a topic and processes messages with the provided handler
	Subscribe(ctx context.Context, topic string, handler HandlerFunc) error
	// Errors returns a channel that receives errors from background operations.
	// The channel is buffered to prevent blocking the subscriber.
	// The channel is closed when Close() is called.
	// Consumers SHOULD drain this channel to prevent memory leaks.
	Errors() <-chan *SubscriberError
	// Close closes the underlying subscriber
	Close() error
}

const (
	// ErrorChannelBufferSize is the buffer size for the error channel
	// Large enough to handle bursts without blocking
	ErrorChannelBufferSize = 100
)

// subscriber wraps a Watermill subscriber and provides worker pools for parallel message processing
type subscriber struct {
	sub            message.Subscriber
	parallelism    int
	subscriptionID string
	logger         logger.Logger // Broker logger (always present - default logger if not provided)
	wg             sync.WaitGroup

	// Error notification channel
	errorChan chan *SubscriberError

	// Track if closed to prevent sending on closed channel
	closed  bool
	closeMu sync.RWMutex
}

// Subscribe subscribes to a topic and processes messages with the provided handler
// The subscriptionID stored in the subscriber struct determines whether subscribers share messages
// (same ID = shared, different IDs = separate)
func (s *subscriber) Subscribe(ctx context.Context, topic string, handler HandlerFunc) error {
	if handler == nil {
		return fmt.Errorf("handler must be provided")
	}

	// Create a per-call Watermill logger adapter with the per-call context.
	wmLogger := logger.NewWatermillLoggerAdapter(s.logger, ctx)

	// Log subscription start - logger is guaranteed non-nil
	s.logger.Infof(ctx, "Starting subscription to topic %s", topic)

	// Create a new router for this subscription
	router, err := message.NewRouter(message.RouterConfig{}, wmLogger)
	if err != nil {
		s.logger.Errorf(ctx, "Failed to create router: %v", err)
		return fmt.Errorf("failed to create router: %w", err)
	}

	// Add standard middleware
	router.AddMiddleware(
		middleware.Recoverer,
	)

	// Create the Watermill handler function
	h := func(msg *message.Message) error {
		// Use message context for tracing/metadata preservation
		msgCtx := msg.Context()

		// Log message received - logger is guaranteed non-nil
		s.logger.Infof(msgCtx, "Received message from topic %s", topic)

		// Convert Watermill message to CloudEvent
		evt, err := messageToEvent(msg)
		if err != nil {
			s.logger.Errorf(msgCtx, "Failed to convert message to CloudEvent: %v", err)
			// If conversion fails, we return error which triggers Nack/Retry
			// If it's a permanent error (malformed), Retry middleware will give up after MaxRetries
			// and message will be Nacked (or sent to PoisonQueue if configured, but here just Nacked)
			return fmt.Errorf("failed to convert message to CloudEvent: %w", err)
		}

		// Process the event with the handler
		// IMPORTANT: Pass msg.Context() to preserve tracing/metadata
		err = handler(msgCtx, evt)
		if err != nil {
			s.logger.Errorf(msgCtx, "Handler failed to process event: %v", err)
		} else {
			s.logger.Infof(msgCtx, "Successfully processed event %s from topic %s subscription %s", evt.ID(), topic, s.subscriptionID)
		}

		return err
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

	// Log successful subscription setup
	s.logger.Infof(ctx, "Successfully subscribed to topic %s subscription %s", topic, s.subscriptionID)

	// Run the router in the background
	s.wg.Go(func() {
		if err := router.Run(ctx); err != nil {
			// Determine if this is fatal (connection lost) or recoverable
			fatal := !isContextCanceled(err)

			// Log router error
			s.logger.Errorf(ctx, "Router stopped with error: %v", err)

			s.sendError(&SubscriberError{
				Op:             "router",
				Topic:          topic,
				SubscriptionID: s.subscriptionID,
				Err:            err,
				Timestamp:      time.Now(),
				Fatal:          fatal,
			})
		}
	})

	return nil
}

// Errors returns the error notification channel
func (s *subscriber) Errors() <-chan *SubscriberError {
	return s.errorChan
}

// sendError sends an error to the error channel without blocking
func (s *subscriber) sendError(err *SubscriberError) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()

	if s.closed {
		return // Don't send on closed channel
	}

	select {
	case s.errorChan <- err:
		// Error sent successfully
	default:
		// Channel full - log and drop oldest error to make room
		s.logger.Errorf(context.Background(),
			"Error channel full, dropping error: topic=%s, subscription_id=%s, buffer_size=%d, error=%v",
			err.Topic, s.subscriptionID, ErrorChannelBufferSize, err.Err)

		// Try to drain one old error and send new one
		select {
		case <-s.errorChan:
			// Drained one, try again
			select {
			case s.errorChan <- err:
			default:
			}
		default:
		}
	}
}

// Close closes the underlying subscriber
func (s *subscriber) Close() error {
	// Log close operation
	s.logger.Info(context.Background(), "Closing subscriber")

	// Closing the subscriber will stop all routers receiving messages
	err := s.sub.Close()
	if err != nil {
		s.logger.Errorf(context.Background(), "Failed to close underlying subscriber: %v", err)
		return err
	}
	s.wg.Wait()

	// Mark as closed and close error channel
	s.closeMu.Lock()
	s.closed = true
	close(s.errorChan)
	s.closeMu.Unlock()

	s.logger.Info(context.Background(), "Successfully closed subscriber")
	return nil
}

// isContextCanceled checks if error is from context cancellation
func isContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
