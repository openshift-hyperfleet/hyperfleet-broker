package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockPublisher_Publish(t *testing.T) {
	pub := NewMockPublisher()

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	err := pub.Publish("test-topic", &evt)
	require.NoError(t, err)

	events := pub.GetPublishedEvents("test-topic")
	require.Len(t, events, 1)
	assert.Equal(t, "test-id", events[0].ID())
}

func TestMockPublisher_PublishError(t *testing.T) {
	pub := NewMockPublisher()
	pub.PublishError = errors.New("publish failed")

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	err := pub.Publish("test-topic", &evt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "publish failed")

	// No events should be stored when error occurs
	events := pub.GetPublishedEvents("test-topic")
	assert.Empty(t, events)
}

func TestMockPublisher_PublishFunc(t *testing.T) {
	pub := NewMockPublisher()

	callCount := 0
	pub.PublishFunc = func(topic string, evt *event.Event) error {
		callCount++
		if topic == "error-topic" {
			return errors.New("custom error")
		}
		return nil
	}

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	// Should succeed for non-error topic
	err := pub.Publish("test-topic", &evt)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)

	// Should fail for error topic
	err = pub.Publish("error-topic", &evt)
	assert.Error(t, err)
	assert.Equal(t, 2, callCount)
}

func TestMockPublisher_Close(t *testing.T) {
	pub := NewMockPublisher()

	assert.False(t, pub.Closed)

	err := pub.Close()
	assert.NoError(t, err)
	assert.True(t, pub.Closed)
}

func TestMockPublisher_CloseError(t *testing.T) {
	pub := NewMockPublisher()
	pub.CloseError = errors.New("close failed")

	err := pub.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
	assert.True(t, pub.Closed)
}

func TestMockPublisher_GetAllPublishedEvents(t *testing.T) {
	pub := NewMockPublisher()

	evt1 := event.New()
	evt1.SetID("id-1")
	evt1.SetType("test.type")
	evt1.SetSource("test-source")

	evt2 := event.New()
	evt2.SetID("id-2")
	evt2.SetType("test.type")
	evt2.SetSource("test-source")

	err := pub.Publish("topic-1", &evt1)
	require.NoError(t, err)
	err = pub.Publish("topic-2", &evt2)
	require.NoError(t, err)

	allEvents := pub.GetAllPublishedEvents()
	assert.Len(t, allEvents, 2)
	assert.Len(t, allEvents["topic-1"], 1)
	assert.Len(t, allEvents["topic-2"], 1)
}

func TestMockPublisher_Reset(t *testing.T) {
	pub := NewMockPublisher()
	pub.PublishError = errors.New("some error")
	pub.CloseError = errors.New("close error")
	pub.Closed = true

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	pub.Reset()

	assert.Empty(t, pub.PublishedEvents)
	assert.Nil(t, pub.PublishError)
	assert.Nil(t, pub.CloseError)
	assert.False(t, pub.Closed)

	// Should be able to publish after reset
	err := pub.Publish("test-topic", &evt)
	assert.NoError(t, err)
}

func TestMockSubscriber_Subscribe(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}

	err := sub.Subscribe(ctx, "test-topic", handler)
	require.NoError(t, err)

	assert.True(t, sub.HasHandler("test-topic"))
	assert.Len(t, sub.GetHandlers("test-topic"), 1)
}

func TestMockSubscriber_SubscribeNilHandler(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	err := sub.Subscribe(ctx, "test-topic", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler must be provided")
}

func TestMockSubscriber_SubscribeError(t *testing.T) {
	sub := NewMockSubscriber()
	sub.SubscribeError = errors.New("subscribe failed")
	ctx := context.Background()

	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}

	err := sub.Subscribe(ctx, "test-topic", handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subscribe failed")
}

func TestMockSubscriber_SubscribeFunc(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	subscribeCalls := 0
	sub.SubscribeFunc = func(ctx context.Context, topic string, handler HandlerFunc) error {
		subscribeCalls++
		if topic == "error-topic" {
			return errors.New("custom error")
		}
		return nil
	}

	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}

	err := sub.Subscribe(ctx, "test-topic", handler)
	assert.NoError(t, err)
	assert.Equal(t, 1, subscribeCalls)

	err = sub.Subscribe(ctx, "error-topic", handler)
	assert.Error(t, err)
	assert.Equal(t, 2, subscribeCalls)
}

func TestMockSubscriber_SimulateMessage(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	receivedEvents := make([]*event.Event, 0)
	handler := func(ctx context.Context, evt *event.Event) error {
		receivedEvents = append(receivedEvents, evt)
		return nil
	}

	err := sub.Subscribe(ctx, "test-topic", handler)
	require.NoError(t, err)

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	err = sub.SimulateMessage(ctx, "test-topic", &evt)
	require.NoError(t, err)

	require.Len(t, receivedEvents, 1)
	assert.Equal(t, "test-id", receivedEvents[0].ID())
}

func TestMockSubscriber_SimulateMessageNoHandler(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	err := sub.SimulateMessage(ctx, "test-topic", &evt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no handlers registered for topic")
}

func TestMockSubscriber_SimulateMessageHandlerError(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	handler := func(ctx context.Context, evt *event.Event) error {
		return errors.New("handler error")
	}

	err := sub.Subscribe(ctx, "test-topic", handler)
	require.NoError(t, err)

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	err = sub.SimulateMessage(ctx, "test-topic", &evt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler error")
}

func TestMockSubscriber_MultipleHandlers(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	handler1Called := false
	handler2Called := false

	handler1 := func(ctx context.Context, evt *event.Event) error {
		handler1Called = true
		return nil
	}
	handler2 := func(ctx context.Context, evt *event.Event) error {
		handler2Called = true
		return nil
	}

	err := sub.Subscribe(ctx, "test-topic", handler1)
	require.NoError(t, err)
	err = sub.Subscribe(ctx, "test-topic", handler2)
	require.NoError(t, err)

	assert.Len(t, sub.GetHandlers("test-topic"), 2)

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	err = sub.SimulateMessage(ctx, "test-topic", &evt)
	require.NoError(t, err)

	assert.True(t, handler1Called)
	assert.True(t, handler2Called)
}

func TestMockSubscriber_Close(t *testing.T) {
	sub := NewMockSubscriber()

	assert.False(t, sub.Closed)

	err := sub.Close()
	assert.NoError(t, err)
	assert.True(t, sub.Closed)
}

func TestMockSubscriber_CloseError(t *testing.T) {
	sub := NewMockSubscriber()
	sub.CloseError = errors.New("close failed")

	err := sub.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
	assert.True(t, sub.Closed)
}

func TestMockSubscriber_Reset(t *testing.T) {
	sub := NewMockSubscriber()
	sub.SubscribeError = errors.New("some error")
	sub.CloseError = errors.New("close error")
	sub.Closed = true
	ctx := context.Background()

	// Reinitialize the event queue since it was closed
	sub.eventQueue = make(chan *mockEvent, 100)

	sub.Reset()

	assert.Empty(t, sub.Handlers)
	assert.Nil(t, sub.SubscribeError)
	assert.Nil(t, sub.CloseError)
	assert.False(t, sub.Closed)

	// Should be able to subscribe after reset
	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}
	err := sub.Subscribe(ctx, "test-topic", handler)
	assert.NoError(t, err)
}

// Test that demonstrates using mocks for error handling tests
func TestSubscriberSubscribeErrorHandling_WithMock(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	tests := []struct {
		name        string
		topic       string
		handler     HandlerFunc
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil handler",
			topic:       "test-topic",
			handler:     nil,
			expectError: true,
			errorMsg:    "handler must be provided",
		},
		{
			name:  "valid handler",
			topic: "test-topic",
			handler: func(ctx context.Context, evt *event.Event) error {
				return nil
			},
			expectError: false,
		},
		{
			name:  "empty topic",
			topic: "",
			handler: func(ctx context.Context, evt *event.Event) error {
				return nil
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub.Reset()

			err := sub.Subscribe(ctx, tt.topic, tt.handler)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Test handler error handling with mock
func TestSubscriberHandlerErrorHandling_WithMock(t *testing.T) {
	sub := NewMockSubscriber()
	ctx := context.Background()

	errorHandler := func(ctx context.Context, evt *event.Event) error {
		return errors.New("handler error")
	}

	err := sub.Subscribe(ctx, "test-topic", errorHandler)
	require.NoError(t, err)

	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	// Simulate message delivery and verify error is returned
	err = sub.SimulateMessage(ctx, "test-topic", &evt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler error")
}

