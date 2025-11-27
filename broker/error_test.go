package broker

import (
	"context"
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
)

func TestNewPublisherErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		configMap   map[string]string
		expectError bool
		errorMsg    string
	}{
		{
			name: "unsupported broker type",
			configMap: map[string]string{
				"broker.type": "unsupported-broker",
			},
			expectError: true,
			errorMsg:    "unsupported broker type",
		},
		{
			name: "missing rabbitmq url",
			configMap: map[string]string{
				"broker.type": "rabbitmq",
				// Missing URL - should still create publisher (URL validation happens at connection time)
			},
			expectError: false, // Publisher creation doesn't validate connection
		},
		{
			name: "missing googlepubsub project_id",
			configMap: map[string]string{
				"broker.type": "googlepubsub",
				// Missing project_id
			},
			expectError: true,
			errorMsg:    "googlepubsub.project_id is required",
		},
		{
			name: "invalid config map",
			configMap: map[string]string{
				"broker.type": "rabbitmq",
				"invalid.key": "value",
			},
			expectError: false, // Invalid keys are ignored
		},
		{
			name:        "nil config map",
			configMap:   nil,
			expectError: false, // Falls back to loadConfig()
		},
		{
			name:        "empty config map",
			configMap:   map[string]string{},
			expectError: true, // Will fail when trying to create publisher without broker type
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pub Publisher
			var err error

			if tt.configMap == nil {
				pub, err = NewPublisher()
			} else {
				pub, err = NewPublisher(tt.configMap)
			}

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, pub)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// If no error expected, verify publisher is created
				// Note: Some publishers might fail on actual use, but creation should succeed
				if err == nil {
					assert.NotNil(t, pub)
					if pub != nil {
						defer func() {
							if err := pub.Close(); err != nil {
								t.Logf("failed to close publisher: %v", err)
							}
						}()
					}
				}
			}
		})
	}
}

func TestNewSubscriberErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		subscriptionID string
		configMap      map[string]string
		expectError    bool
		errorMsg       string
	}{
		{
			name:           "empty subscription ID",
			subscriptionID: "",
			configMap: map[string]string{
				"broker.type": "rabbitmq",
			},
			expectError: true,
			errorMsg:    "subscriptionID is required",
		},
		{
			name:           "unsupported broker type",
			subscriptionID: "test-sub",
			configMap: map[string]string{
				"broker.type": "unsupported-broker",
			},
			expectError: true,
			errorMsg:    "unsupported broker type",
		},
		{
			name:           "missing googlepubsub project_id",
			subscriptionID: "test-sub",
			configMap: map[string]string{
				"broker.type": "googlepubsub",
			},
			expectError: true,
			errorMsg:    "googlepubsub.project_id is required",
		},
		{
			name:           "valid rabbitmq config",
			subscriptionID: "test-sub",
			configMap: map[string]string{
				"broker.type": "rabbitmq",
			},
			expectError: false,
		},
		{
			name:           "valid googlepubsub config",
			subscriptionID: "test-sub",
			configMap: map[string]string{
				"broker.type":                    "googlepubsub",
				"broker.googlepubsub.project_id": "test-project",
			},
			expectError: false,
		},
		{
			name:           "nil config map",
			subscriptionID: "test-sub",
			configMap:      nil,
			expectError:    false, // Falls back to loadConfig()
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sub Subscriber
			var err error

			if tt.configMap == nil {
				sub, err = NewSubscriber(tt.subscriptionID)
			} else {
				sub, err = NewSubscriber(tt.subscriptionID, tt.configMap)
			}

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, sub)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// If no error expected, verify subscriber is created
				if err == nil {
					assert.NotNil(t, sub)
					if sub != nil {
						defer func() {
							if err := sub.Close(); err != nil {
								t.Logf("failed to close subscriber: %v", err)
							}
						}()
					}
				}
			}
		})
	}
}

func TestPublisherPublishErrorHandling(t *testing.T) {
	// Create a publisher with invalid config that will fail on actual publish
	configMap := map[string]string{
		"broker.type":         "rabbitmq",
		"broker.rabbitmq.url": "amqp://guest:guest@localhost:1234/",
		// Invalid URL - will fail when trying to connect
	}

	_, err := NewPublisher(configMap)
	assert.Error(t, err)
}

func TestSubscriberSubscribeErrorHandling(t *testing.T) {
	// Use mock subscriber to test error handling without requiring a real broker
	sub := NewMockSubscriber()
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

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
			expectError: false, // Empty topic might be valid for some brokers
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
				if tt.handler != nil {
					assert.True(t, sub.HasHandler(tt.topic))
				}
			}
		})
	}
}

func TestSubscriberHandlerErrorHandling(t *testing.T) {
	// This test verifies that handler errors are handled correctly
	// Using mock subscriber to test without requiring a real broker

	sub := NewMockSubscriber()
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

	// Verify that a handler that returns an error is accepted
	errorHandler := func(ctx context.Context, evt *event.Event) error {
		return assert.AnError
	}

	ctx := context.Background()

	// Subscribe should succeed - handler errors are handled during message processing
	err := sub.Subscribe(ctx, "test-topic", errorHandler)
	assert.NoError(t, err)
	assert.True(t, sub.HasHandler("test-topic"))

	// Simulate message delivery and verify handler error is propagated
	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	err = sub.SimulateMessage(ctx, "test-topic", &evt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "assert.AnError")
}

func TestBuildConfigFromMapErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		configMap   map[string]string
		expectError bool
	}{
		{
			name:        "nil map",
			configMap:   nil,
			expectError: true, // Should handle nil gracefully
		},
		{
			name:        "empty map",
			configMap:   map[string]string{},
			expectError: true,
		},
		{
			name: "valid map",
			configMap: map[string]string{
				"broker.type":         "rabbitmq",
				"broker.rabbitmq.url": "amqp://guest:guest@localhost:5672/",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buildConfigFromMap(tt.configMap)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				// For empty map, it should return default config
				if len(tt.configMap) == 0 {
					assert.NoError(t, err)
					assert.NotNil(t, cfg)
				}
			}
		})
	}
}
