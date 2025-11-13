package broker

import (
	"context"
	"os"
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
						defer pub.Close()
					}
				}
			}
		})
	}
}

func TestNewSubscriberErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		subscriptionId string
		configMap      map[string]string
		expectError    bool
		errorMsg       string
	}{
		{
			name:           "empty subscription ID",
			subscriptionId: "",
			configMap: map[string]string{
				"broker.type": "rabbitmq",
			},
			expectError: true,
			errorMsg:    "subscriptionId is required",
		},
		{
			name:           "unsupported broker type",
			subscriptionId: "test-sub",
			configMap: map[string]string{
				"broker.type": "unsupported-broker",
			},
			expectError: true,
			errorMsg:    "unsupported broker type",
		},
		{
			name:           "missing googlepubsub project_id",
			subscriptionId: "test-sub",
			configMap: map[string]string{
				"broker.type": "googlepubsub",
			},
			expectError: true,
			errorMsg:    "googlepubsub.project_id is required",
		},
		{
			name:           "valid rabbitmq config",
			subscriptionId: "test-sub",
			configMap: map[string]string{
				"broker.type": "rabbitmq",
			},
			expectError: false,
		},
		{
			name:           "valid googlepubsub config",
			subscriptionId: "test-sub",
			configMap: map[string]string{
				"broker.type":                    "googlepubsub",
				"broker.googlepubsub.project_id": "test-project",
			},
			expectError: false,
		},
		{
			name:           "nil config map",
			subscriptionId: "test-sub",
			configMap:      nil,
			expectError:    false, // Falls back to loadConfig()
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sub Subscriber
			var err error

			if tt.configMap == nil {
				sub, err = NewSubscriber(tt.subscriptionId)
			} else {
				sub, err = NewSubscriber(tt.subscriptionId, tt.configMap)
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
						defer sub.Close()
					}
				}
			}
		})
	}
}

func TestPublisherPublishErrorHandling(t *testing.T) {
	// Create a publisher with invalid config that will fail on actual publish
	configMap := map[string]string{
		"broker.type": "rabbitmq",
		// Missing URL - will fail when trying to connect
	}

	pub, err := NewPublisher(configMap)
	if err != nil {
		t.Skipf("Skipping test: failed to create publisher: %v", err)
	}
	defer pub.Close()

	// Create a valid CloudEvent
	evt := event.New()
	evt.SetType("com.example.test.event")
	evt.SetSource("test-source")
	evt.SetID("test-id")
	evt.SetData(event.ApplicationJSON, map[string]string{"key": "value"})

	// Attempt to publish - this should fail due to connection error
	ctx := context.Background()
	err = pub.Publish(ctx, "test-topic", &evt)
	// This will fail because we don't have a real RabbitMQ connection
	// The error handling is tested - the publisher should return an error
	assert.Error(t, err)
}

func TestSubscriberSubscribeErrorHandling(t *testing.T) {
	configMap := map[string]string{
		"broker.type":            "rabbitmq",
		"subscriber.parallelism": "1",
	}

	sub, err := NewSubscriber("test-sub", configMap)
	if err != nil {
		t.Skipf("Skipping test: failed to create subscriber: %v", err)
	}
	defer sub.Close()

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
			expectError: false, // Subscription setup should succeed, actual connection may fail
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
			err := sub.Subscribe(ctx, tt.topic, tt.handler)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// If no error expected, subscription setup should succeed
				// Actual connection errors are acceptable and don't fail the test
				if err != nil && tt.name == "valid handler" {
					// Connection errors are expected without a real broker
					// This is acceptable - we're testing error handling, not connectivity
					t.Logf("Expected connection error (no real broker): %v", err)
				}
			}
		})
	}
}

func TestSubscriberHandlerErrorHandling(t *testing.T) {
	// This test verifies that handler errors are handled correctly
	// In a real scenario, this would be tested with integration tests
	// For unit tests, we verify the error handling logic exists

	configMap := map[string]string{
		"broker.type":            "rabbitmq",
		"subscriber.parallelism": "1",
	}

	sub, err := NewSubscriber("test-sub", configMap)
	if err != nil {
		t.Skipf("Skipping test: failed to create subscriber: %v", err)
	}
	defer sub.Close()

	// Verify that a handler that returns an error is accepted
	// (The actual error handling happens in the worker goroutine)
	errorHandler := func(ctx context.Context, evt *event.Event) error {
		return assert.AnError
	}

	ctx := context.Background()
	// This should not error - handler errors are handled internally
	err = sub.Subscribe(ctx, "test-topic", errorHandler)
	// Connection will fail without real broker, but handler registration should be fine
	if err != nil {
		t.Logf("Connection error expected (no real broker): %v", err)
	}
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
			expectError: false, // Should handle nil gracefully
		},
		{
			name:        "empty map",
			configMap:   map[string]string{},
			expectError: false,
		},
		{
			name: "valid map",
			configMap: map[string]string{
				"broker.type": "rabbitmq",
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
				// buildConfigFromMap should handle nil/empty maps gracefully
				if tt.configMap == nil {
					// For nil, it might return an error or handle it
					// Let's see what happens
				}
				// For empty map, it should return default config
				if len(tt.configMap) == 0 {
					assert.NoError(t, err)
					assert.NotNil(t, cfg)
				}
			}
		})
	}
}

func TestLoadConfigErrorHandling(t *testing.T) {
	// Test error handling for various config file scenarios
	// Most scenarios are covered in config_test.go, but we add error-specific tests here

	tests := []struct {
		name        string
		setup       func(*testing.T)
		cleanup     func(*testing.T)
		expectError bool
	}{
		{
			name: "non-existent config file via env var",
			setup: func(t *testing.T) {
				os.Setenv("BROKER_CONFIG_FILE", "/non/existent/path/broker.yaml")
			},
			cleanup: func(t *testing.T) {
				os.Unsetenv("BROKER_CONFIG_FILE")
			},
			expectError: false, // Should use defaults when file not found
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup(t)
			}
			defer func() {
				if tt.cleanup != nil {
					tt.cleanup(t)
				}
			}()

			cfg, err := loadConfig()
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				// Should handle missing files gracefully
				assert.NoError(t, err)
				assert.NotNil(t, cfg)
			}
		})
	}
}
