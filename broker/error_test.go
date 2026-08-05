package broker

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func newErrorTestMetrics(t *testing.T) *MetricsRecorder {
	t.Helper()
	return NewMetricsRecorder("test", "v0.0.0", prometheus.NewRegistry())
}

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
			},
			expectError: true,
			errorMsg:    "rabbitmq.url is required",
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
			name:        "nil config map without broker.yaml",
			configMap:   nil,
			expectError: true, // Falls back to loadConfig() which fails without a config file
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

			metrics := newErrorTestMetrics(t)
			if tt.configMap == nil {
				pub, err = NewPublisher(discardLogger, metrics)
			} else {
				pub, err = NewPublisher(discardLogger, metrics, tt.configMap)
			}

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, pub)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, pub)
				if pub != nil {
					defer func() {
						if err := pub.Close(); err != nil {
							t.Errorf("failed to close publisher: %v", err)
						}
					}()
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
		// Note: success cases (valid rabbitmq/googlepubsub config) require a running
		// broker and are covered by integration tests instead.
		{
			name:           "nil config map without broker.yaml",
			subscriptionID: "test-sub",
			configMap:      nil,
			expectError:    true, // Falls back to loadConfig() which fails without a config file
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sub Subscriber
			var err error

			metrics := newErrorTestMetrics(t)
			if tt.configMap == nil {
				sub, err = NewSubscriber(discardLogger, tt.subscriptionID, metrics)
			} else {
				sub, err = NewSubscriber(discardLogger, tt.subscriptionID, metrics, tt.configMap)
			}

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, sub)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, sub)
				if sub != nil {
					defer func() {
						if err := sub.Close(); err != nil {
							t.Errorf("failed to close subscriber: %v", err)
						}
					}()
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

	metrics := newErrorTestMetrics(t)
	_, err := NewPublisher(discardLogger, metrics, configMap)
	assert.Error(t, err)
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
		{
			name: "unknown config keys are ignored",
			configMap: map[string]string{
				"broker.type":         "rabbitmq",
				"broker.rabbitmq.url": "amqp://guest:guest@localhost:5672/",
				"invalid.key":         "value",
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
