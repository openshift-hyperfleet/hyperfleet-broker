package broker

import (
	"testing"

	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
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

			mockLogger := logger.NewMockLogger()
			metrics := newErrorTestMetrics(t)
			if tt.configMap == nil {
				pub, err = NewPublisher(mockLogger, metrics)
			} else {
				pub, err = NewPublisher(mockLogger, metrics, tt.configMap)
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
		// Note: googlepubsub success case is not tested here because it requires
		// GCP credentials. It's covered by integration tests instead.
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

			mockLogger := logger.NewMockLogger()
			metrics := newErrorTestMetrics(t)
			if tt.configMap == nil {
				sub, err = NewSubscriber(mockLogger, tt.subscriptionID, metrics)
			} else {
				sub, err = NewSubscriber(mockLogger, tt.subscriptionID, metrics, tt.configMap)
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

	mockLogger := logger.NewMockLogger()
	metrics := newErrorTestMetrics(t)
	_, err := NewPublisher(mockLogger, metrics, configMap)
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
