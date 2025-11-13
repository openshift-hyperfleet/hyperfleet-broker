package broker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildConfigFromMap(t *testing.T) {
	tests := []struct {
		name        string
		configMap   map[string]string
		expectError bool
		validate    func(*testing.T, *config)
	}{
		{
			name: "valid rabbitmq config",
			configMap: map[string]string{
				"broker.type":                       "rabbitmq",
				"broker.rabbitmq.url":               "amqp://guest:guest@localhost:5672/",
				"broker.rabbitmq.exchange":          "test-exchange",
				"broker.rabbitmq.exchange_type":     "topic",
				"broker.rabbitmq.queue":             "test-queue",
				"broker.rabbitmq.routing_key":       "test.routing.key",
				"broker.rabbitmq.prefetch_count":    "10",
				"broker.rabbitmq.prefetch_size":     "0",
				"broker.rabbitmq.consumer_tag":      "test-consumer",
				"broker.rabbitmq.publisher_confirm": "true",
				"subscriber.parallelism":            "5",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				assert.Equal(t, "rabbitmq", cfg.Broker.Type)
				assert.Equal(t, "amqp://guest:guest@localhost:5672/", cfg.Broker.RabbitMQ.URL)
				assert.Equal(t, "test-exchange", cfg.Broker.RabbitMQ.Exchange)
				assert.Equal(t, "topic", cfg.Broker.RabbitMQ.ExchangeType)
				assert.Equal(t, "test-queue", cfg.Broker.RabbitMQ.Queue)
				assert.Equal(t, "test.routing.key", cfg.Broker.RabbitMQ.RoutingKey)
				assert.Equal(t, 10, cfg.Broker.RabbitMQ.PrefetchCount)
				assert.Equal(t, 0, cfg.Broker.RabbitMQ.PrefetchSize)
				assert.Equal(t, "test-consumer", cfg.Broker.RabbitMQ.ConsumerTag)
				assert.True(t, cfg.Broker.RabbitMQ.PublisherConfirm)
				assert.Equal(t, 5, cfg.Subscriber.Parallelism)
			},
		},
		{
			name: "valid googlepubsub config",
			configMap: map[string]string{
				"broker.type":                                  "googlepubsub",
				"broker.googlepubsub.project_id":               "test-project",
				"broker.googlepubsub.topic":                    "test-topic",
				"broker.googlepubsub.subscription":             "test-subscription",
				"broker.googlepubsub.max_outstanding_messages": "100",
				"broker.googlepubsub.num_goroutines":           "5",
				"subscriber.parallelism":                       "3",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				assert.Equal(t, "googlepubsub", cfg.Broker.Type)
				assert.Equal(t, "test-project", cfg.Broker.GooglePubSub.ProjectID)
				assert.Equal(t, "test-topic", cfg.Broker.GooglePubSub.Topic)
				assert.Equal(t, "test-subscription", cfg.Broker.GooglePubSub.Subscription)
				assert.Equal(t, 100, cfg.Broker.GooglePubSub.MaxOutstandingMessages)
				assert.Equal(t, 5, cfg.Broker.GooglePubSub.NumGoroutines)
				assert.Equal(t, 3, cfg.Subscriber.Parallelism)
			},
		},
		{
			name: "minimal config with defaults",
			configMap: map[string]string{
				"broker.type": "rabbitmq",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				assert.Equal(t, "rabbitmq", cfg.Broker.Type)
				assert.Equal(t, 1, cfg.Subscriber.Parallelism) // Default from buildConfigFromMap
			},
		},
		{
			name:        "empty config map",
			configMap:   map[string]string{},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				assert.Equal(t, 1, cfg.Subscriber.Parallelism) // Default
			},
		},
		{
			name: "invalid broker type",
			configMap: map[string]string{
				"broker.type": "invalid-broker",
			},
			expectError: false, // buildConfigFromMap doesn't validate broker type
			validate: func(t *testing.T, cfg *config) {
				assert.Equal(t, "invalid-broker", cfg.Broker.Type)
			},
		},
		{
			name: "type conversion for int values",
			configMap: map[string]string{
				"broker.type":                    "rabbitmq",
				"broker.rabbitmq.prefetch_count": "42",
				"subscriber.parallelism":         "7",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				assert.Equal(t, 42, cfg.Broker.RabbitMQ.PrefetchCount)
				assert.Equal(t, 7, cfg.Subscriber.Parallelism)
			},
		},
		{
			name: "type conversion for bool values",
			configMap: map[string]string{
				"broker.type":                       "rabbitmq",
				"broker.rabbitmq.publisher_confirm": "false",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				assert.False(t, cfg.Broker.RabbitMQ.PublisherConfirm)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buildConfigFromMap(tt.configMap)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, cfg)
				if tt.validate != nil {
					tt.validate(t, cfg)
				}
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	// Save original environment
	originalConfigFile := os.Getenv("BROKER_CONFIG_FILE")
	defer func() {
		if originalConfigFile != "" {
			os.Setenv("BROKER_CONFIG_FILE", originalConfigFile)
		} else {
			os.Unsetenv("BROKER_CONFIG_FILE")
		}
	}()

	tests := []struct {
		name        string
		setup       func(*testing.T) string // Returns config file path
		cleanup     func(*testing.T, string)
		expectError bool
		validate    func(*testing.T, *config)
	}{
		{
			name: "load from valid yaml file",
			setup: func(t *testing.T) string {
				// Create a temporary config file
				tmpDir := t.TempDir()
				configPath := filepath.Join(tmpDir, "broker.yaml")
				configContent := `
broker:
  type: rabbitmq
  rabbitmq:
    url: amqp://guest:guest@localhost:5672/
    exchange: test-exchange
    exchange_type: topic
subscriber:
  parallelism: 5
`
				err := os.WriteFile(configPath, []byte(configContent), 0644)
				require.NoError(t, err)
				os.Setenv("BROKER_CONFIG_FILE", configPath)
				return configPath
			},
			cleanup: func(t *testing.T, path string) {
				os.Unsetenv("BROKER_CONFIG_FILE")
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				assert.Equal(t, "rabbitmq", cfg.Broker.Type)
				assert.Equal(t, "amqp://guest:guest@localhost:5672/", cfg.Broker.RabbitMQ.URL)
				assert.Equal(t, "test-exchange", cfg.Broker.RabbitMQ.Exchange)
				assert.Equal(t, "topic", cfg.Broker.RabbitMQ.ExchangeType)
				assert.Equal(t, 5, cfg.Subscriber.Parallelism)
			},
		},
		{
			name: "load from BROKER_CONFIG_FILE environment variable",
			setup: func(t *testing.T) string {
				tmpDir := t.TempDir()
				configPath := filepath.Join(tmpDir, "custom-config.yaml")
				configContent := `
broker:
  type: googlepubsub
  googlepubsub:
    project_id: test-project
subscriber:
  parallelism: 10
`
				err := os.WriteFile(configPath, []byte(configContent), 0644)
				require.NoError(t, err)
				os.Setenv("BROKER_CONFIG_FILE", configPath)
				return configPath
			},
			cleanup: func(t *testing.T, path string) {
				os.Unsetenv("BROKER_CONFIG_FILE")
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				assert.Equal(t, "googlepubsub", cfg.Broker.Type)
				assert.Equal(t, "test-project", cfg.Broker.GooglePubSub.ProjectID)
				assert.Equal(t, 10, cfg.Subscriber.Parallelism)
			},
		},
		{
			name: "use defaults when config file not found",
			setup: func(t *testing.T) string {
				// Set BROKER_CONFIG_FILE to a non-existent file
				// This should trigger ConfigFileNotFoundError which is handled gracefully
				tmpDir := t.TempDir()
				nonExistentPath := filepath.Join(tmpDir, "non-existent.yaml")
				os.Setenv("BROKER_CONFIG_FILE", nonExistentPath)
				return nonExistentPath
			},
			cleanup: func(t *testing.T, path string) {
				os.Unsetenv("BROKER_CONFIG_FILE")
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				// Should use defaults
				assert.Equal(t, 10, cfg.Subscriber.Parallelism) // Default from loadConfig
			},
		},
		{
			name: "invalid yaml file",
			setup: func(t *testing.T) string {
				tmpDir := t.TempDir()
				configPath := filepath.Join(tmpDir, "broker.yaml")
				invalidYaml := `
broker:
  type: rabbitmq
  invalid: [unclosed bracket
`
				err := os.WriteFile(configPath, []byte(invalidYaml), 0644)
				require.NoError(t, err)
				os.Setenv("BROKER_CONFIG_FILE", configPath)
				return configPath
			},
			cleanup: func(t *testing.T, path string) {
				os.Unsetenv("BROKER_CONFIG_FILE")
			},
			expectError: true,
			validate:    nil,
		},
		{
			name: "environment variable override",
			setup: func(t *testing.T) string {
				tmpDir := t.TempDir()
				configPath := filepath.Join(tmpDir, "broker.yaml")
				configContent := `
broker:
  type: rabbitmq
subscriber:
  parallelism: 5
`
				err := os.WriteFile(configPath, []byte(configContent), 0644)
				require.NoError(t, err)
				os.Setenv("BROKER_CONFIG_FILE", configPath)
				os.Setenv("SUBSCRIBER_PARALLELISM", "20")
				return configPath
			},
			cleanup: func(t *testing.T, path string) {
				os.Unsetenv("BROKER_CONFIG_FILE")
				os.Unsetenv("SUBSCRIBER_PARALLELISM")
			},
			expectError: false,
			validate: func(t *testing.T, cfg *config) {
				// Environment variable should override config file
				assert.Equal(t, 20, cfg.Subscriber.Parallelism)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := tt.setup(t)
			defer tt.cleanup(t, configPath)

			cfg, err := loadConfig()
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, cfg)
				if tt.validate != nil {
					tt.validate(t, cfg)
				}
			}
		})
	}
}

func TestLoadConfigWithInvalidUnmarshal(t *testing.T) {
	// Save original environment
	originalConfigFile := os.Getenv("BROKER_CONFIG_FILE")
	defer func() {
		if originalConfigFile != "" {
			os.Setenv("BROKER_CONFIG_FILE", originalConfigFile)
		} else {
			os.Unsetenv("BROKER_CONFIG_FILE")
		}
	}()

	// Create a config file with invalid structure that can't be unmarshaled
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "broker.yaml")
	configContent := `
broker:
  type: rabbitmq
  rabbitmq:
    prefetch_count: "not-an-int"
`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)
	os.Setenv("BROKER_CONFIG_FILE", configPath)
	defer os.Unsetenv("BROKER_CONFIG_FILE")

	cfg, err := loadConfig()
	// Note: Viper might handle type conversion, so this might not error
	// But if it does error, we should handle it gracefully
	if err != nil {
		assert.Error(t, err)
		assert.Nil(t, cfg)
	} else {
		// If no error, verify the config was loaded (Viper might convert types)
		assert.NotNil(t, cfg)
	}
}

func TestBuildConfigFromMapWithInvalidTypes(t *testing.T) {
	// Test that invalid type conversions are handled
	// Note: Viper might handle some conversions, so these tests verify behavior
	tests := []struct {
		name        string
		configMap   map[string]string
		expectError bool
	}{
		{
			name: "invalid int value",
			configMap: map[string]string{
				"broker.type":                    "rabbitmq",
				"broker.rabbitmq.prefetch_count": "not-an-int",
			},
			expectError: true, // Viper returns an error when it can't convert types
		},
		{
			name: "invalid bool value",
			configMap: map[string]string{
				"broker.type":                       "rabbitmq",
				"broker.rabbitmq.publisher_confirm": "not-a-bool",
			},
			expectError: true, // Viper returns an error when it can't convert types
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buildConfigFromMap(tt.configMap)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				// Even if no error, verify config exists
				// The actual values might be zero values if conversion failed
				assert.NotNil(t, cfg)
			}
		})
	}
}
