package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// config holds the broker configuration
type config struct {
	LogConfig  bool             `mapstructure:"log_config"`
	Broker     brokerConfig     `mapstructure:"broker"`
	Subscriber subscriberConfig `mapstructure:"subscriber"`
}

// brokerConfig holds broker-specific configuration
type brokerConfig struct {
	Type         string             `mapstructure:"type"`
	RabbitMQ     rabbitMQConfig     `mapstructure:"rabbitmq"`
	GooglePubSub googlePubSubConfig `mapstructure:"googlepubsub"`
}

// rabbitMQConfig holds RabbitMQ-specific configuration
type rabbitMQConfig struct {
	URL              string `mapstructure:"url"`
	Exchange         string `mapstructure:"exchange"`
	ExchangeType     string `mapstructure:"exchange_type"`
	Queue            string `mapstructure:"queue"`
	RoutingKey       string `mapstructure:"routing_key"`
	PrefetchCount    int    `mapstructure:"prefetch_count"`
	PrefetchSize     int    `mapstructure:"prefetch_size"`
	ConsumerTag      string `mapstructure:"consumer_tag"`
	PublisherConfirm bool   `mapstructure:"publisher_confirm"`
}

// googlePubSubConfig holds Google Pub/Sub-specific configuration
type googlePubSubConfig struct {
	ProjectID              string `mapstructure:"project_id"`
	Topic                  string `mapstructure:"topic"`
	Subscription           string `mapstructure:"subscription"`
	MaxOutstandingMessages int    `mapstructure:"max_outstanding_messages"`
	NumGoroutines          int    `mapstructure:"num_goroutines"`
}

// subscriberConfig holds subscriber-specific configuration
type subscriberConfig struct {
	Parallelism int `mapstructure:"parallelism"`
}

// loadConfig reads the configuration from broker.yaml and environment variables
func loadConfig() (*config, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	// Check for BROKER_CONFIG_FILE environment variable
	configFile := os.Getenv("BROKER_CONFIG_FILE")
	if configFile != "" {
		// Use the specified config file path
		v.SetConfigFile(configFile)
	} else {
		// Default: look for broker.yaml in the same folder as the application
		// First try to get the executable directory
		execPath, err := os.Executable()
		if err == nil {
			// Resolve symlinks to get the actual executable path
			execPath, err = filepath.EvalSymlinks(execPath)
			if err == nil {
				execDir := filepath.Dir(execPath)
				configPath := filepath.Join(execDir, "broker.yaml")
				v.SetConfigFile(configPath)
			} else {
				// Fallback to current directory
				v.SetConfigName("broker")
				v.AddConfigPath(".")
			}
		} else {
			// Fallback to current directory if we can't determine executable path
			v.SetConfigName("broker")
			v.AddConfigPath(".")
		}
	}

	// Enable environment variable overrides
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Set defaults
	v.SetDefault("log_config", false)
	v.SetDefault("subscriber.parallelism", 10)

	// Read config file (optional - will use defaults if not found)
	if err := v.ReadInConfig(); err != nil {
		// Check if it's a ConfigFileNotFoundError (when using SetConfigName/AddConfigPath)
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// File not found is OK, will use defaults
		} else if os.IsNotExist(err) {
			// File not found when using SetConfigFile with specific path is also OK
			// Will use defaults
		} else {
			// Other errors (permission denied, invalid YAML, etc.) should be returned
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	var cfg config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Validate configuration
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// buildConfigFromMap builds a config from a map[string]string.
// Map keys should use dot notation matching the config structure (e.g., "broker.type", "broker.rabbitmq.url").
// String values will be automatically converted to appropriate types (int, bool) during unmarshaling.
func buildConfigFromMap(configMap map[string]string) (*config, error) {
	// Create a new viper instance to avoid conflicts with the package-level viper instance
	v := viper.New()
	v.SetConfigType("yaml")

	// Set defaults
	v.SetDefault("log_config", false)
	v.SetDefault("subscriber.parallelism", DefaultSubscriberParallelism)

	// Set values from the map
	// Viper will handle type conversion from string to int/bool during Unmarshal
	for key, value := range configMap {
		v.Set(key, value)
	}

	// Enable environment variable overrides (they can still override map values)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	var cfg config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config from map: %w", err)
	}

	// Validate configuration
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// validateConfig validates the configuration based on the broker type
func validateConfig(cfg *config) error {
	switch cfg.Broker.Type {
	case "rabbitmq":
		return validateRabbitMQConfig(cfg)
	case "googlepubsub":
		return validateGooglePubSubConfig(cfg)
	default:
		return fmt.Errorf("unsupported broker type: %s", cfg.Broker.Type)
	}
}

// validateRabbitMQConfig validates RabbitMQ configuration
func validateRabbitMQConfig(cfg *config) error {
	rmq := cfg.Broker.RabbitMQ

	// URL is required
	if rmq.URL == "" {
		return fmt.Errorf("rabbitmq.url is required")
	}

	// Validate URL format (should start with amqp:// or amqps://)
	if !strings.HasPrefix(rmq.URL, "amqp://") && !strings.HasPrefix(rmq.URL, "amqps://") {
		return fmt.Errorf("rabbitmq.url must start with 'amqp://' or 'amqps://'")
	}

	// Validate exchange type if provided
	if rmq.ExchangeType != "" {
		validExchangeTypes := map[string]bool{
			"direct":  true,
			"fanout":  true,
			"topic":   true,
			"headers": true,
		}
		if !validExchangeTypes[rmq.ExchangeType] {
			return fmt.Errorf("rabbitmq.exchange_type must be one of: direct, fanout, topic, headers")
		}
	}

	// Validate prefetch_count (must be non-negative)
	if rmq.PrefetchCount < 0 {
		return fmt.Errorf("rabbitmq.prefetch_count must be non-negative")
	}

	// Validate prefetch_size (must be non-negative)
	if rmq.PrefetchSize < 0 {
		return fmt.Errorf("rabbitmq.prefetch_size must be non-negative")
	}

	return nil
}

// validateGooglePubSubConfig validates Google Pub/Sub configuration
func validateGooglePubSubConfig(cfg *config) error {
	gps := cfg.Broker.GooglePubSub

	// ProjectID is required
	if gps.ProjectID == "" {
		return fmt.Errorf("googlepubsub.project_id is required")
	}

	// Validate MaxOutstandingMessages (must be non-negative if provided)
	if gps.MaxOutstandingMessages < 0 {
		return fmt.Errorf("googlepubsub.max_outstanding_messages must be non-negative")
	}

	// Validate NumGoroutines (must be non-negative if provided)
	if gps.NumGoroutines < 0 {
		return fmt.Errorf("googlepubsub.num_goroutines must be non-negative")
	}

	return nil
}
