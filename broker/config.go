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
	Broker     brokerConfig     `mapstructure:"broker"`
	Subscriber subscriberConfig `mapstructure:"subscriber"`
	LogConfig  bool             `mapstructure:"log_config"`
}

// brokerConfig holds broker-specific configuration
type brokerConfig struct {
	Type         string             `mapstructure:"type"`
	Postgres     postgresConfig     `mapstructure:"postgres"`
	RabbitMQ     rabbitMQConfig     `mapstructure:"rabbitmq"`
	GooglePubSub googlePubSubConfig `mapstructure:"googlepubsub"`
}

// subscriberConfig holds subscriber-specific configuration
type subscriberConfig struct {
	Parallelism int `mapstructure:"parallelism"`
}

// resolveConfig builds the configuration from configMap when one is given,
// and otherwise loads it from broker.yaml and the environment.
func resolveConfig(configMap ...map[string]string) (*config, error) {
	if len(configMap) > 0 && configMap[0] != nil {
		cfg, err := buildConfigFromMap(configMap[0])
		if err != nil {
			return nil, fmt.Errorf("failed to build config from map: %w", err)
		}
		return cfg, nil
	}
	cfg, err := loadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	return cfg, nil
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
	if err := bindPostgresEnv(v); err != nil {
		return nil, err
	}

	// Set defaults
	v.SetDefault("log_config", false)
	v.SetDefault("subscriber.parallelism", DefaultSubscriberParallelism)
	v.SetDefault("broker.postgres.password", "")

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
	v.SetDefault("broker.postgres.password", "")

	// Set values from the map
	// Viper will handle type conversion from string to int/bool during Unmarshal
	for key, value := range configMap {
		v.Set(key, value)
	}

	// Enable environment variable overrides; they fill keys the map does
	// not set, since map values are viper override-layer writes that take
	// precedence over the environment.
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	if err := bindPostgresEnv(v); err != nil {
		return nil, err
	}

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
	case brokerTypeRabbitMQ:
		return validateRabbitMQConfig(cfg)
	case brokerTypeGooglePubSub:
		return validateGooglePubSubConfig(cfg)
	case brokerTypePostgres:
		return validatePostgresConfig(cfg)
	default:
		return fmt.Errorf("unsupported broker type: %s", cfg.Broker.Type)
	}
}

// postgresEnvKeys are bound explicitly because AutomaticEnv only
// overrides keys Viper already knows about; without a registered
// default or config-file entry the broker.postgres block stays
// invisible to the environment.
var postgresEnvKeys = []string{
	"broker.type",
	"broker.postgres.host", "broker.postgres.port",
	"broker.postgres.database", "broker.postgres.user",
	"broker.postgres.password", "broker.postgres.sslmode",
	"broker.postgres.ca_cert_file", "broker.postgres.pool_max_conns",
	"broker.postgres.poll_interval", "broker.postgres.fetch_cooldown",
	"broker.postgres.batch_size", "broker.postgres.grace_period",
	"broker.postgres.lease_duration", "broker.postgres.max_attempts",
	"broker.postgres.retry_backoff", "broker.postgres.retention",
}

// bindPostgresEnv wires the postgres configuration keys to their
// BROKER_POSTGRES_* environment variables.
func bindPostgresEnv(v *viper.Viper) error {
	for _, key := range postgresEnvKeys {
		if err := v.BindEnv(key); err != nil {
			return fmt.Errorf("failed to bind %s to its environment variable: %w", key, err)
		}
	}
	return nil
}
