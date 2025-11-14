package broker

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	amqp "github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	googlepubsub "github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	DefaultSubscriberParallelism = 10
	DefaultTestParallelism       = 1
	DefaultShutdownTimeout       = 30 * time.Second
)

// NewPublisher creates a new publisher based on the configuration.
// If configMap is provided, it will be used instead of loading from file.
// Config keys should use dot notation (e.g., "broker.type", "broker.rabbitmq.url").
func NewPublisher(configMap ...map[string]string) (Publisher, error) {
	var cfg *config

	if len(configMap) > 0 && configMap[0] != nil {
		var err error
		cfg, err = buildConfigFromMap(configMap[0])
		if err != nil {
			return nil, fmt.Errorf("failed to build config from map: %w", err)
		}
	} else {
		var err error
		cfg, err = loadConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load config: %w", err)
		}
	}

	logger := watermill.NewStdLogger(false, false)

	// Log configuration before creating publisher if enabled
	if cfg.LogConfig {
		logConfiguration(cfg, "Publisher", logger)
	}

	var pub message.Publisher
	var err error

	switch cfg.Broker.Type {
	case "rabbitmq":
		pub, err = newRabbitMQPublisher(cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create RabbitMQ publisher: %w", err)
		}
	case "googlepubsub":
		pub, err = newGooglePubSubPublisher(cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create Google Pub/Sub publisher: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", cfg.Broker.Type)
	}

	return &publisher{pub: pub}, nil
}

// NewSubscriber creates a new subscriber based on the configuration.
// subscriptionId determines whether subscribers share messages (same ID = shared, different IDs = separate).
// If configMap is provided, it will be used instead of loading from file.
// Config keys should use dot notation (e.g., "broker.type", "broker.rabbitmq.url").
func NewSubscriber(subscriptionId string, configMap ...map[string]string) (Subscriber, error) {
	if subscriptionId == "" {
		return nil, fmt.Errorf("subscriptionId is required")
	}

	var cfg *config

	if len(configMap) > 0 && configMap[0] != nil {
		var err error
		cfg, err = buildConfigFromMap(configMap[0])
		if err != nil {
			return nil, fmt.Errorf("failed to build config from map: %w", err)
		}
	} else {
		var err error
		cfg, err = loadConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load config: %w", err)
		}
	}

	logger := watermill.NewStdLogger(false, false)

	// Log configuration before creating subscriber if enabled
	if cfg.LogConfig {
		logConfiguration(cfg, "Subscriber", logger)
	}

	var sub message.Subscriber
	var err error

	switch cfg.Broker.Type {
	case "rabbitmq":
		sub, err = newRabbitMQSubscriber(cfg, logger, subscriptionId)
		if err != nil {
			return nil, fmt.Errorf("failed to create RabbitMQ subscriber: %w", err)
		}
	case "googlepubsub":
		sub, err = newGooglePubSubSubscriber(cfg, logger, subscriptionId)
		if err != nil {
			return nil, fmt.Errorf("failed to create Google Pub/Sub subscriber: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", cfg.Broker.Type)
	}

	parallelism := cfg.Subscriber.Parallelism
	if parallelism <= 0 {
		parallelism = DefaultSubscriberParallelism
	}
	return &subscriber{
		sub:            sub,
		parallelism:    parallelism,
		subscriptionID: subscriptionId,
		logger:         logger,
	}, nil
}

// newRabbitMQPublisher creates a RabbitMQ publisher
func newRabbitMQPublisher(cfg *config, logger watermill.LoggerAdapter) (message.Publisher, error) {
	amqpConfig := amqp.NewDurablePubSubConfig(
		cfg.Broker.RabbitMQ.URL,
		amqp.GenerateQueueNameTopicNameWithSuffix(""),
	)

	// Override defaults with config values if provided
	if cfg.Broker.RabbitMQ.ExchangeType != "" {
		amqpConfig.Exchange.Type = cfg.Broker.RabbitMQ.ExchangeType
	}
	if cfg.Broker.RabbitMQ.Exchange != "" {
		// Use static exchange name if provided
		exchangeName := cfg.Broker.RabbitMQ.Exchange
		amqpConfig.Exchange.GenerateName = func(topic string) string {
			return exchangeName
		}
	}
	if cfg.Broker.RabbitMQ.RoutingKey != "" {
		// Use static routing key if provided
		routingKey := cfg.Broker.RabbitMQ.RoutingKey
		amqpConfig.Publish.GenerateRoutingKey = func(topic string) string {
			return routingKey
		}
	}
	if cfg.Broker.RabbitMQ.PublisherConfirm {
		amqpConfig.Publish.ConfirmDelivery = true
	}

	return amqp.NewPublisher(amqpConfig, logger)
}

// newRabbitMQSubscriber creates a RabbitMQ subscriber
func newRabbitMQSubscriber(cfg *config, logger watermill.LoggerAdapter, subscriptionId string) (message.Subscriber, error) {
	// Create a queue name generator that incorporates subscription IDs
	// The topic passed to Subscribe will be the original topic, and we append subscription ID for queue naming
	queueNameGenerator := func(topic string) string {
		if cfg.Broker.RabbitMQ.Queue != "" {
			// If a static queue name is provided, use it as base and append subscription ID
			// This allows subscribers with the same subscriptionId to share the same queue
			return fmt.Sprintf("%s-%s", cfg.Broker.RabbitMQ.Queue, subscriptionId)
		}
		// Generate queue name: "topic-subscriptionId"
		// This allows subscribers with the same subscriptionId to share the same queue
		return fmt.Sprintf("%s-%s", topic, subscriptionId)
	}

	amqpConfig := amqp.NewDurablePubSubConfig(
		cfg.Broker.RabbitMQ.URL,
		queueNameGenerator,
	)

	// Override defaults with config values if provided
	if cfg.Broker.RabbitMQ.ExchangeType != "" {
		amqpConfig.Exchange.Type = cfg.Broker.RabbitMQ.ExchangeType
	}
	if cfg.Broker.RabbitMQ.Exchange != "" {
		// Use static exchange name if provided
		exchangeName := cfg.Broker.RabbitMQ.Exchange
		amqpConfig.Exchange.GenerateName = func(topic string) string {
			return exchangeName
		}
	}
	if cfg.Broker.RabbitMQ.PrefetchCount > 0 {
		amqpConfig.Consume.Qos.PrefetchCount = cfg.Broker.RabbitMQ.PrefetchCount
	}
	if cfg.Broker.RabbitMQ.PrefetchSize > 0 {
		amqpConfig.Consume.Qos.PrefetchSize = cfg.Broker.RabbitMQ.PrefetchSize
	}
	if cfg.Broker.RabbitMQ.ConsumerTag != "" {
		amqpConfig.Consume.Consumer = cfg.Broker.RabbitMQ.ConsumerTag
	}

	return amqp.NewSubscriber(amqpConfig, logger)
}

// newGooglePubSubPublisher creates a Google Pub/Sub publisher
func newGooglePubSubPublisher(cfg *config, logger watermill.LoggerAdapter) (message.Publisher, error) {
	if cfg.Broker.GooglePubSub.ProjectID == "" {
		return nil, fmt.Errorf("googlepubsub.project_id is required")
	}

	pubsubConfig := googlepubsub.PublisherConfig{
		ProjectID: cfg.Broker.GooglePubSub.ProjectID,
	}

	return googlepubsub.NewPublisher(pubsubConfig, logger)
}

// newGooglePubSubSubscriber creates a Google Pub/Sub subscriber
func newGooglePubSubSubscriber(cfg *config, logger watermill.LoggerAdapter, subscriptionId string) (message.Subscriber, error) {
	if cfg.Broker.GooglePubSub.ProjectID == "" {
		return nil, fmt.Errorf("googlepubsub.project_id is required")
	}

	// Configure subscription name generator to use subscription ID
	// The topic passed to Subscribe will be the original topic (no colon)
	// We append subscription ID to create unique subscription names
	pubsubConfig := googlepubsub.SubscriberConfig{
		ProjectID: cfg.Broker.GooglePubSub.ProjectID,
		GenerateSubscriptionName: func(topic string) string {
			// Generate subscription name: "topic-subscriptionId"
			// This allows subscribers with the same subscriptionId to share the same subscription
			return fmt.Sprintf("%s-%s", topic, subscriptionId)
		},
	}

	// Set MaxOutstandingMessages if configured
	if cfg.Broker.GooglePubSub.MaxOutstandingMessages > 0 {
		pubsubConfig.ReceiveSettings.MaxOutstandingMessages = cfg.Broker.GooglePubSub.MaxOutstandingMessages
	}

	// Set NumGoroutines if configured
	if cfg.Broker.GooglePubSub.NumGoroutines > 0 {
		pubsubConfig.ReceiveSettings.NumGoroutines = cfg.Broker.GooglePubSub.NumGoroutines
	}

	return googlepubsub.NewSubscriber(pubsubConfig, logger)
}

// logConfiguration logs the complete configuration object as JSON
func logConfiguration(cfg *config, component string, logger watermill.LoggerAdapter) {
	// Create a copy of config with masked password for logging
	logCfg := *cfg
	if cfg.Broker.Type == "rabbitmq" && cfg.Broker.RabbitMQ.URL != "" {
		logCfg.Broker.RabbitMQ.URL = maskPassword(cfg.Broker.RabbitMQ.URL)
	}

	// Marshal to JSON with indentation
	jsonBytes, err := json.MarshalIndent(logCfg, "", "  ")
	if err != nil {
		logger.Error(fmt.Sprintf("Error marshaling %s configuration to JSON", component), err, nil)
		// Fallback to simple logging
		logger.Info(fmt.Sprintf("=== %s Configuration ===", component), nil)
		logger.Info(fmt.Sprintf("Broker Type: %s", cfg.Broker.Type), nil)
		return
	}

	logger.Info(fmt.Sprintf("=== %s Configuration (JSON) ===\n%s\n========================================", component, string(jsonBytes)), nil)
}

// maskPassword masks passwords in URLs for logging
func maskPassword(url string) string {
	if url == "" {
		return ""
	}
	// Look for password pattern in URL (e.g., amqp://user:pass@host)
	if idx := strings.Index(url, "@"); idx > 0 {
		// Find the last colon before @
		if colonIdx := strings.LastIndex(url[:idx], ":"); colonIdx > 0 {
			// Mask the password part
			return url[:colonIdx+1] + "***" + url[idx:]
		}
	}
	return url
}
