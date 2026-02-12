package broker

import (
	"context"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	amqp "github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

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

// newRabbitMQHealthCheck creates a health check function for a RabbitMQ publisher.
// It uses the AMQP ConnectionWrapper's IsConnected() and Closed() methods
// to determine if the broker connection is alive.
func newRabbitMQHealthCheck(pub message.Publisher) healthCheckFunc {
	return func(_ context.Context) error {
		amqpPub, ok := pub.(*amqp.Publisher)
		if !ok {
			return fmt.Errorf("unexpected publisher type for RabbitMQ health check")
		}
		if amqpPub == nil {
			return fmt.Errorf("RabbitMQ publisher is nil")
		}
		if amqpPub.Closed() {
			return fmt.Errorf("RabbitMQ connection is closed")
		}
		if !amqpPub.IsConnected() {
			return fmt.Errorf("RabbitMQ connection is not established")
		}
		return nil
	}
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
func newRabbitMQSubscriber(cfg *config, logger watermill.LoggerAdapter, subscriptionID string) (message.Subscriber, error) {
	// Create a queue name generator that incorporates subscription IDs
	// The topic passed to Subscribe will be the original topic, and we append subscription ID for queue naming
	queueNameGenerator := func(topic string) string {
		if cfg.Broker.RabbitMQ.Queue != "" {
			// If a static queue name is provided, use it as base and append subscription ID
			// This allows subscribers with the same subscriptionID to share the same queue
			return fmt.Sprintf("%s-%s", cfg.Broker.RabbitMQ.Queue, subscriptionID)
		}
		// Generate queue name: "topic-subscriptionID"
		// This allows subscribers with the same subscriptionID to share the same queue
		return fmt.Sprintf("%s-%s", topic, subscriptionID)
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
