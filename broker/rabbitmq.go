package broker

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	amqp "github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

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
	if cfg.Subscriber.Parallelism > 0 {
		amqpConfig.Consume.Qos.PrefetchCount = cfg.Subscriber.Parallelism
	}
	/*
	 */
	if cfg.Broker.RabbitMQ.PrefetchSize > 0 {
		amqpConfig.Consume.Qos.PrefetchSize = cfg.Broker.RabbitMQ.PrefetchSize
	}
	if cfg.Broker.RabbitMQ.ConsumerTag != "" {
		amqpConfig.Consume.Consumer = cfg.Broker.RabbitMQ.ConsumerTag
	}

	return amqp.NewSubscriber(amqpConfig, logger)
}
