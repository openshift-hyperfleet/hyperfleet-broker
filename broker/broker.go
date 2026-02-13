package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
)

const (
	DefaultSubscriberParallelism = 1
	DefaultTestParallelism       = 1
	DefaultShutdownTimeout       = 30 * time.Second
)

// NewPublisher creates a new publisher with a required logger and optional configuration.
// Usage:
//   - NewPublisher(logger) - uses provided logger and loads config from file
//   - NewPublisher(logger, configMap) - uses provided logger with config map
func NewPublisher(log logger.Logger, configMap ...map[string]string) (Publisher, error) {
	if log == nil {
		return nil, fmt.Errorf("logger is required")
	}

	var cfg *config
	var err error

	if len(configMap) > 0 && configMap[0] != nil {
		cfg, err = buildConfigFromMap(configMap[0])
		if err != nil {
			return nil, fmt.Errorf("failed to build config from map: %w", err)
		}
	} else {
		cfg, err = loadConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load config: %w", err)
		}
	}

	// Create Watermill logger adapter with context.Background().
	// Unlike Subscriber which creates a per-call adapter with the incoming context,
	// Publisher uses a long-lived adapter because Watermill's publisher is created once
	// and reused - it doesn't support per-call logger injection. The adapter is only
	// used for Watermill's internal logging (connection setup, batching, etc.), not
	// for application-level publish logging which uses the broker logger directly.
	watermillLogger := logger.NewWatermillLoggerAdapter(log, context.Background())

	// Log configuration if enabled
	if cfg.LogConfig {
		log.Info(context.Background(), "Creating publisher")
		logConfiguration(cfg, "Publisher", watermillLogger)
	}

	var pub message.Publisher
	var hc healthCheckFunc
	var healthCloser io.Closer

	switch cfg.Broker.Type {
	case "rabbitmq":
		pub, err = newRabbitMQPublisher(cfg, watermillLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create RabbitMQ publisher: %w", err)
		}
		hc = newRabbitMQHealthCheck(pub)
	case "googlepubsub":
		pub, err = newGooglePubSubPublisher(cfg, watermillLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create Google Pub/Sub publisher: %w", err)
		}

		// Create a persistent Pub/Sub client for health checks, reused across calls.
		var healthClient *pubsub.Client
		healthClient, err = pubsub.NewClient(context.Background(), cfg.Broker.GooglePubSub.ProjectID)
		if err != nil {
			if closeErr := pub.Close(); closeErr != nil {
				return nil, fmt.Errorf("failed to create health check client: %w (also failed to close publisher: %v)", err, closeErr)
			}
			return nil, fmt.Errorf("failed to create health check client: %w", err)
		}
		hc = newGooglePubSubHealthCheck(healthClient, cfg.Broker.GooglePubSub.ProjectID)
		healthCloser = healthClient
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", cfg.Broker.Type)
	}

	return &publisher{
		pub:          pub,
		logger:       log,
		healthCheck:  hc,
		healthCloser: healthCloser,
	}, nil
}

// NewSubscriber creates a new subscriber with a required logger and optional configuration.
// Usage:
//   - NewSubscriber(logger, "id") - uses provided logger and loads config from file
//   - NewSubscriber(logger, "id", configMap) - uses provided logger with config map
func NewSubscriber(log logger.Logger, subscriptionID string, configMap ...map[string]string) (Subscriber, error) {
	if subscriptionID == "" {
		return nil, fmt.Errorf("subscriptionID is required")
	}
	if log == nil {
		return nil, fmt.Errorf("logger is required")
	}

	var cfg *config
	var err error

	if len(configMap) > 0 && configMap[0] != nil {
		cfg, err = buildConfigFromMap(configMap[0])
		if err != nil {
			return nil, fmt.Errorf("failed to build config from map: %w", err)
		}
	} else {
		cfg, err = loadConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load config: %w", err)
		}
	}

	// Create Watermill logger adapter with context.Background() for subscriber creation.
	// This is used for Watermill's internal subscriber logging (connection, queue management).
	// A separate per-call adapter is created in Subscribe() with the request context
	// for message routing and handling.
	watermillLogger := logger.NewWatermillLoggerAdapter(log, context.Background())

	// Log configuration if enabled
	if cfg.LogConfig {
		log.Info(context.Background(), "Creating subscriber")
		logConfiguration(cfg, "Subscriber", watermillLogger)
	}

	var sub message.Subscriber

	switch cfg.Broker.Type {
	case "rabbitmq":
		sub, err = newRabbitMQSubscriber(cfg, watermillLogger, subscriptionID)
		if err != nil {
			return nil, fmt.Errorf("failed to create RabbitMQ subscriber: %w", err)
		}
	case "googlepubsub":
		sub, err = newGooglePubSubSubscriber(cfg, watermillLogger, subscriptionID)
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
		subscriptionID: subscriptionID,
		logger:         log,
		errorChan:      make(chan *SubscriberError, ErrorChannelBufferSize),
	}, nil
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

// messageToEvent converts a Watermill message to a CloudEvent
func messageToEvent(msg *message.Message) (*event.Event, error) {
	evt := event.New()

	// Unmarshal the event from the payload (Structured Content Mode)
	if err := json.Unmarshal(msg.Payload, &evt); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event from JSON: %w", err)
	}

	return &evt, nil
}

// eventToMessage converts a CloudEvent to a Watermill message
func eventToMessage(evt *event.Event) (*message.Message, error) {
	// Marshal the event to JSON (Structured Content Mode)
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	msg := message.NewMessage(evt.ID(), payload)
	return msg, nil
}
