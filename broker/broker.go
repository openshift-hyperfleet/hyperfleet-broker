package broker

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
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

const (
	// CloudEvents metadata keys
	metadataTypeKey            = "cloudevents.type"
	metadataSourceKey          = "cloudevents.source"
	metadataIDKey              = "cloudevents.id"
	metadataSpecVersionKey     = "cloudevents.specversion"
	metadataTimeKey            = "cloudevents.time"
	metadataSubjectKey         = "cloudevents.subject"
	metadataDataSchemaKey      = "cloudevents.dataschema"
	metadataDataContentTypeKey = "cloudevents.datacontenttype"
)

// messageToEvent converts a Watermill message to a CloudEvent
func messageToEvent(msg *message.Message) (*event.Event, error) {
	evt := event.New()

	// Extract CloudEvents attributes from metadata
	if eventType := msg.Metadata.Get(metadataTypeKey); eventType != "" {
		evt.SetType(eventType)
	} else {
		return nil, fmt.Errorf("missing required CloudEvents attribute: type")
	}

	if source := msg.Metadata.Get(metadataSourceKey); source != "" {
		evt.SetSource(source)
	}

	if id := msg.Metadata.Get(metadataIDKey); id != "" {
		evt.SetID(id)
	} else {
		// Use Watermill UUID as fallback
		evt.SetID(msg.UUID)
	}

	if specVersion := msg.Metadata.Get(metadataSpecVersionKey); specVersion != "" {
		evt.SetSpecVersion(specVersion)
	} else {
		evt.SetSpecVersion(event.CloudEventsVersionV1)
	}

	if timeStr := msg.Metadata.Get(metadataTimeKey); timeStr != "" {
		// Parse RFC3339 time format
		if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
			evt.SetTime(t)
		} else {
			// Try parsing as string representation (fallback)
			if t, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
				evt.SetTime(t)
			}
		}
	}

	if subject := msg.Metadata.Get(metadataSubjectKey); subject != "" {
		evt.SetSubject(subject)
	}

	if dataSchema := msg.Metadata.Get(metadataDataSchemaKey); dataSchema != "" {
		evt.SetDataSchema(dataSchema)
	}

	contentType := msg.Metadata.Get(metadataDataContentTypeKey)
	if contentType == "" {
		contentType = cloudevents.ApplicationJSON
	}
	evt.SetDataContentType(contentType)

	// Set the data payload (JSON only)
	if len(msg.Payload) > 0 {
		// Unmarshal as JSON
		var data interface{}
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}
		if err := evt.SetData(contentType, data); err != nil {
			return nil, fmt.Errorf("failed to set event data: %w", err)
		}
	}

	// Copy any additional metadata as extensions (after setting data)
	for key, value := range msg.Metadata {
		if !isCloudEventsAttribute(key) {
			evt.SetExtension(key, value)
		}
	}

	return &evt, nil
}

// eventToMessage converts a CloudEvent to a Watermill message
func eventToMessage(evt *event.Event) (*message.Message, error) {
	msg := message.NewMessage(evt.ID(), nil)

	// Set CloudEvents attributes as metadata
	msg.Metadata.Set(metadataTypeKey, evt.Type())
	msg.Metadata.Set(metadataSourceKey, evt.Source())
	msg.Metadata.Set(metadataIDKey, evt.ID())
	msg.Metadata.Set(metadataSpecVersionKey, evt.SpecVersion())

	if !evt.Time().IsZero() {
		msg.Metadata.Set(metadataTimeKey, evt.Time().Format(time.RFC3339))
	}

	if evt.Subject() != "" {
		msg.Metadata.Set(metadataSubjectKey, evt.Subject())
	}

	if evt.DataSchema() != "" {
		msg.Metadata.Set(metadataDataSchemaKey, evt.DataSchema())
	}

	contentType := evt.DataContentType()
	if contentType != "" {
		msg.Metadata.Set(metadataDataContentTypeKey, contentType)
	}

	// Set the data payload (JSON only)
	if evt.Data() != nil {
		// CloudEvents SDK returns JSON-encoded bytes for JSON data
		msg.Payload = evt.Data()
	}

	// Copy extensions as metadata
	for key, value := range evt.Extensions() {
		msg.Metadata.Set(key, fmt.Sprintf("%v", value))
	}

	return msg, nil
}

// isCloudEventsAttribute checks if a metadata key is a standard CloudEvents attribute
func isCloudEventsAttribute(key string) bool {
	standardAttributes := []string{
		metadataTypeKey,
		metadataSourceKey,
		metadataIDKey,
		metadataSpecVersionKey,
		metadataTimeKey,
		metadataSubjectKey,
		metadataDataSchemaKey,
		metadataDataContentTypeKey,
	}

	for _, attr := range standardAttributes {
		if key == attr {
			return true
		}
	}
	return false
}
