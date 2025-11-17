package broker

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	googlepubsub "github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
)

// newGooglePubSubPublisher creates a Google Pub/Sub publisher
func newGooglePubSubPublisher(cfg *config, logger watermill.LoggerAdapter) (message.Publisher, error) {
	pubsubConfig := googlepubsub.PublisherConfig{
		ProjectID: cfg.Broker.GooglePubSub.ProjectID,
	}

	return googlepubsub.NewPublisher(pubsubConfig, logger)
}

// newGooglePubSubSubscriber creates a Google Pub/Sub subscriber
func newGooglePubSubSubscriber(cfg *config, logger watermill.LoggerAdapter, subscriptionId string) (message.Subscriber, error) {
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
