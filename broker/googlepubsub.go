package broker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/ThreeDotsLabs/watermill"
	googlepubsub "github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// googlePubSubConfig holds Google Pub/Sub-specific configuration
type googlePubSubConfig struct {
	// Connection settings
	ProjectID string `mapstructure:"project_id"`

	// Subscription settings
	AckDeadlineSeconds       int    `mapstructure:"ack_deadline_seconds"`
	MessageRetentionDuration string `mapstructure:"message_retention_duration"`
	ExpirationTTL            string `mapstructure:"expiration_ttl"`
	EnableMessageOrdering    bool   `mapstructure:"enable_message_ordering"`
	RetryMinBackoff          string `mapstructure:"retry_min_backoff"`
	RetryMaxBackoff          string `mapstructure:"retry_max_backoff"`

	// Dead letter settings
	DeadLetterTopic       string `mapstructure:"dead_letter_topic"`
	DeadLetterMaxAttempts int    `mapstructure:"dead_letter_max_attempts"`

	// Topic settings
	TopicRetentionDuration string `mapstructure:"topic_retention_duration"`

	// Receive settings (client-side flow control)
	MaxOutstandingMessages int `mapstructure:"max_outstanding_messages"`
	MaxOutstandingBytes    int `mapstructure:"max_outstanding_bytes"`
	NumGoroutines          int `mapstructure:"num_goroutines"`

	// Behavior flags (default: false - don't auto-create infrastructure)
	CreateTopicIfMissing        bool `mapstructure:"create_topic_if_missing"`
	CreateSubscriptionIfMissing bool `mapstructure:"create_subscription_if_missing"`
}

// parseGoogleCloudDuration parses a Google Cloud duration string (e.g., "604800s", "10m", "1h")
// and returns a time.Duration. Supports formats: Ns (seconds), Nm (minutes), Nh (hours), Nd (days)
func parseGoogleCloudDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}

	s = strings.TrimSpace(s)
	if s == "0" || s == "0s" {
		return 0, nil
	}

	// Try standard Go duration parsing first (handles "10s", "5m", "1h", etc.)
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Handle "Nd" format for days (not supported by Go's time.ParseDuration)
	if strings.HasSuffix(s, "d") {
		numStr := strings.TrimSuffix(s, "d")
		days, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration format: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}

	return 0, fmt.Errorf("invalid duration format: %s", s)
}

// newGooglePubSubPublisher creates a Google Pub/Sub publisher
func newGooglePubSubPublisher(cfg *config, logger watermill.LoggerAdapter) (message.Publisher, error) {
	gps := cfg.Broker.GooglePubSub

	pubsubConfig := googlepubsub.PublisherConfig{
		ProjectID:                 gps.ProjectID,
		DoNotCreateTopicIfMissing: !gps.CreateTopicIfMissing, // Invert: our positive flag -> watermill's negative flag
		EnableMessageOrdering:     gps.EnableMessageOrdering,
	}

	return googlepubsub.NewPublisher(pubsubConfig, logger)
}

// ensureDeadLetterTopicExists creates the dead letter topic if it doesn't exist
func ensureDeadLetterTopicExists(ctx context.Context, projectID, topicName string, logger watermill.LoggerAdapter) error {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to create pubsub client: %w", err)
	}
	defer client.Close()

	fullyQualifiedName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicName)

	// Check if the topic exists
	_, err = client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{
		Topic: fullyQualifiedName,
	})
	if err == nil {
		// Topic already exists
		logger.Debug("Dead letter topic already exists", watermill.LogFields{
			"topic":      topicName,
			"project_id": projectID,
		})
		return nil
	}

	// If error is not "not found", return it
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		return fmt.Errorf("failed to check if dead letter topic exists: %w", err)
	}

	// Topic doesn't exist, create it
	logger.Info("Creating dead letter topic", watermill.LogFields{
		"topic":      topicName,
		"project_id": projectID,
	})

	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: fullyQualifiedName,
	})
	if err != nil {
		// Check if it was created by another process (race condition)
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			logger.Debug("Dead letter topic was created by another process", watermill.LogFields{
				"topic":      topicName,
				"project_id": projectID,
			})
			return nil
		}
		return fmt.Errorf("failed to create dead letter topic: %w", err)
	}

	logger.Info("Dead letter topic created successfully", watermill.LogFields{
		"topic":      topicName,
		"project_id": projectID,
	})
	return nil
}

// newGooglePubSubSubscriber creates a Google Pub/Sub subscriber
func newGooglePubSubSubscriber(cfg *config, logger watermill.LoggerAdapter, subscriptionID string) (message.Subscriber, error) {
	gps := cfg.Broker.GooglePubSub

	// If dead letter topic is configured and we're allowed to create topics, ensure it exists
	if gps.CreateTopicIfMissing {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := ensureDeadLetterTopicExists(ctx, gps.ProjectID, subscriptionID+"-dlq", logger); err != nil {
			return nil, fmt.Errorf("failed to ensure dead letter topic exists: %w", err)
		}
	}

	// Configure subscription name generator to use subscription ID
	// The topic passed to Subscribe will be the original topic (no colon)
	// We append subscription ID to create unique subscription names
	pubsubConfig := googlepubsub.SubscriberConfig{
		ProjectID: gps.ProjectID,
		GenerateSubscriptionName: func(topic string) string {
			return subscriptionID
		},
		DoNotCreateTopicIfMissing:        !gps.CreateTopicIfMissing,        // Invert: our positive flag -> watermill's negative flag
		DoNotCreateSubscriptionIfMissing: !gps.CreateSubscriptionIfMissing, // Invert: our positive flag -> watermill's negative flag
	}

	// Set ReceiveSettings for client-side flow control
	if gps.MaxOutstandingMessages > 0 {
		pubsubConfig.ReceiveSettings.MaxOutstandingMessages = gps.MaxOutstandingMessages
	}
	if gps.MaxOutstandingBytes > 0 {
		pubsubConfig.ReceiveSettings.MaxOutstandingBytes = gps.MaxOutstandingBytes
	}
	if gps.NumGoroutines > 0 {
		pubsubConfig.ReceiveSettings.NumGoroutines = gps.NumGoroutines
	}

	// Configure GenerateSubscription callback for subscription settings
	pubsubConfig.GenerateSubscription = func(params googlepubsub.GenerateSubscriptionParams) *pubsubpb.Subscription {
		sub := &pubsubpb.Subscription{
			EnableMessageOrdering: gps.EnableMessageOrdering,
		}

		// Set AckDeadlineSeconds (10-600 seconds)
		if gps.AckDeadlineSeconds > 0 {
			sub.AckDeadlineSeconds = int32(gps.AckDeadlineSeconds)
		}

		// Set MessageRetentionDuration (how long to retain unacknowledged messages)
		if gps.MessageRetentionDuration != "" {
			if d, err := parseGoogleCloudDuration(gps.MessageRetentionDuration); err == nil && d > 0 {
				sub.MessageRetentionDuration = durationpb.New(d)
			}
		}

		// Set ExpirationPolicy (time of inactivity before subscription is deleted)
		if gps.ExpirationTTL != "" {
			if d, err := parseGoogleCloudDuration(gps.ExpirationTTL); err == nil {
				if d == 0 {
					// TTL of 0 means never expire
					sub.ExpirationPolicy = &pubsubpb.ExpirationPolicy{}
				} else {
					sub.ExpirationPolicy = &pubsubpb.ExpirationPolicy{
						Ttl: durationpb.New(d),
					}
				}
			}
		}

		// Set RetryPolicy (backoff settings for message delivery)
		if gps.RetryMinBackoff != "" || gps.RetryMaxBackoff != "" {
			retryPolicy := &pubsubpb.RetryPolicy{}
			if gps.RetryMinBackoff != "" {
				if d, err := parseGoogleCloudDuration(gps.RetryMinBackoff); err == nil && d > 0 {
					retryPolicy.MinimumBackoff = durationpb.New(d)
				}
			}
			if gps.RetryMaxBackoff != "" {
				if d, err := parseGoogleCloudDuration(gps.RetryMaxBackoff); err == nil && d > 0 {
					retryPolicy.MaximumBackoff = durationpb.New(d)
				}
			}
			sub.RetryPolicy = retryPolicy
		}

		// Set DeadLetterPolicy (for handling messages that fail repeatedly)
		deadLetterPolicy := &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic: fmt.Sprintf("projects/%s/topics/%s", gps.ProjectID, subscriptionID+"-dlq"),
		}
		if gps.DeadLetterMaxAttempts > 0 {
			deadLetterPolicy.MaxDeliveryAttempts = int32(gps.DeadLetterMaxAttempts)
		} else {
			// Default to 5 attempts if not specified
			deadLetterPolicy.MaxDeliveryAttempts = 5
		}
		sub.DeadLetterPolicy = deadLetterPolicy

		return sub
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

	// Validate AckDeadlineSeconds (must be between 10 and 600 seconds if provided)
	if gps.AckDeadlineSeconds != 0 {
		if gps.AckDeadlineSeconds < 10 || gps.AckDeadlineSeconds > 600 {
			return fmt.Errorf("googlepubsub.ack_deadline_seconds must be between 10 and 600 seconds")
		}
	}

	// Validate MessageRetentionDuration format if provided
	if gps.MessageRetentionDuration != "" {
		d, err := parseGoogleCloudDuration(gps.MessageRetentionDuration)
		if err != nil {
			return fmt.Errorf("googlepubsub.message_retention_duration: %w", err)
		}
		// Must be between 10 minutes and 31 days
		minRetention := 10 * time.Minute
		maxRetention := 31 * 24 * time.Hour
		if d < minRetention || d > maxRetention {
			return fmt.Errorf("googlepubsub.message_retention_duration must be between 10m and 31d")
		}
	}

	// Validate ExpirationTTL format if provided
	if gps.ExpirationTTL != "" {
		d, err := parseGoogleCloudDuration(gps.ExpirationTTL)
		if err != nil {
			return fmt.Errorf("googlepubsub.expiration_ttl: %w", err)
		}
		// Must be at least 1 day or 0 (never expire)
		if d != 0 && d < 24*time.Hour {
			return fmt.Errorf("googlepubsub.expiration_ttl must be at least 1d or 0 (never expire)")
		}
	}

	// Validate RetryMinBackoff format if provided
	if gps.RetryMinBackoff != "" {
		d, err := parseGoogleCloudDuration(gps.RetryMinBackoff)
		if err != nil {
			return fmt.Errorf("googlepubsub.retry_min_backoff: %w", err)
		}
		// Must be between 0 and 600 seconds
		if d < 0 || d > 600*time.Second {
			return fmt.Errorf("googlepubsub.retry_min_backoff must be between 0s and 600s")
		}
	}

	// Validate RetryMaxBackoff format if provided
	if gps.RetryMaxBackoff != "" {
		d, err := parseGoogleCloudDuration(gps.RetryMaxBackoff)
		if err != nil {
			return fmt.Errorf("googlepubsub.retry_max_backoff: %w", err)
		}
		// Must be between 0 and 600 seconds
		if d < 0 || d > 600*time.Second {
			return fmt.Errorf("googlepubsub.retry_max_backoff must be between 0s and 600s")
		}
	}

	// Validate retry backoff relationship (min <= max)
	if gps.RetryMinBackoff != "" && gps.RetryMaxBackoff != "" {
		minBackoff, _ := parseGoogleCloudDuration(gps.RetryMinBackoff)
		maxBackoff, _ := parseGoogleCloudDuration(gps.RetryMaxBackoff)
		if minBackoff > maxBackoff {
			return fmt.Errorf("googlepubsub.retry_min_backoff must be less than or equal to retry_max_backoff")
		}
	}

	// Validate DeadLetterMaxAttempts (must be between 5 and 100 if provided)
	if gps.DeadLetterMaxAttempts != 0 {
		if gps.DeadLetterMaxAttempts < 5 || gps.DeadLetterMaxAttempts > 100 {
			return fmt.Errorf("googlepubsub.dead_letter_max_attempts must be between 5 and 100")
		}
	}

	// Validate TopicRetentionDuration format if provided
	if gps.TopicRetentionDuration != "" {
		d, err := parseGoogleCloudDuration(gps.TopicRetentionDuration)
		if err != nil {
			return fmt.Errorf("googlepubsub.topic_retention_duration: %w", err)
		}
		// Must be between 10 minutes and 31 days
		minRetention := 10 * time.Minute
		maxRetention := 31 * 24 * time.Hour
		if d != 0 && (d < minRetention || d > maxRetention) {
			return fmt.Errorf("googlepubsub.topic_retention_duration must be between 10m and 31d, or 0 (disabled)")
		}
	}

	// Validate MaxOutstandingMessages (must be non-negative if provided)
	if gps.MaxOutstandingMessages < 0 {
		return fmt.Errorf("googlepubsub.max_outstanding_messages must be non-negative")
	}

	// Validate MaxOutstandingBytes (must be non-negative if provided)
	if gps.MaxOutstandingBytes < 0 {
		return fmt.Errorf("googlepubsub.max_outstanding_bytes must be non-negative")
	}

	// Validate NumGoroutines (must be non-negative if provided)
	if gps.NumGoroutines < 0 {
		return fmt.Errorf("googlepubsub.num_goroutines must be non-negative")
	}

	return nil
}
