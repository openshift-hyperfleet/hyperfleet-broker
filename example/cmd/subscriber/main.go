package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

func main() {
	// Get subscriber instance ID from environment variable (defaults to "1")
	instanceID := os.Getenv("SUBSCRIBER_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}

	// Create subscriber with subscription ID
	// Both subscribers use the same subscription ID to share messages (load balancing)
	subscriptionID := "shared-subscription"
	subscriber, err := broker.NewSubscriber(subscriptionID)
	if err != nil {
		log.Fatalf("Failed to create subscriber: %v", err)
	}
	defer subscriber.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "example-topic"

	log.Printf("Subscriber instance %s started. Listening on topic: %s with subscription ID: %s", instanceID, topic, subscriptionID)

	// Define handler
	handler := func(ctx context.Context, evt *event.Event) error {
		log.Printf("[Subscriber %s] Received event - ID: %s, Type: %s, Source: %s",
			instanceID, evt.ID(), evt.Type(), evt.Source())

		// Extract data
		var data map[string]interface{}
		if err := evt.DataAs(&data); err == nil {
			log.Printf("[Subscriber %s] Event data: %+v", instanceID, data)
		}

		return nil
	}

	// Subscribe to topic
	if err := subscriber.Subscribe(ctx, topic, handler); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Printf("Shutting down subscriber instance %s...", instanceID)
}
