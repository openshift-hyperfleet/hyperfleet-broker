package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

func main() {
	processTime := flag.Duration("process-time", 2*time.Second, "Time to simulate message processing")
	topic := flag.String("topic", "example-topic", "Topic to subscribe to")
	subscription := flag.String("subscription", "shared-subscription", "Subscription ID for load balancing")
	flag.Parse()

	// Get subscriber instance ID from environment variable (defaults to "1")
	instanceID := os.Getenv("SUBSCRIBER_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}

	// Create subscriber with subscription ID
	// Both subscribers use the same subscription ID to share messages (load balancing)
	subscriber, err := broker.NewSubscriber(*subscription)
	if err != nil {
		log.Fatalf("Failed to create subscriber: %v", err)
	}
	defer subscriber.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Subscriber instance %s started. Listening on topic: %s with subscription ID: %s", instanceID, *topic, *subscription)

	// Define handler
	handler := func(ctx context.Context, evt *event.Event) error {
		log.Printf("[Subscriber %s] Received event - ID: %s, Type: %s, Source: %s",
			instanceID, evt.ID(), evt.Type(), evt.Source())

		// Extract data
		var data map[string]interface{}
		if err := evt.DataAs(&data); err == nil {
			log.Printf("[Subscriber %s] Event data: %+v", instanceID, data)
		}

		time.Sleep(*processTime)
		log.Printf("[Subscriber %s] Processed event - ID: %s, Type: %s, Source: %s",
			instanceID, evt.ID(), evt.Type(), evt.Source())
		return nil
	}

	// Subscribe to topic
	if err := subscriber.Subscribe(ctx, *topic, handler); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Printf("Shutting down subscriber instance %s...", instanceID)
}
