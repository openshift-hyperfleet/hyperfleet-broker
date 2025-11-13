package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

func main() {
	// Create publisher
	publisher, err := broker.NewPublisher()
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	ctx := context.Background()

	topic := "example-topic"
	// Get topic from command line argument
	if len(os.Args) > 1 {
		topic = os.Args[1]
	}

	log.Printf("Publisher started. Publishing events to topic: %s", topic)
	log.Printf("Press Ctrl+C to stop...")

	// Publish events every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	counter := 0
	for range ticker.C {
		counter++

		// Create a CloudEvent
		evt := event.New()
		evt.SetType("com.example.event.created")
		evt.SetSource("example-publisher")
		evt.SetID(fmt.Sprintf("event-%d", counter))
		evt.SetTime(time.Now())
		evt.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
			"message":   "Hello from publisher",
			"counter":   counter,
			"timestamp": time.Now().Format(time.RFC3339),
		})

		// Publish to topic
		if err := publisher.Publish(ctx, topic, &evt); err != nil {
			log.Printf("Error publishing event: %v", err)
		} else {
			log.Printf("Published event #%d (ID: %s)", counter, evt.ID())
		}
	}
}
