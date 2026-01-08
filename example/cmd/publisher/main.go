package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
)

func createEvent(id string, message string) event.Event {
	evt := event.New()
	evt.SetType("com.example.event.created")
	evt.SetSource("example-publisher")
	evt.SetID(id)
	evt.SetTime(time.Now())
	evt.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"message":   message,
		"timestamp": time.Now().Format(time.RFC3339),
	})
	return evt
}

func main() {
	ctx := context.Background()

	interval := flag.Duration("interval", 20*time.Millisecond, "Interval between publishing events")
	topic := flag.String("topic", "example-topic", "Topic to publish events to")
	message := flag.String("message", "", "Send a single message with this content and exit")
	flag.Parse()

	// Create logger and publisher
	appLogger := logger.NewTestLogger()
	publisher, err := broker.NewPublisher(appLogger)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	// Single message mode
	if *message != "" {
		evt := createEvent(fmt.Sprintf("event-%d", time.Now().UnixNano()), *message)
		if err := publisher.Publish(ctx, *topic, &evt); err != nil {
			log.Fatalf("Error publishing event: %v", err)
		}
		log.Printf("Published single message to topic %s: %s", *topic, *message)
		return
	}

	// Continuous publishing mode
	log.Printf("Publisher started. Publishing events to topic: %s with interval: %v", *topic, *interval)
	log.Printf("Press Ctrl+C to stop...")

	// Publish events
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	counter := 0
	for range ticker.C {
		counter++
		evt := createEvent(fmt.Sprintf("event-%d", counter), "Hello from publisher")
		if err := publisher.Publish(ctx, *topic, &evt); err != nil {
			log.Printf("Error publishing event: %v", err)
		} else {
			log.Printf("Published event #%d (ID: %s)", counter, evt.ID())
		}
	}
}
