package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

func main() {
	interval := flag.Duration("interval", 20*time.Millisecond, "Interval between publishing events")
	flag.Parse()

	// Create publisher
	publisher, err := broker.NewPublisher()
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	topic := "example-topic"
	// Get topic from command line argument
	if flag.NArg() > 0 {
		topic = flag.Arg(0)
	}

	log.Printf("Publisher started. Publishing events to topic: %s with interval: %v", topic, *interval)
	log.Printf("Press Ctrl+C to stop...")

	// Publish events
	ticker := time.NewTicker(*interval)
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
		if err := publisher.Publish(topic, &evt); err != nil {
			log.Printf("Error publishing event: %v", err)
		} else {
			log.Printf("Published event #%d (ID: %s)", counter, evt.ID())
		}
	}
}
