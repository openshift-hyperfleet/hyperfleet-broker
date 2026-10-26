package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	hfl "github.com/openshift-hyperfleet/hyperfleet-logger"
)

func createEvent(id string, message string) (event.Event, error) {
	evt := event.New()
	evt.SetType("com.example.event.created")
	evt.SetSource("example-publisher")
	evt.SetID(id)
	evt.SetTime(time.Now())
	if err := evt.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"message":   message,
		"timestamp": time.Now().Format(time.RFC3339),
	}); err != nil {
		return event.Event{}, fmt.Errorf("set event data: %w", err)
	}
	return evt, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	interval := flag.Duration("interval", 20*time.Millisecond, "Interval between publishing events")
	topic := flag.String("topic", "example-topic", "Topic to publish events to")
	message := flag.String("message", "", "Send a single message with this content and exit")
	flag.Parse()

	// Create logger using the shared HyperFleet handler, metrics, and publisher
	appLogger := slog.New(hfl.NewHandler("example-publisher", "v0.0.0"))
	metrics := broker.NewMetricsRecorder("example-publisher", "v0.0.0", nil)
	publisher, err := broker.NewPublisher(appLogger, metrics)
	if err != nil {
		return fmt.Errorf("failed to create publisher: %w", err)
	}
	defer func() {
		// Cleanup failures are logged; they do not replace the publishing result.
		if closeErr := publisher.Close(); closeErr != nil {
			log.Printf("Failed to close publisher: %v", closeErr)
		}
	}()

	// Single message mode
	if *message != "" {
		evt, createErr := createEvent(fmt.Sprintf("event-%d", time.Now().UnixNano()), *message)
		if createErr != nil {
			return createErr
		}
		if err := publisher.Publish(ctx, *topic, &evt); err != nil {
			return fmt.Errorf("error publishing event: %w", err)
		}
		log.Printf("Published single message to topic %s: %s", *topic, *message)
		return nil
	}

	// Continuous publishing mode
	log.Printf("Publisher started. Publishing events to topic: %s with interval: %v, broker type: %s",
		*topic, *interval, publisher.BrokerType())
	log.Printf("Press Ctrl+C to stop...")

	// Publish events
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	counter := 0
	for range ticker.C {
		counter++
		evt, createErr := createEvent(fmt.Sprintf("event-%d", counter), "Hello from publisher")
		if createErr != nil {
			return createErr
		}
		if err := publisher.Publish(ctx, *topic, &evt); err != nil {
			log.Printf("Error publishing event: %v", err)
		} else {
			log.Printf("Published event #%d (ID: %s)", counter, evt.ID())
		}
	}
	return nil
}
