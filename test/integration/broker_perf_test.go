package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/require"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

// we keep this short for now to make the tests run faster
const TEST_DURATION = 10 * time.Second

// PerformanceTestResult holds the results of a performance test
type PerformanceTestResult struct {
	BrokerType        string            `json:"broker_type"`
	TestDuration      string            `json:"test_duration"`
	MessagesPublished int64             `json:"messages_published"`
	Subscriber1       SubscriberMetrics `json:"subscriber_1"`
	Subscriber2       SubscriberMetrics `json:"subscriber_2"`
	TotalReceived     int64             `json:"total_received"`
	PublishRate       float64           `json:"publish_rate_per_sec"`
	ReceiveRate       float64           `json:"receive_rate_per_sec"`
	Timestamp         time.Time         `json:"timestamp"`
}

// SubscriberMetrics holds metrics for a single subscriber
type SubscriberMetrics struct {
	MessagesReceived int64   `json:"messages_received"`
	ReceiveRate      float64 `json:"receive_rate_per_sec"`
}

// TestRabbitMQPerformance tests publish/subscribe performance with two subscribers
func TestRabbitMQPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	rabbitMQURL := setupRabbitMQContainer(t)
	configMap := buildConfigMap("rabbitmq", rabbitMQURL, "")

	// Create publisher
	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	// Create two subscribers with the same subscriptionID (shared subscription)
	subscriptionID := "perf-subscription"
	sub1, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub1.Close(); err != nil {
			t.Logf("failed to close subscriber 1: %v", err)
		}
	}()

	sub2, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub2.Close(); err != nil {
			t.Logf("failed to close subscriber 2: %v", err)
		}
	}()

	// Metrics
	var messagesPublished int64
	var sub1Received int64
	var sub2Received int64
	var wg sync.WaitGroup

	// Set up handlers
	handler1 := func(ctx context.Context, e *event.Event) error {
		//time.Sleep(1000 * time.Millisecond)
		//t.Logf("Subscriber 1 received message %s - %d", e.ID(), sub1Received)
		atomic.AddInt64(&sub1Received, 1)
		return nil
	}

	handler2 := func(ctx context.Context, e *event.Event) error {
		//time.Sleep(10 * time.Millisecond)
		//t.Logf("Subscriber 2 received message %s - %d", e.ID(), sub2Received)
		atomic.AddInt64(&sub2Received, 1)
		return nil
	}

	// Subscribe both subscribers
	err = sub1.Subscribe(ctx, "perf-topic", handler1)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "perf-topic", handler2)
	require.NoError(t, err)

	// Give subscribers time to set up
	time.Sleep(500 * time.Millisecond)

	startTime := time.Now()
	endTime := startTime.Add(TEST_DURATION)

	// Start publisher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		eventID := int64(0)
		for time.Now().Before(endTime) {
			evt := event.New()
			evt.SetType("com.example.performance.event")
			evt.SetSource("perf-source")
			evt.SetID(fmt.Sprintf("perf-id-%d", atomic.AddInt64(&eventID, 1)))
			if err := evt.SetData(event.ApplicationJSON, map[string]interface{}{
				"timestamp": time.Now().UnixNano(),
				"id":        eventID,
			}); err != nil {
				t.Logf("Error setting event data: %v", err)
				continue
			}

			if err := pub.Publish("perf-topic", &evt); err != nil {
				t.Logf("Error publishing message: %v", err)
				continue
			}
			atomic.AddInt64(&messagesPublished, 1)
		}
	}()

	// Wait for publisher to finish
	wg.Wait()

	// Give some time for remaining messages to be processed
	time.Sleep(2 * time.Second)

	actualDuration := time.Since(startTime)
	published := atomic.LoadInt64(&messagesPublished)
	received1 := atomic.LoadInt64(&sub1Received)
	received2 := atomic.LoadInt64(&sub2Received)
	totalReceived := received1 + received2

	publishRate := float64(published) / actualDuration.Seconds()
	receiveRate := float64(totalReceived) / actualDuration.Seconds()

	result := PerformanceTestResult{
		BrokerType:        "rabbitmq",
		TestDuration:      actualDuration.String(),
		MessagesPublished: published,
		Subscriber1: SubscriberMetrics{
			MessagesReceived: received1,
			ReceiveRate:      float64(received1) / actualDuration.Seconds(),
		},
		Subscriber2: SubscriberMetrics{
			MessagesReceived: received2,
			ReceiveRate:      float64(received2) / actualDuration.Seconds(),
		},
		TotalReceived: totalReceived,
		PublishRate:   publishRate,
		ReceiveRate:   receiveRate,
		Timestamp:     time.Now(),
	}

	// Write results to JSON file
	writeResults(t, result)

	t.Logf("RabbitMQ Performance Test Results:")
	t.Logf("  Duration: %s", actualDuration)
	t.Logf("  Messages Published: %d", published)
	t.Logf("  Subscriber 1 Received: %d (%.2f msg/sec)", received1, result.Subscriber1.ReceiveRate)
	t.Logf("  Subscriber 2 Received: %d (%.2f msg/sec)", received2, result.Subscriber2.ReceiveRate)
	t.Logf("  Total Received: %d", totalReceived)
	t.Logf("  Publish Rate: %.2f msg/sec", publishRate)
	t.Logf("  Receive Rate: %.2f msg/sec", receiveRate)
}

// TestGooglePubSubPerformance tests publish/subscribe performance with two subscribers
func TestGooglePubSubPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	projectID, _ := setupPubSubEmulator(t)
	configMap := buildConfigMap("googlepubsub", "", projectID)

	//configMap["broker.googlepubsub.max_outstanding_messages"] = "10"
	// Set NumGoroutines to 1 to use a single streaming pull stream
	configMap["subscriber.parallelism"] = "10"
	configMap["broker.googlepubsub.num_goroutines"] = "10"
	// Create publisher
	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	// Create two subscribers with the same subscriptionID (shared subscription)
	subscriptionID := "perf-subscription"
	sub1, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub1.Close(); err != nil {
			t.Logf("failed to close subscriber 1: %v", err)
		}
	}()

	sub2, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub2.Close(); err != nil {
			t.Logf("failed to close subscriber 2: %v", err)
		}
	}()

	// Metrics
	var messagesPublished int64
	var sub1Received int64
	var sub2Received int64
	var wg sync.WaitGroup

	// Set up handlers
	handler1 := func(ctx context.Context, e *event.Event) error {
		//time.Sleep(1000 * time.Millisecond)
		//t.Logf("Subscriber 1 received message %s - %d", e.ID(), sub1Received)
		atomic.AddInt64(&sub1Received, 1)
		return nil
	}

	handler2 := func(ctx context.Context, e *event.Event) error {
		//time.Sleep(1000 * time.Millisecond)
		//t.Logf("Subscriber 2 received message %s - %d", e.ID(), sub2Received)
		atomic.AddInt64(&sub2Received, 1)
		return nil
	}

	// Subscribe both subscribers
	err = sub1.Subscribe(ctx, "perf-topic", handler1)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "perf-topic", handler2)
	require.NoError(t, err)

	// Give subscribers time to set up
	time.Sleep(2 * time.Second)

	// Run test for at least 1 minute
	//testDuration := 1 * time.Minute
	testDuration := 10 * time.Second
	startTime := time.Now()
	endTime := startTime.Add(testDuration)

	// Start publisher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		eventID := int64(0)
		for time.Now().Before(endTime) {
			evt := event.New()
			evt.SetType("com.example.performance.event")
			evt.SetSource("perf-source")
			evt.SetID(fmt.Sprintf("perf-id-%d", atomic.AddInt64(&eventID, 1)))
			if err := evt.SetData(event.ApplicationJSON, map[string]interface{}{
				"timestamp": time.Now().UnixNano(),
				"id":        eventID,
			}); err != nil {
				t.Logf("Error setting event data: %v", err)
				continue
			}

			if err := pub.Publish("perf-topic", &evt); err != nil {
				t.Logf("Error publishing message: %v", err)
				continue
			}
			atomic.AddInt64(&messagesPublished, 1)
		}
	}()

	// Wait for publisher to finish
	wg.Wait()

	// Give some time for remaining messages to be processed
	time.Sleep(5 * time.Second)

	actualDuration := time.Since(startTime)
	published := atomic.LoadInt64(&messagesPublished)
	received1 := atomic.LoadInt64(&sub1Received)
	received2 := atomic.LoadInt64(&sub2Received)
	totalReceived := received1 + received2

	publishRate := float64(published) / actualDuration.Seconds()
	receiveRate := float64(totalReceived) / actualDuration.Seconds()

	result := PerformanceTestResult{
		BrokerType:        "googlepubsub",
		TestDuration:      actualDuration.String(),
		MessagesPublished: published,
		Subscriber1: SubscriberMetrics{
			MessagesReceived: received1,
			ReceiveRate:      float64(received1) / actualDuration.Seconds(),
		},
		Subscriber2: SubscriberMetrics{
			MessagesReceived: received2,
			ReceiveRate:      float64(received2) / actualDuration.Seconds(),
		},
		TotalReceived: totalReceived,
		PublishRate:   publishRate,
		ReceiveRate:   receiveRate,
		Timestamp:     time.Now(),
	}

	// Write results to JSON file
	writeResults(t, result)

	t.Logf("Google Pub/Sub Performance Test Results:")
	t.Logf("  Duration: %s", actualDuration)
	t.Logf("  Messages Published: %d", published)
	t.Logf("  Subscriber 1 Received: %d (%.2f msg/sec)", received1, result.Subscriber1.ReceiveRate)
	t.Logf("  Subscriber 2 Received: %d (%.2f msg/sec)", received2, result.Subscriber2.ReceiveRate)
	t.Logf("  Total Received: %d", totalReceived)
	t.Logf("  Publish Rate: %.2f msg/sec", publishRate)
	t.Logf("  Receive Rate: %.2f msg/sec", receiveRate)
}

// writeResults writes performance test results to test.out.json
func writeResults(t *testing.T, result PerformanceTestResult) {
	// Read existing results if file exists
	var results []PerformanceTestResult
	filePath := "test.out.json"

	// Try to read existing results
	if data, err := os.ReadFile(filePath); err == nil {
		if err := json.Unmarshal(data, &results); err != nil {
			t.Logf("Warning: Could not parse existing results file: %v", err)
			results = []PerformanceTestResult{}
		}
	} else {
		// File doesn't exist, start with empty slice
		results = []PerformanceTestResult{}
	}

	// Append new result
	results = append(results, result)

	// Write results back to file
	data, err := json.MarshalIndent(results, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(filePath, data, 0644)
	require.NoError(t, err)

	t.Logf("Results written to %s", filePath)
}
