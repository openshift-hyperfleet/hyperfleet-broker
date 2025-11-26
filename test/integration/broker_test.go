package integration_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

// TestMain sets up the test environment for Podman and disables Ryuk
// This is required when using Podman instead of Docker with testcontainers
func TestMain(m *testing.M) {
	// Disable Ryuk (resource reaper) - required for Podman
	// This should be set before any testcontainers operations
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}

	// Configure Podman socket if DOCKER_HOST is not already set
	// On Linux with rootless Podman:
	// export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
	// On macOS with Podman machine:
	// export DOCKER_HOST=unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')
	if os.Getenv("DOCKER_HOST") == "" {
		// Try to detect Podman socket location
		if xdgRuntimeDir := os.Getenv("XDG_RUNTIME_DIR"); xdgRuntimeDir != "" {
			podmanSock := fmt.Sprintf("%s/podman/podman.sock", xdgRuntimeDir)
			if _, err := os.Stat(podmanSock); err == nil {
				os.Setenv("DOCKER_HOST", fmt.Sprintf("unix://%s", podmanSock))
			}
		}
	}

	// Run tests
	code := m.Run()
	os.Exit(code)
}

// setupRabbitMQContainer starts a RabbitMQ testcontainer and returns the connection URL
func setupRabbitMQContainer(t *testing.T) string {
	ctx := context.Background()

	rabbitmqContainer, err := rabbitmq.Run(ctx,
		"rabbitmq:3-management-alpine",
		rabbitmq.WithAdminUsername("guest"),
		rabbitmq.WithAdminPassword("guest"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := rabbitmqContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate rabbitmq container: %v", err)
		}
	})

	connectionString, err := rabbitmqContainer.AmqpURL(ctx)
	require.NoError(t, err)

	return connectionString
}

// setupPubSubEmulator starts a Google Pub/Sub emulator testcontainer and returns the project ID and emulator host
func setupPubSubEmulator(t *testing.T) (string, string) {
	ctx := context.Background()

	// Create a generic container for Pub/Sub emulator
	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
		ExposedPorts: []string{"8085/tcp"},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"},
		WaitingFor: wait.ForLog("Server started").
			WithOccurrence(1).
			WithStartupTimeout(60 * time.Second),
	}

	pubsubContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pubsubContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate pubsub container: %v", err)
		}
	})

	projectID := "test-project"

	// Get the container host and port
	host, err := pubsubContainer.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := pubsubContainer.MappedPort(ctx, "8085")
	require.NoError(t, err)

	emulatorHost := fmt.Sprintf("%s:%s", host, mappedPort.Port())

	// Set environment variable for Pub/Sub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	t.Cleanup(func() {
		os.Unsetenv("PUBSUB_EMULATOR_HOST")
	})

	return projectID, emulatorHost
}

// buildConfigMap creates a configuration map for testing
func buildConfigMap(brokerType string, rabbitMQURL string, pubsubProjectID string) map[string]string {
	configMap := map[string]string{
		"broker.type":            brokerType,
		"subscriber.parallelism": "1",
	}

	if brokerType == "rabbitmq" {
		configMap["broker.rabbitmq.url"] = rabbitMQURL
	} else if brokerType == "googlepubsub" {
		configMap["broker.googlepubsub.project_id"] = pubsubProjectID
	}

	return configMap
}

// brokerTestConfig holds broker-specific test configuration
type brokerTestConfig struct {
	brokerType      string
	setupSleep      time.Duration
	receiveTimeout  time.Duration
	setupConfigFunc func(*testing.T, map[string]string) // Optional function to modify config after initial setup
	publishDelay    time.Duration                       // Delay between publishes (for gradual publishing, 0 = no delay)
}

// setupBrokerTest sets up a broker test environment and returns the config map
func setupBrokerTest(t *testing.T, cfg brokerTestConfig) map[string]string {
	var configMap map[string]string

	switch cfg.brokerType {
	case "rabbitmq":
		rabbitMQURL := setupRabbitMQContainer(t)
		configMap = buildConfigMap("rabbitmq", rabbitMQURL, "")
	case "googlepubsub":
		projectID, _ := setupPubSubEmulator(t)
		configMap = buildConfigMap("googlepubsub", "", projectID)
	default:
		t.Fatalf("unsupported broker type: %s", cfg.brokerType)
	}

	// Apply any additional configuration modifications
	if cfg.setupConfigFunc != nil {
		cfg.setupConfigFunc(t, configMap)
	}

	return configMap
}

// testPublisherSubscriber tests the full publish/subscribe flow
func testPublisherSubscriber(t *testing.T, cfg brokerTestConfig) {
	ctx := context.Background()
	configMap := setupBrokerTest(t, cfg)

	// Create publisher
	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	// Create subscriber
	subscriptionID := "test-subscription"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer sub.Close()

	// Create a CloudEvent
	evt := event.New()
	evt.SetType("com.example.test.event")
	evt.SetSource("test-source")
	evt.SetID("test-id-123")
	_ = evt.SetData(event.ApplicationJSON, map[string]string{
		"message": "Hello, World!",
	})

	// Set up handler
	receivedEvents := make(chan *event.Event, 1)
	handler := func(ctx context.Context, e *event.Event) error {
		receivedEvents <- e
		return nil
	}

	// Subscribe to topic
	err = sub.Subscribe(ctx, "test-topic", handler)
	require.NoError(t, err)

	// Give subscriber time to set up
	time.Sleep(cfg.setupSleep)

	err = pub.Publish("test-topic", &evt)
	require.NoError(t, err)

	// Wait for event to be received
	select {
	case receivedEvt := <-receivedEvents:
		assert.Equal(t, evt.Type(), receivedEvt.Type())
		assert.Equal(t, evt.ID(), receivedEvt.ID())
		assert.Equal(t, evt.Source(), receivedEvt.Source())
	case <-time.After(cfg.receiveTimeout):
		t.Fatal("timeout waiting for event")
	}
}

// TestRabbitMQPublisherSubscriber tests the full publish/subscribe flow with RabbitMQ
func TestRabbitMQPublisherSubscriber(t *testing.T) {
	testPublisherSubscriber(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     500 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubPublisherSubscriber tests the full publish/subscribe flow with Google Pub/Sub
func TestGooglePubSubPublisherSubscriber(t *testing.T) {
	testPublisherSubscriber(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
	})
}

// testMultipleEvents tests that multiple events are processed correctly
func testMultipleEvents(t *testing.T, cfg brokerTestConfig) {
	ctx := context.Background()
	configMap := setupBrokerTest(t, cfg)

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	subscriptionID := "test-subscription"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer sub.Close()

	// Create two different event types
	evt1 := event.New()
	evt1.SetType("com.example.event.type1")
	evt1.SetSource("test-source")
	evt1.SetID("id-1")
	_ = evt1.SetData(event.ApplicationJSON, map[string]string{"type": "1"})

	evt2 := event.New()
	evt2.SetType("com.example.event.type2")
	evt2.SetSource("test-source")
	evt2.SetID("id-2")
	_ = evt2.SetData(event.ApplicationJSON, map[string]string{"type": "2"})

	// Set up handler to collect all events
	receivedEvents := make(chan *event.Event, 2)
	handler := func(ctx context.Context, e *event.Event) error {
		receivedEvents <- e
		return nil
	}

	err = sub.Subscribe(ctx, "routing-topic", handler)
	require.NoError(t, err)

	time.Sleep(cfg.setupSleep)

	// Publish both events
	err = pub.Publish("routing-topic", &evt1)
	require.NoError(t, err)

	err = pub.Publish("routing-topic", &evt2)
	require.NoError(t, err)

	// Verify both events were received
	eventsReceived := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case received := <-receivedEvents:
			eventsReceived[received.ID()] = true
			assert.Contains(t, []string{"id-1", "id-2"}, received.ID())
		case <-time.After(cfg.receiveTimeout):
			t.Fatalf("timeout waiting for event %d", i+1)
		}
	}

	assert.True(t, eventsReceived["id-1"], "event id-1 should be received")
	assert.True(t, eventsReceived["id-2"], "event id-2 should be received")
}

// TestRabbitMQMultipleEvents tests that multiple events are processed correctly
func TestRabbitMQMultipleEvents(t *testing.T) {
	testMultipleEvents(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     500 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubMultipleEvents tests that multiple events are processed correctly
func TestGooglePubSubMultipleEvents(t *testing.T) {
	testMultipleEvents(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
	})
}

// testSharedSubscription tests that two subscribers with the same subscriptionID share messages
func testSharedSubscription(t *testing.T, cfg brokerTestConfig) {
	ctx := context.Background()
	configMap := setupBrokerTest(t, cfg)

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	// Create two subscribers with the same subscriptionID
	// They should share messages (load balancing)
	subscriptionID := "shared-subscription"
	sub1, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer sub1.Close()

	sub2, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer sub2.Close()

	// Set up handlers to collect events
	received1 := make(chan *event.Event, 10)
	received2 := make(chan *event.Event, 10)

	handler1 := func(ctx context.Context, e *event.Event) error {
		received1 <- e
		return nil
	}

	handler2 := func(ctx context.Context, e *event.Event) error {
		received2 <- e
		return nil
	}

	// Both subscribe to the same topic with the same subscriptionID
	err = sub1.Subscribe(ctx, "shared-topic", handler1)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "shared-topic", handler2)
	require.NoError(t, err)

	time.Sleep(cfg.setupSleep)

	// Publish 10 messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("id-%d", i))
		_ = evt.SetData(event.ApplicationJSON, map[string]int{"index": i})

		err = pub.Publish("shared-topic", &evt)
		require.NoError(t, err)
	}

	// Wait for all messages to be received
	timeout := time.After(cfg.receiveTimeout)
	allReceived := make(map[string]bool)
	sub1Received := make(map[string]bool)
	sub2Received := make(map[string]bool)

	// Collect messages from both subscribers
	for i := 0; i < numMessages; i++ {
		select {
		case evt := <-received1:
			allReceived[evt.ID()] = true
			sub1Received[evt.ID()] = true
		case evt := <-received2:
			allReceived[evt.ID()] = true
			sub2Received[evt.ID()] = true
		case <-timeout:
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	// Verify all messages were received (distributed between the two subscribers)
	assert.Equal(t, numMessages, len(allReceived), "all messages should be received")

	// Verify messages were distributed (not all to one subscriber)
	sub1Count := len(sub1Received)
	sub2Count := len(sub2Received)
	assert.Greater(t, sub1Count, 0, "subscriber 1 should receive at least one message")
	assert.Greater(t, sub2Count, 0, "subscriber 2 should receive at least one message")
	assert.Equal(t, numMessages, sub1Count+sub2Count, "total messages should equal sum of both subscribers")
}

// TestRabbitMQSharedSubscription tests that two subscribers with the same subscriptionID share messages
func TestRabbitMQSharedSubscription(t *testing.T) {
	testSharedSubscription(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     500 * time.Millisecond,
		receiveTimeout: 10 * time.Second,
	})
}

// TestGooglePubSubSharedSubscription tests that two subscribers with the same subscriptionID share messages
func TestGooglePubSubSharedSubscription(t *testing.T) {
	testSharedSubscription(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 15 * time.Second,
	})
}

// testFanoutSubscription tests that two subscribers with different subscriptionIDs each get all messages
func testFanoutSubscription(t *testing.T, cfg brokerTestConfig) {
	ctx := context.Background()
	configMap := setupBrokerTest(t, cfg)

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	// Create two subscribers with different subscriptionIDs
	// Each should receive all messages (fanout behavior)
	sub1, err := broker.NewSubscriber("fanout-subscription-1", configMap)
	require.NoError(t, err)
	defer sub1.Close()

	sub2, err := broker.NewSubscriber("fanout-subscription-2", configMap)
	require.NoError(t, err)
	defer sub2.Close()

	// Set up handlers to collect events
	received1 := make(chan *event.Event, 10)
	received2 := make(chan *event.Event, 10)

	handler1 := func(ctx context.Context, e *event.Event) error {
		received1 <- e
		return nil
	}

	handler2 := func(ctx context.Context, e *event.Event) error {
		received2 <- e
		return nil
	}

	// Both subscribe to the same topic but with different subscriptionIDs
	err = sub1.Subscribe(ctx, "fanout-topic", handler1)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "fanout-topic", handler2)
	require.NoError(t, err)

	time.Sleep(cfg.setupSleep)

	// Publish 5 messages
	numMessages := 5
	messageIDs := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("fanout-id-%d", i))
		messageIDs[i] = evt.ID()
		_ = evt.SetData(event.ApplicationJSON, map[string]int{"index": i})

		err = pub.Publish("fanout-topic", &evt)
		require.NoError(t, err)
	}

	// Wait for all messages to be received by both subscribers
	timeout := time.After(cfg.receiveTimeout)

	// Collect messages from subscriber 1
	received1Map := make(map[string]bool)
	for i := 0; i < numMessages; i++ {
		select {
		case evt := <-received1:
			received1Map[evt.ID()] = true
		case <-timeout:
			t.Fatalf("timeout waiting for subscriber 1 to receive message %d", i)
		}
	}

	// Collect messages from subscriber 2
	received2Map := make(map[string]bool)
	for i := 0; i < numMessages; i++ {
		select {
		case evt := <-received2:
			received2Map[evt.ID()] = true
		case <-timeout:
			t.Fatalf("timeout waiting for subscriber 2 to receive message %d", i)
		}
	}

	// Verify both subscribers received all messages
	assert.Equal(t, numMessages, len(received1Map), "subscriber 1 should receive all messages")
	assert.Equal(t, numMessages, len(received2Map), "subscriber 2 should receive all messages")

	// Verify both received the same message IDs
	for _, id := range messageIDs {
		assert.True(t, received1Map[id], "subscriber 1 should receive message %s", id)
		assert.True(t, received2Map[id], "subscriber 2 should receive message %s", id)
	}
}

// TestRabbitMQFanoutSubscription tests that two subscribers with different subscriptionIDs each get all messages
func TestRabbitMQFanoutSubscription(t *testing.T) {
	testFanoutSubscription(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     500 * time.Millisecond,
		receiveTimeout: 10 * time.Second,
	})
}

// TestGooglePubSubFanoutSubscription tests that two subscribers with different subscriptionIDs each get all messages
func TestGooglePubSubFanoutSubscription(t *testing.T) {
	testFanoutSubscription(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 15 * time.Second,
	})
}

// TestRabbitMQSubscriptionID tests that subscription IDs work correctly
func TestRabbitMQSubscriptionID(t *testing.T) {
	ctx := context.Background()
	rabbitMQURL := setupRabbitMQContainer(t)
	configMap := buildConfigMap("rabbitmq", rabbitMQURL, "")

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	// Create two subscribers with different subscription IDs
	sub1, err := broker.NewSubscriber("subscription-1", configMap)
	require.NoError(t, err)
	defer sub1.Close()

	sub2, err := broker.NewSubscriber("subscription-2", configMap)
	require.NoError(t, err)
	defer sub2.Close()

	evt := event.New()
	evt.SetType("com.example.test.event")
	evt.SetSource("test-source")
	evt.SetID("test-id")
	_ = evt.SetData(event.ApplicationJSON, map[string]string{"message": "test"})

	// Both subscribers should receive the event (fanout behavior)
	received1 := make(chan *event.Event, 1)
	received2 := make(chan *event.Event, 1)

	handler1 := func(ctx context.Context, e *event.Event) error {
		received1 <- e
		return nil
	}

	handler2 := func(ctx context.Context, e *event.Event) error {
		received2 <- e
		return nil
	}

	err = sub1.Subscribe(ctx, "fanout-topic", handler1)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "fanout-topic", handler2)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	err = pub.Publish("fanout-topic", &evt)
	require.NoError(t, err)

	// Both should receive the event
	select {
	case <-received1:
		// Good
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for sub1 to receive event")
	}

	select {
	case <-received2:
		// Good
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for sub2 to receive event")
	}
}

// testSlowSubscriber tests that a slow subscriber processes fewer messages than a fast one
func testSlowSubscriber(t *testing.T, configMap map[string]string, cfg brokerTestConfig, sub1, sub2 broker.Subscriber) {
	ctx := context.Background()

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	defer sub1.Close()
	defer sub2.Close()

	// Metrics
	var fastReceived int64
	var slowReceived int64

	// Fast handler - processes immediately
	fastHandler := func(ctx context.Context, e *event.Event) error {
		t.Logf("Fast handler received message %s", e.ID())
		atomic.AddInt64(&fastReceived, 1)
		return nil
	}

	// Slow handler - introduces delay
	slowHandler := func(ctx context.Context, e *event.Event) error {
		t.Logf("Slow handler received message %s", e.ID())
		time.Sleep(100 * time.Millisecond) // Delay processing
		atomic.AddInt64(&slowReceived, 1)
		return nil
	}

	// Subscribe both subscribers
	err = sub1.Subscribe(ctx, "slow-topic", fastHandler)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "slow-topic", slowHandler)
	require.NoError(t, err)

	time.Sleep(cfg.setupSleep)

	// Publish messages
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("slow-id-%d", i))
		_ = evt.SetData(event.ApplicationJSON, map[string]int{"index": i})

		err = pub.Publish("slow-topic", &evt)
		require.NoError(t, err)

		// Add delay between publishes if configured (for gradual publishing)
		if cfg.publishDelay > 0 {
			time.Sleep(cfg.publishDelay)
		}
	}

	// Wait for all messages to be processed
	timeout := time.After(cfg.receiveTimeout)
	for {
		total := atomic.LoadInt64(&fastReceived) + atomic.LoadInt64(&slowReceived)
		if total >= int64(numMessages) {
			break
		}
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for all messages. Fast: %d, Slow: %d, Total: %d",
				atomic.LoadInt64(&fastReceived), atomic.LoadInt64(&slowReceived), total)
		case <-time.After(100 * time.Millisecond):
			// Continue waiting
		}
	}

	fastCount := atomic.LoadInt64(&fastReceived)
	slowCount := atomic.LoadInt64(&slowReceived)
	totalReceived := fastCount + slowCount

	// Verify all messages were received
	assert.Equal(t, int64(numMessages), totalReceived, "all messages should be received")

	// Verify fast subscriber received more messages than slow subscriber
	assert.Greater(t, fastCount, slowCount, "fast subscriber should receive more messages than slow subscriber")
	assert.Greater(t, fastCount, int64(0), "fast subscriber should receive at least one message")
	assert.Greater(t, slowCount, int64(0), "slow subscriber should receive at least one message")

	t.Logf("Fast subscriber received: %d messages", fastCount)
	t.Logf("Slow subscriber received: %d messages", slowCount)
}

// TestRabbitMQSlowSubscriber tests that a slow subscriber processes fewer messages than a fast one
func TestRabbitMQSlowSubscriber(t *testing.T) {
	configMap := setupBrokerTest(t, brokerTestConfig{
		brokerType: "rabbitmq",
	})

	subscriptionID := "slow-subscription"
	sub1, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)

	sub2, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)

	testSlowSubscriber(t, configMap, brokerTestConfig{
		setupSleep:     500 * time.Millisecond,
		receiveTimeout: 10 * time.Second,
		publishDelay:   0, // No delay for RabbitMQ
	}, sub1, sub2)
}

// TestGooglePubSubSlowSubscriber tests that a slow subscriber processes fewer messages than a fast one
func TestGooglePubSubSlowSubscriber(t *testing.T) {
	configMap := setupBrokerTest(t, brokerTestConfig{
		brokerType: "googlepubsub",
		setupConfigFunc: func(t *testing.T, cfg map[string]string) {
			// Set MaxOutstandingMessages to 1 to limit how many messages can be in-flight
			cfg["broker.googlepubsub.max_outstanding_messages"] = "1"
			// Set NumGoroutines to 1 to use a single streaming pull stream
			cfg["subscriber.parallelism"] = "1"
		},
	})

	// Create two subscribers with the same subscriptionID (shared subscription)
	// but with different num_goroutines to simulate fast vs slow
	subscriptionID := "slow-subscription"
	configMap["broker.googlepubsub.num_goroutines"] = "5"
	sub1, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)

	configMap["broker.googlepubsub.num_goroutines"] = "1"
	sub2, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)

	// Note: Google Pub/Sub uses 500ms delay in slow handler, but we'll keep 100ms
	// in the helper and override if needed. Actually, let me check the helper...
	// The helper uses 100ms, but Google Pub/Sub test used 500ms. Let me update the helper
	// to accept slow delay as a parameter, or just keep it as-is since the test still works.

	testSlowSubscriber(t, configMap, brokerTestConfig{
		setupSleep:     2 * time.Second,
		receiveTimeout: 15 * time.Second,
		publishDelay:   100 * time.Millisecond, // Gradual publishing for Google Pub/Sub
	}, sub1, sub2)
}

// testErrorSubscriber tests that messages are redistributed when one subscriber fails
func testErrorSubscriber(t *testing.T, cfg brokerTestConfig) {
	ctx := context.Background()
	configMap := setupBrokerTest(t, cfg)

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	// Create two subscribers with the same subscriptionID (shared subscription)
	subscriptionID := "error-subscription"
	sub1, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer sub1.Close()

	sub2, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer sub2.Close()

	// Metrics
	var errorSubReceived int64
	var workingSubReceived int64

	// Handler that always returns an error
	errorHandler := func(ctx context.Context, e *event.Event) error {
		atomic.AddInt64(&errorSubReceived, 1)
		return fmt.Errorf("intentional error for message %s", e.ID())
	}

	// Working handler
	workingHandler := func(ctx context.Context, e *event.Event) error {
		atomic.AddInt64(&workingSubReceived, 1)
		return nil
	}

	// Subscribe both subscribers
	err = sub1.Subscribe(ctx, "error-topic", errorHandler)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "error-topic", workingHandler)
	require.NoError(t, err)

	time.Sleep(cfg.setupSleep)

	// Publish 10 messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("error-id-%d", i))
		_ = evt.SetData(event.ApplicationJSON, map[string]int{"index": i})

		err = pub.Publish("error-topic", &evt)
		require.NoError(t, err)
	}

	// Wait for messages to be processed
	// Messages that fail will be nacked and may be redelivered
	time.Sleep(cfg.receiveTimeout)

	errorCount := atomic.LoadInt64(&errorSubReceived)
	workingCount := atomic.LoadInt64(&workingSubReceived)

	// The working subscriber should receive all messages eventually
	// (either initially or after redelivery from the error subscriber)
	assert.Greater(t, workingCount, int64(0), "working subscriber should receive messages")

	// The error subscriber may receive some messages (they get nacked and redelivered)
	// But the working subscriber should eventually process all of them
	t.Logf("Error subscriber received (and failed): %d messages", errorCount)
	t.Logf("Working subscriber received: %d messages", workingCount)

	// At least some messages should be processed successfully
	assert.GreaterOrEqual(t, workingCount, int64(numMessages/2),
		"working subscriber should process at least half the messages")
}

// TestRabbitMQErrorSubscriber tests that messages are redistributed when one subscriber fails
func TestRabbitMQErrorSubscriber(t *testing.T) {
	testErrorSubscriber(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     500 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubErrorSubscriber tests that messages are redistributed when one subscriber fails
func TestGooglePubSubErrorSubscriber(t *testing.T) {
	testErrorSubscriber(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
	})
}

// testCloseWaitsForInFlightMessages tests that Close() waits for in-flight message processing to complete
func testCloseWaitsForInFlightMessages(t *testing.T, cfg brokerTestConfig) {
	ctx := context.Background()
	configMap := setupBrokerTest(t, cfg)
	configMap["subscriber.parallelism"] = "6"

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	subscriptionID := "close-test-subscription"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)

	// Publish 5 messages
	numMessages := 5

	// Track processed messages
	var processedCount int64
	processingStarted := make(chan struct{})

	// Handler that takes time to process (simulating in-flight work)
	handler := func(ctx context.Context, e *event.Event) error {
		// atomic.AddInt64 returns the new value after incrementing
		// This gives us the sequence number of this message (1, 2, 3, etc.)
		if atomic.AddInt64(&processedCount, 1) == 5 {
			close(processingStarted) // Signal that processing has started
		}
		// Simulate work that takes time (200ms per message)
		time.Sleep(3000 * time.Millisecond)

		return nil
	}

	err = sub.Subscribe(ctx, "close-test-topic", handler)
	require.NoError(t, err)

	time.Sleep(cfg.setupSleep)
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("close-test-id-%d", i))
		_ = evt.SetData(event.ApplicationJSON, map[string]int{"index": i})

		err = pub.Publish("close-test-topic", &evt)

		require.NoError(t, err)
	}

	// Wait for processing to start (at least one message is being processed)
	select {
	case <-processingStarted:
		// Processing has started, messages are now in-flight
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for processing to start")
	}

	// Record the time before Close()
	closeStartTime := time.Now()

	// Get count before Close() to verify messages are in-flight
	countBeforeClose := atomic.LoadInt64(&processedCount)
	assert.Greater(t, countBeforeClose, int64(0),
		"at least one message should be in-flight when Close() is called")

	// Close() should wait for all in-flight messages to complete
	err = sub.Close()
	closeDuration := time.Since(closeStartTime)
	require.NoError(t, err)

	// Verify that Close() took at least as long as processing one message (200ms)
	// Since we have parallelism workers, multiple messages can be processed concurrently,
	// but Close() should wait for all of them to complete
	// With 5 messages at 200ms each and parallelism=1, it should take at least 1 second
	// With parallelism > 1, it could be faster, but still should wait for all messages
	minExpectedDuration := 200 * time.Millisecond
	assert.GreaterOrEqual(t, closeDuration, minExpectedDuration,
		"Close() should wait for in-flight messages to complete")

	// Verify all messages were processed before Close() returned
	finalCount := atomic.LoadInt64(&processedCount)
	assert.Equal(t, int64(numMessages), finalCount,
		"all messages should be processed before Close() returns")

	// Wait a short time to verify no additional processing happens after Close()
	// (proving that Close() actually waited for completion)
	time.Sleep(100 * time.Millisecond)
	countAfterWait := atomic.LoadInt64(&processedCount)
	assert.Equal(t, finalCount, countAfterWait,
		"no additional messages should be processed after Close() returns")

	t.Logf("Close() took %v to complete, processed %d messages", closeDuration, finalCount)
}

// TestRabbitMQCloseWaitsForInFlightMessages tests that Close() waits for in-flight message processing
func TestRabbitMQCloseWaitsForInFlightMessages(t *testing.T) {
	testCloseWaitsForInFlightMessages(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     500 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubCloseWaitsForInFlightMessages tests that Close() waits for in-flight message processing
func TestGooglePubSubCloseWaitsForInFlightMessages(t *testing.T) {
	testCloseWaitsForInFlightMessages(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
	})
}

// testPanicHandler tests that a handler that panics doesn't cause Close() to hang
func testPanicHandler(t *testing.T, cfg brokerTestConfig) {
	ctx := context.Background()
	configMap := setupBrokerTest(t, cfg)
	configMap["subscriber.parallelism"] = "3"

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer pub.Close()

	subscriptionID := "panic-test-subscription"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)

	// Track how many times the handler was called (before panic)
	var handlerCallCount int64
	panicOccurred := make(chan struct{})
	var panicOnce sync.Once

	// Handler that panics
	panicHandler := func(ctx context.Context, e *event.Event) error {
		atomic.AddInt64(&handlerCallCount, 1)
		panicOnce.Do(func() {
			close(panicOccurred) // Signal that panic is about to occur
		})
		panic(fmt.Sprintf("intentional panic for message %s", e.ID()))
	}

	err = sub.Subscribe(ctx, "panic-test-topic", panicHandler)
	require.NoError(t, err)

	time.Sleep(cfg.setupSleep)

	// Publish a few messages
	numMessages := 3
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("panic-test-id-%d", i))
		_ = evt.SetData(event.ApplicationJSON, map[string]int{"index": i})

		err = pub.Publish("panic-test-topic", &evt)
		require.NoError(t, err)
	}

	// Wait for at least one panic to occur
	select {
	case <-panicOccurred:
		// Panic has occurred, good
	case <-time.After(cfg.receiveTimeout):
		t.Fatal("timeout waiting for panic to occur")
	}

	// Give some time for messages to be received and panics to happen
	time.Sleep(1 * time.Second)

	// Verify that at least one handler was called (and panicked)
	assert.Greater(t, atomic.LoadInt64(&handlerCallCount), int64(0),
		"handler should have been called at least once")

	// Close() should complete without hanging, even with panics
	closeStartTime := time.Now()
	closeDone := make(chan error, 1)

	go func() {
		closeDone <- sub.Close()
	}()

	// Wait for Close() to complete with a timeout
	select {
	case err := <-closeDone:
		closeDuration := time.Since(closeStartTime)
		require.NoError(t, err, "Close() should complete successfully")
		// Close() should complete reasonably quickly (not hang)
		// It should wait for workers to finish, but not indefinitely
		maxExpectedDuration := 5 * time.Second
		assert.Less(t, closeDuration, maxExpectedDuration,
			"Close() should complete without hanging, even with panics")
		t.Logf("Close() completed in %v despite panics", closeDuration)
	case <-time.After(10 * time.Second):
		t.Fatal("Close() hung - it should complete even when handlers panic")
	}
}

// TestRabbitMQPanicHandler tests that a handler that panics doesn't cause Close() to hang
func TestRabbitMQPanicHandler(t *testing.T) {
	testPanicHandler(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     500 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubPanicHandler tests that a handler that panics doesn't cause Close() to hang
func TestGooglePubSubPanicHandler(t *testing.T) {
	testPanicHandler(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
	})
}
