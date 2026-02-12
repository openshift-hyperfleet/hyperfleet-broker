package integration_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcRabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/common"
)

// Shared container URLs created once in TestMain and reused across all tests in this package.
var (
	sharedRabbitMQURL    string
	sharedPubSubProjectID string
)

// TestMain sets up the test environment, creates shared RabbitMQ and PubSub containers,
// runs all tests, then terminates the containers.
func TestMain(m *testing.M) {
	common.SetupTestEnvironment()

	ctx := context.Background()

	// Start RabbitMQ container
	rabbitmqContainer, err := tcRabbitmq.Run(ctx,
		"rabbitmq:3-management-alpine",
		tcRabbitmq.WithAdminUsername("guest"),
		tcRabbitmq.WithAdminPassword("guest"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start rabbitmq container: %v\n", err)
		os.Exit(1)
	}

	connectionString, err := rabbitmqContainer.AmqpURL(ctx)
	if err != nil {
		_ = rabbitmqContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get rabbitmq AMQP URL: %v\n", err)
		os.Exit(1)
	}
	sharedRabbitMQURL = connectionString

	// Start PubSub emulator container
	pubsubReq := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
		ExposedPorts: []string{"8085/tcp"},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"},
		WaitingFor: wait.ForLog("Server started").
			WithOccurrence(1).
			WithStartupTimeout(60 * time.Second),
	}

	pubsubContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pubsubReq,
		Started:          true,
	})
	if err != nil {
		_ = rabbitmqContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to start pubsub emulator container: %v\n", err)
		os.Exit(1)
	}

	sharedPubSubProjectID = "test-project"

	host, err := pubsubContainer.Host(ctx)
	if err != nil {
		_ = rabbitmqContainer.Terminate(ctx)
		_ = pubsubContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get pubsub container host: %v\n", err)
		os.Exit(1)
	}

	mappedPort, err := pubsubContainer.MappedPort(ctx, "8085")
	if err != nil {
		_ = rabbitmqContainer.Terminate(ctx)
		_ = pubsubContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get pubsub container mapped port: %v\n", err)
		os.Exit(1)
	}

	emulatorHost := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	if err := os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost); err != nil {
		_ = rabbitmqContainer.Terminate(ctx)
		_ = pubsubContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to set PUBSUB_EMULATOR_HOST: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	_ = os.Unsetenv("PUBSUB_EMULATOR_HOST")
	if err := rabbitmqContainer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate rabbitmq container: %v\n", err)
	}
	if err := pubsubContainer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate pubsub container: %v\n", err)
	}

	os.Exit(code)
}

// brokerTestConfig holds broker-specific test configuration
type brokerTestConfig struct {
	brokerType     string
	setupSleep     time.Duration
	receiveTimeout time.Duration
	// leakTolerance is the number of goroutines allowed to remain after Close().
	// Google PubSub's gRPC client and Watermill's non-context-aware backoff retries
	// can leave goroutines that take time to terminate. This tolerance accounts for that.
	leakTolerance int
}

// setupBrokerTest sets up a broker test environment and returns the config map
// using the shared containers created in TestMain.
func setupBrokerTest(t *testing.T, cfg brokerTestConfig) map[string]string {
	var configMap map[string]string

	switch cfg.brokerType {
	case "rabbitmq":
		configMap = common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")
	case "googlepubsub":
		configMap = common.BuildConfigMap("googlepubsub", "", sharedPubSubProjectID)
	default:
		t.Fatalf("unsupported broker type: %s", cfg.brokerType)
	}

	return configMap
}

// testGoroutineLeak verifies that Close() properly cleans up all goroutines
// created by Subscribe(). Acts as a regression guard against goroutine leaks.
func testGoroutineLeak(t *testing.T, cfg brokerTestConfig) {
	configMap := setupBrokerTest(t, cfg)
	pub, err := broker.NewPublisher(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), configMap)
	require.NoError(t, err)

	// Clean up environment
	waitForGC()

	before := runtime.NumGoroutine()
	t.Logf("📊 Goroutines BEFORE: %d", before)

	sub, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "leak-demo", configMap)
	require.NoError(t, err)

	ctx := context.Background()

	// Simple handler
	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}

	// Call Subscribe() multiple times (simulates real usage)
	numSubscriptions := 3
	t.Logf("🔄 Calling Subscribe() %d times (parallelism=%d)...", numSubscriptions, 2)
	t.Log("")

	for i := 0; i < numSubscriptions; i++ {
		err := sub.Subscribe(ctx, fmt.Sprintf("topic-%d", i), handler)
		require.NoError(t, err)
	}

	for i := 0; i < numSubscriptions; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("error-id-%d", i))
		if err := evt.SetData(event.ApplicationJSON, map[string]int{"index": i}); err != nil {
			require.NoError(t, err, "failed to set event data")
		}

		err = pub.Publish(context.Background(), "topic-"+strconv.Itoa(i), &evt)
		require.NoError(t, err)
	}

	// Wait for goroutines to start
	time.Sleep(cfg.setupSleep)

	during := runtime.NumGoroutine()
	created := during - before
	t.Logf("📊 Goroutines DURING subscribe: %d (created: %d)", during, created)
	t.Log("")

	// Explain what was created
	t.Log("💡 What Subscribe() creates per call:")
	t.Log("   - parallelism workers (2)")
	t.Log("   - 1 message distributor goroutine (line 61)")
	t.Log("   - 1 waiter goroutine (line 81)")
	t.Logf("   = 4 goroutines per Subscribe() call")
	t.Logf("   Total expected: %d subscriptions × 4 = ~%d goroutines", numSubscriptions,
		numSubscriptions*4)
	t.Log("")

	// Close subscriber
	t.Log("🛑 Calling Close()...")

	err = sub.Close()
	require.NoError(t, err)

	t.Log("🛑 Calling Close() on publisher...")
	err = pub.Close()
	require.NoError(t, err, "Failed to close publisher")

	waitForGC()

	after := runtime.NumGoroutine()
	leaked := after - before

	t.Logf("📊 Goroutines AFTER Close(): %d (tolerance: %d)", after, cfg.leakTolerance)
	t.Log("")

	// RESULT
	if leaked > cfg.leakTolerance {
		assert.FailNow(t,
			fmt.Sprintf("GOROUTINE LEAK REGRESSION: %d goroutines leaked after Close() (tolerance: %d)", leaked, cfg.leakTolerance))
	} else {
		t.Logf("✅ OK: Only %d goroutines remaining (acceptable, tolerance: %d)", leaked, cfg.leakTolerance)
	}
}

func waitForGC() {
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
}

// TestRabbitMQGoroutineLeak tests goroutine leak with RabbitMQ
func TestRabbitMQGoroutineLeak(t *testing.T) {
	testGoroutineLeak(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     100 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubGoroutineLeak tests goroutine leak with Google Pub/Sub
func TestGooglePubSubGoroutineLeak(t *testing.T) {
	testGoroutineLeak(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
		leakTolerance:  4,
	})
}

func testLeakIncreasesWithUsage(t *testing.T, cfg brokerTestConfig) {
	t.Log("=== Verify goroutine count stays within tolerance as subscriptions grow ===")
	t.Log("")

	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}

	testCases := []struct {
		numSubscriptions int // number of Subscribe() calls to make
	}{
		{1}, // single subscription
		{3}, // moderate usage
		{5}, // heavy usage
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d_subscriptions", tc.numSubscriptions), func(t *testing.T) {
			waitForGC()
			before := runtime.NumGoroutine()

			configMap := setupBrokerTest(t, cfg)

			sub, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "leak-demo", configMap)
			require.NoError(t, err)

			ctx := context.Background()

			// Subscribe N times
			for i := 0; i < tc.numSubscriptions; i++ {
				if err := sub.Subscribe(ctx, fmt.Sprintf("topic-%d", i), handler); err != nil {
					t.Logf("failed to subscribe to topic-%d: %v", i, err)
				}
			}
			time.Sleep(cfg.setupSleep)

			during := runtime.NumGoroutine()

			// Close
			if err := sub.Close(); err != nil {
				t.Logf("failed to close subscriber: %v", err)
			}
			waitForGC()

			after := runtime.NumGoroutine()
			leaked := after - before

			t.Logf("Subscriptions: %d | Before: %d | During: %d | After: %d | Leaked: %d (tolerance: %d)",
				tc.numSubscriptions, before, during, after, leaked, cfg.leakTolerance)

			assert.LessOrEqual(t, leaked, cfg.leakTolerance,
				"Leaked goroutines should be within tolerance")
		})
	}

	t.Log("")
}

// TestRabbitMQLeakIncreasesWithUsage verifies goroutine cleanup scales with subscription count using RabbitMQ
func TestRabbitMQLeakIncreasesWithUsage(t *testing.T) {
	testLeakIncreasesWithUsage(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     100 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubLeakIncreasesWithUsage verifies goroutine cleanup scales with subscription count using Google Pub/Sub
func TestGooglePubSubLeakIncreasesWithUsage(t *testing.T) {
	testLeakIncreasesWithUsage(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
		// The Watermill Google PubSub subscriber uses a non-context-aware exponential backoff
		// for retries and the gRPC client may leave goroutines that take time to terminate.
		// Allow a small tolerance per subscription.
		leakTolerance: 4,
	})
}

// testMultipleSubscriptionsSameTopic tests that subscribing multiple times to the same topic
// and then closing the subscriber does not leak goroutines
func testMultipleSubscriptionsSameTopic(t *testing.T, cfg brokerTestConfig) {
	t.Log("=== Testing: Multiple subscriptions to the SAME topic ===")
	t.Log("")

	configMap := setupBrokerTest(t, cfg)

	waitForGC()

	before := runtime.NumGoroutine()
	t.Logf("📊 Goroutines BEFORE: %d", before)

	// Create publisher and subscriber
	pub, err := broker.NewPublisher(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), configMap)
	require.NoError(t, err)

	sub, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "same-topic-test", configMap)
	require.NoError(t, err)

	ctx := context.Background()

	// Track how many times each handler is called
	var mu sync.Mutex
	handlerCallCounts := make(map[int]int)

	// Create multiple handlers for the same topic
	numSubscriptions := 5
	sameTopic := "shared-topic"

	t.Logf("🔄 Subscribing %d times to the SAME topic: '%s'", numSubscriptions, sameTopic)
	t.Log("   Each subscription creates:")
	t.Log("   - parallelism workers (1)")
	t.Log("   - 1 message distributor goroutine")
	t.Log("   = 2 goroutines per subscription")
	t.Log("")

	// Subscribe multiple times to the same topic
	for i := 0; i < numSubscriptions; i++ {
		handlerID := i
		handler := func(ctx context.Context, evt *event.Event) error {
			mu.Lock()
			handlerCallCounts[handlerID]++
			mu.Unlock()
			return nil
		}
		err := sub.Subscribe(ctx, sameTopic, handler)
		require.NoError(t, err, "Failed to subscribe handler %d", i)
	}

	// Wait for goroutines to start
	time.Sleep(cfg.setupSleep)

	during := runtime.NumGoroutine()
	created := during - before
	t.Logf("📊 Goroutines DURING subscribe: %d (created: %d)", during, created)
	t.Log("")

	// Publish some messages to verify subscriptions are working
	numMessages := 3
	t.Logf("📨 Publishing %d messages to topic '%s'...", numMessages, sameTopic)
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("msg-id-%d", i))
		if err := evt.SetData(event.ApplicationJSON, map[string]int{"index": i}); err != nil {
			require.NoError(t, err, "failed to set event data")
		}

		err = pub.Publish(context.Background(), sameTopic, &evt)
		require.NoError(t, err)
	}

	// Give handlers time to process messages
	time.Sleep(200 * time.Millisecond)

	// Close subscriber
	t.Log("🛑 Calling Close()...")
	err = sub.Close()
	require.NoError(t, err)

	t.Log("🛑 Calling Close() on publisher...")
	err = pub.Close()
	require.NoError(t, err, "Failed to close publisher")

	// Wait for cleanup
	waitForGC()

	after := runtime.NumGoroutine()
	leaked := after - before

	t.Logf("📊 Goroutines AFTER Close(): %d (tolerance: %d)", after, cfg.leakTolerance)
	t.Logf("📊 Goroutines leaked: %d", leaked)
	t.Log("")

	// Verify no goroutine leaks
	if leaked > cfg.leakTolerance {
		assert.FailNow(t,
			fmt.Sprintf("GOROUTINE LEAK REGRESSION: %d goroutines leaked after Close() with multiple subscriptions to same topic (tolerance: %d)", leaked, cfg.leakTolerance))
	} else {
		t.Logf("✅ SUCCESS: Only %d goroutines remaining (acceptable, tolerance: %d)", leaked, cfg.leakTolerance)
		t.Log("   All goroutines from multiple subscriptions to the same topic were properly cleaned up")
	}
}

// TestRabbitMQMultipleSubscriptionsSameTopic tests multiple subscriptions to the same topic with RabbitMQ
func TestRabbitMQMultipleSubscriptionsSameTopic(t *testing.T) {
	testMultipleSubscriptionsSameTopic(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     100 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubMultipleSubscriptionsSameTopic tests multiple subscriptions to the same topic with Google Pub/Sub
func TestGooglePubSubMultipleSubscriptionsSameTopic(t *testing.T) {
	testMultipleSubscriptionsSameTopic(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
		leakTolerance:  4,
	})
}
