package integration_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/common"
	pubsub "github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/googlepubsub"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/rabbitmq"
)

// TestMain sets up the test environment for Podman and disables Ryuk
func TestMain(m *testing.M) {
	common.SetupTestEnvironment()

	// Run tests
	code := m.Run()
	os.Exit(code)
}

// brokerTestConfig holds broker-specific test configuration
type brokerTestConfig struct {
	brokerType     string
	setupSleep     time.Duration
	receiveTimeout time.Duration
}

// setupBrokerTest sets up a broker test environment and returns the config map
func setupBrokerTest(t *testing.T, cfg brokerTestConfig) map[string]string {
	var configMap map[string]string

	switch cfg.brokerType {
	case "rabbitmq":
		rabbitMQURL := rabbitmq.SetupRabbitMQContainer(t)
		configMap = common.BuildConfigMap("rabbitmq", rabbitMQURL, "")
	case "googlepubsub":
		projectID, _ := pubsub.SetupPubSubEmulator(t)
		configMap = common.BuildConfigMap("googlepubsub", "", projectID)
	default:
		t.Fatalf("unsupported broker type: %s", cfg.brokerType)
	}

	return configMap
}

// ==================== MAIN TEST ====================
// testGoroutineLeak demonstrates the goroutine leak in the current implementation
// This test WILL FAIL, proving that goroutines are leaked after Close()
func testGoroutineLeak(t *testing.T, cfg brokerTestConfig) {
	configMap := setupBrokerTest(t, cfg)
	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)

	// Clean up environment
	waitForGC()

	before := runtime.NumGoroutine()
	t.Logf("📊 Goroutines BEFORE: %d", before)

	sub, err := broker.NewSubscriber("leak-demo", configMap)
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

		err = pub.Publish("topic-"+strconv.Itoa(i), &evt)
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
	t.Log("   Current Close() only calls s.sub.Close() (line 124)")
	t.Log("   It does NOT wait for goroutines to finish")
	t.Log("")

	err = sub.Close()
	require.NoError(t, err)

	t.Log("🛑 Calling Close() on publisher...")
	err = pub.Close()
	require.NoError(t, err, "Failed to close publisher")

	waitForGC()

	after := runtime.NumGoroutine()
	leaked := after - before

	t.Logf("📊 Goroutines AFTER Close(): %d", after)
	t.Log("")

	// RESULT
	if leaked > 4 {
		t.Logf("🔴 GOROUTINES LEAKED: %d", leaked)
		t.Log("")
		t.Log("❌ PROBLEM IDENTIFIED:")
		t.Log("   1. Subscribe() creates goroutines but doesn't track them (no sync.WaitGroup)")
		t.Log("   2. Close() doesn't wait for goroutines to terminate")
		t.Log("   3. Goroutines become orphaned and continue consuming resources")
		t.Log("")
		t.Log("📝 PRODUCTION IMPACT:")
		t.Log("   - Memory leak in long-running applications")
		t.Log("   - File descriptor exhaustion")
		t.Log("   - Messages processed after shutdown")
		t.Log("   - Cannot do graceful restarts")
		t.Log("")
		t.Log("🔧 REQUIRED FIX:")
		t.Log("   1. Add sync.WaitGroup to subscriber struct to track goroutines")
		t.Log("   2. Add []context.CancelFunc to cancel all subscriptions")
		t.Log("   3. Make Close() wait for WaitGroup with timeout")
		t.Log("")

		// Fail the test to prove the leak exists
		assert.FailNow(t,
			fmt.Sprintf("GOROUTINE LEAK DETECTED! %d goroutines leaked after Close(). See logs above for details.", leaked))
	} else {
		t.Logf("✅ OK: Only %d goroutines remaining (acceptable)", leaked)
	}
}

func waitForGC() {
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
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
	})
}

func testLeakIncreasesWithUsage(t *testing.T, cfg brokerTestConfig) {
	t.Log("=== PROOF: Leak grows with each Subscribe() call ===")
	t.Log("")

	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}

	testCases := []struct {
		numSubscriptions int
		expectedLeak     int
	}{
		{1, 4},  // 1 subscription = 4 goroutines
		{3, 12}, // 3 subscriptions = 12 goroutines
		{5, 20}, // scriptions = 20 goroutines
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d_subscriptions", tc.numSubscriptions), func(t *testing.T) {
			waitForGC()
			before := runtime.NumGoroutine()

			configMap := setupBrokerTest(t, cfg)

			sub, err := broker.NewSubscriber("leak-demo", configMap)
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

			t.Logf("Subscriptions: %d | Before: %d | During: %d | After: %d | Leaked: %d",
				tc.numSubscriptions, before, during, after, leaked)

			assert.Equal(t, 0, leaked, "Leak should be 0")
		})
	}

	t.Log("")
}

// TestRabbitMQLeakIncreasesWithUsage tests that leak grows with each Subscribe() call using RabbitMQ
func TestRabbitMQLeakIncreasesWithUsage(t *testing.T) {
	testLeakIncreasesWithUsage(t, brokerTestConfig{
		brokerType:     "rabbitmq",
		setupSleep:     100 * time.Millisecond,
		receiveTimeout: 5 * time.Second,
	})
}

// TestGooglePubSubLeakIncreasesWithUsage tests that leak grows with each Subscribe() call using Google Pub/Sub
func TestGooglePubSubLeakIncreasesWithUsage(t *testing.T) {
	testLeakIncreasesWithUsage(t, brokerTestConfig{
		brokerType:     "googlepubsub",
		setupSleep:     2 * time.Second,
		receiveTimeout: 10 * time.Second,
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
	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)

	sub, err := broker.NewSubscriber("same-topic-test", configMap)
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

		err = pub.Publish(sameTopic, &evt)
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

	t.Logf("📊 Goroutines AFTER Close(): %d", after)
	t.Logf("📊 Goroutines leaked: %d", leaked)
	t.Log("")

	// Verify no goroutine leaks
	if leaked > 4 {
		t.Logf("🔴 GOROUTINES LEAKED: %d", leaked)
		t.Log("")
		t.Log("❌ PROBLEM: Multiple subscriptions to the same topic leak goroutines")
		t.Log("   Each Subscribe() call creates goroutines that are not properly cleaned up")
		t.Log("")
		assert.FailNow(t,
			fmt.Sprintf("GOROUTINE LEAK DETECTED! %d goroutines leaked after Close() with multiple subscriptions to same topic.", leaked))
	} else {
		t.Logf("✅ SUCCESS: Only %d goroutines remaining (acceptable)", leaked)
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
	})
}
