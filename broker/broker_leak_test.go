package broker

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupRabbitMQContainer(t *testing.T) string {
	ctx := context.Background()

	rabbitmqContainer, err := rabbitmq.RunContainer(ctx,
		testcontainers.WithImage("rabbitmq:3"),
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

// ==================== MAIN TEST ====================
// TestGoroutineLeakProof demonstrates the goroutine leak in the current implementation
// This test WILL FAIL, proving that goroutines are leaked after Close()
func TestGoroutineLeak(t *testing.T) {

	rabbitMQURL := setupRabbitMQContainer(t)
	configMap := buildConfigMap("rabbitmq", rabbitMQURL, "")
	pub, err := NewPublisher(configMap)

	// Clean up environment
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	before := runtime.NumGoroutine()
	t.Logf("📊 Goroutines BEFORE: %d", before)

	sub, err := NewSubscriber("leak-demo", configMap)

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
		evt.SetData(event.ApplicationJSON, map[string]int{"index": i})

		err = pub.Publish("topic-"+strconv.Itoa(i), &evt)
		require.NoError(t, err)
	}

	// Wait for goroutines to start
	time.Sleep(100 * time.Millisecond)

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

	// Wait for cleanup (if any)
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

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

func TestLeakIncreasesWithUsage2(t *testing.T) {
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
		{5, 20}, // 5 subscriptions = 20 goroutines
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d_subscriptions", tc.numSubscriptions), func(t *testing.T) {
			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			before := runtime.NumGoroutine()

			rabbitMQURL := setupRabbitMQContainer(t)
			configMap := buildConfigMap("rabbitmq", rabbitMQURL, "")

			sub, err := NewSubscriber("leak-demo", configMap)
			require.NoError(t, err)

			ctx := context.Background()

			// Subscribe N times
			for i := 0; i < tc.numSubscriptions; i++ {
				sub.Subscribe(ctx, fmt.Sprintf("topic-%d", i), handler)
			}
			time.Sleep(100 * time.Millisecond)

			during := runtime.NumGoroutine()

			// Close
			sub.Close()
			time.Sleep(300 * time.Millisecond)
			runtime.GC()
			time.Sleep(50 * time.Millisecond)

			after := runtime.NumGoroutine()
			leaked := after - before

			t.Logf("Subscriptions: %d | Before: %d | During: %d | After: %d | Leaked: %d",
				tc.numSubscriptions, before, during, after, leaked)
			t.Logf("Expected leak: ~%d | Actual leak: %d", tc.expectedLeak, leaked)

			assert.Equal(t, 0, leaked, "Leak should be 0")
		})
	}

	t.Log("")
	t.Log("💡 OBSERVATION: Leak increases proportionally with Subscribe() calls")
	t.Log("   This proves goroutines are accumulated without any lifecycle management")
}
