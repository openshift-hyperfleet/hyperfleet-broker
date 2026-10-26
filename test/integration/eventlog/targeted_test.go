package eventlog_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/common"
)

// TestVisibilityOrderingUnderConcurrentPublishers proves delivery is
// exactly-once and per-publisher ordered when several publishers commit to
// the same topic concurrently. The per-topic sequence counter serializes
// allocation with commit, so an in-flight lower sequence cannot hide behind
// a committed higher one; this test verifies that property end to end.
func TestVisibilityOrderingUnderConcurrentPublishers(t *testing.T) {
	const publishers, perPublisher = 4, 25
	total := publishers * perPublisher

	configMap := eventLogConfigMap()
	topic := fmt.Sprintf("visibility-topic-%d", time.Now().UnixNano())

	pub, err := broker.NewPublisher(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	var mu sync.Mutex
	received := make([]string, 0, total)
	sub, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "visibility-sub", common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()

	handler := func(_ context.Context, evt *event.Event) error {
		mu.Lock()
		received = append(received, evt.ID())
		mu.Unlock()
		return nil
	}
	require.NoError(t, sub.Subscribe(context.Background(), topic, handler))
	time.Sleep(500 * time.Millisecond)

	published := make([][]string, publishers)
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				evt := event.New()
				evt.SetType("com.example.test.event")
				evt.SetSource("test-source")
				evt.SetID(fmt.Sprintf("pub%d-seq%d", p, i))
				require.NoError(t, evt.SetData(event.ApplicationJSON, map[string]int{"pub": p, "i": i}))
				require.NoError(t, pub.Publish(context.Background(), topic, &evt))
				published[p] = append(published[p], evt.ID())
			}
		}(p)
	}
	wg.Wait()

	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		count := len(received)
		mu.Unlock()
		if count >= total || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, total, "every event should be delivered")

	// Exactly once: no losses and no duplicates behind the cursor.
	counts := make(map[string]int, total)
	for _, id := range received {
		counts[id]++
	}
	for id, n := range counts {
		assert.Equal(t, 1, n, "event %s delivered %d times", id, n)
	}

	// Per-publisher ordering: each publisher's events arrive in publish order.
	position := make(map[string]int, total)
	for i, id := range received {
		position[id] = i
	}
	for p, ids := range published {
		last := -1
		for _, id := range ids {
			pos, ok := position[id]
			require.True(t, ok, "event %s never delivered", id)
			assert.Greater(t, pos, last, "publisher %d events out of order: %s at %d after %d", p, id, pos, last)
			last = pos
		}
	}
}

// TestCrashRecoveryRedeliversUnackedRange proves that a range whose holder
// stops progressing is reclaimed after its lease expires and its events are
// redelivered without loss. Subscriber A blocks inside its handler (a
// stand-in for a crashed process: no heartbeat, no ack); subscriber B, on
// the same subscription ID, must reclaim and deliver everything.
func TestCrashRecoveryRedeliversUnackedRange(t *testing.T) {
	const numEvents = 5

	configMap := eventLogConfigMap()
	configMap["broker.eventlog.lease_duration"] = "500ms"
	topic := fmt.Sprintf("crash-topic-%d", time.Now().UnixNano())

	pub, err := broker.NewPublisher(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	var mu sync.Mutex
	deliveredByA := make(map[string]int)
	subA, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "crash-sub", common.NewTestMetrics(t), configMap)
	require.NoError(t, err)

	handlerA := func(ctx context.Context, evt *event.Event) error {
		mu.Lock()
		deliveredByA[evt.ID()]++
		mu.Unlock()
		// Stand-in for a crashed process: hold the event far past the lease.
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(10 * time.Second):
			return nil
		}
	}
	require.NoError(t, subA.Subscribe(context.Background(), topic, handlerA))

	for i := 0; i < numEvents; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("crash-id-%d", i))
		require.NoError(t, evt.SetData(event.ApplicationJSON, map[int]int{i: i}))
		require.NoError(t, pub.Publish(context.Background(), topic, &evt))
	}

	// Wait until A has picked up at least one event (it then blocks).
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		count := len(deliveredByA)
		mu.Unlock()
		if count >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NotEmpty(t, deliveredByA, "subscriber A should deliver at least one event before blocking")

	// B joins the same subscription and must reclaim the expired range.
	deliveredByB := make(map[string]int)
	subB, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "crash-sub", common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	handlerB := func(_ context.Context, evt *event.Event) error {
		mu.Lock()
		deliveredByB[evt.ID()]++
		mu.Unlock()
		return nil
	}
	require.NoError(t, subB.Subscribe(context.Background(), topic, handlerB))

	deadline = time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		count := len(deliveredByB)
		mu.Unlock()
		if count >= numEvents || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.NoError(t, subA.Close())
	require.NoError(t, subB.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, deliveredByB, numEvents, "subscriber B should receive every event after reclaiming")

	// No losses, duplicates bounded: each event delivered at most once by
	// each subscriber.
	for i := 0; i < numEvents; i++ {
		id := fmt.Sprintf("crash-id-%d", i)
		assert.LessOrEqual(t, deliveredByA[id]+deliveredByB[id], 2, "event %s delivered too many times", id)
	}
}

// TestHoleResolutionAfterInjectedHole proves the cursor resumes past a
// sequence hole within the grace period. The publisher allocates the
// sequence in the same transaction as the insert, so it cannot produce
// holes itself; this test injects one directly to prove the reader's
// robustness against a producer that loses an allocated sequence.
func TestHoleResolutionAfterInjectedHole(t *testing.T) {
	const numEvents = 5

	configMap := eventLogConfigMap()
	configMap["broker.eventlog.grace_period"] = "500ms"
	topic := fmt.Sprintf("hole-topic-%d", time.Now().UnixNano())

	pub, err := broker.NewPublisher(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	received := make(chan string, numEvents)
	sub, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "hole-sub", common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()

	handler := func(_ context.Context, evt *event.Event) error {
		received <- evt.ID()
		return nil
	}
	require.NoError(t, sub.Subscribe(context.Background(), topic, handler))
	time.Sleep(500 * time.Millisecond)

	publish := func(i int) {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("hole-id-%d", i))
		require.NoError(t, evt.SetData(event.ApplicationJSON, map[int]int{i: i}))
		require.NoError(t, pub.Publish(context.Background(), topic, &evt))
	}
	for i := 0; i < 3; i++ {
		publish(i)
	}

	// Inject a hole: advance the topic's sequence without an event.
	pool, err := pgxpool.New(context.Background(), sharedPostgresDSN)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`UPDATE broker_topic_seq SET next = next + 1 WHERE topic = $1`, topic)
	require.NoError(t, err)
	pool.Close()

	for i := 3; i < numEvents; i++ {
		publish(i)
	}

	// All events must arrive within a bound well below an unbounded stall;
	// the hole consumes at most the grace period plus poll cycles.
	var got []string
	deadline := time.Now().Add(10 * time.Second)
	for len(got) < numEvents && time.Now().Before(deadline) {
		select {
		case id := <-received:
			got = append(got, id)
		case <-time.After(100 * time.Millisecond):
		}
	}

	require.Len(t, got, numEvents, "every event should be delivered past the hole")
	unique := make(map[string]bool, len(got))
	for _, id := range got {
		unique[id] = true
	}
	assert.Len(t, unique, numEvents, "no duplicates around the hole")
}

// TestTransactionPoolingThroughPgBouncer runs a focused subset of the
// shared scenarios through PgBouncer in transaction mode: publish, shared
// subscription (competing range claims), and fan-out (independent cursors).
func TestTransactionPoolingThroughPgBouncer(t *testing.T) {
	configMap := common.BuildConfigMap("eventlog", sharedPgBouncerDSN, "")

	common.RunPublisherSubscriber(t, configMap, common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})

	common.RunSharedSubscription(t, configMap, common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 20 * time.Second,
	})

	common.RunFanoutSubscription(t, configMap, common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 20 * time.Second,
	})
}
