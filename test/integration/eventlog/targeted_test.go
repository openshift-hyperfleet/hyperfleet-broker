package eventlog_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/testutil"
)

const testInterval = "100ms"

// waitFor polls cond until it holds or the timeout passes, reusing one
// timer across iterations per the resource-lifecycle standard.
func waitFor(timeout time.Duration, cond func() bool) bool {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		<-timer.C
		timer.Reset(100 * time.Millisecond)
	}
}

// collectUntil gathers values from ch until n arrive or the timeout
// passes, reusing one timer across iterations.
func collectUntil[T any](ch chan T, n int, timeout time.Duration) []T {
	var got []T
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	deadline := time.Now().Add(timeout)
	for len(got) < n && time.Now().Before(deadline) {
		select {
		case v := <-ch:
			got = append(got, v)
		case <-timer.C:
			timer.Reset(100 * time.Millisecond)
		}
	}
	return got
}

// TestVisibilityOrderingUnderConcurrentPublishers verifies no losses and
// no duplicates in a failure-free run, and per-publisher ordering, when
// several publishers commit to the same topic concurrently. The per-topic
// sequence counter serializes allocation with commit, so an in-flight
// lower sequence cannot hide behind a committed higher one; this test
// verifies that property end to end.
func TestVisibilityOrderingUnderConcurrentPublishers(t *testing.T) {
	const publishers, perPublisher = 4, 25
	total := publishers * perPublisher

	configMap := postgresConfigMap(sharedPostgresDSN)
	topic := fmt.Sprintf("visibility-topic-%d", time.Now().UnixNano())

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending

	var mu sync.Mutex
	received := make([]string, 0, total)
	sub, err := broker.NewSubscriber(testutil.NewTestLogger(), "visibility-sub", testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }() // best-effort cleanup; the test is ending

	handler := func(_ context.Context, evt *event.Event) error {
		mu.Lock()
		received = append(received, evt.ID())
		mu.Unlock()
		return nil
	}
	require.NoError(t, sub.Subscribe(context.Background(), topic, handler))
	time.Sleep(eventLogSetupSleep)

	published, err := publishVisibilityEvents(context.Background(), pub, topic, publishers, perPublisher)
	require.NoError(t, err)

	waitFor(20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= total
	})

	mu.Lock()
	receivedSnapshot := append([]string(nil), received...)
	mu.Unlock()
	assertVisibilityResults(t, receivedSnapshot, published, total)
}

func publishVisibilityEvents(
	ctx context.Context, pub broker.Publisher, topic string, publishers, perPublisher int,
) ([][]string, error) {
	published := make([][]string, publishers)
	pubErrs := make(chan error, publishers*perPublisher)
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
				if err := evt.SetData(event.ApplicationJSON, map[string]int{"pub": p, "i": i}); err != nil {
					pubErrs <- fmt.Errorf("publisher %d event %d: %w", p, i, err)
					return
				}
				if err := pub.Publish(ctx, topic, &evt); err != nil {
					pubErrs <- fmt.Errorf("publisher %d event %d: %w", p, i, err)
					return
				}
				published[p] = append(published[p], evt.ID())
			}
		}(p)
	}
	wg.Wait()
	close(pubErrs)
	var errs []error
	for err := range pubErrs {
		errs = append(errs, err)
	}
	return published, errors.Join(errs...)
}

func assertVisibilityResults(t *testing.T, received []string, published [][]string, total int) {
	t.Helper()
	require.Len(t, received, total, "every event should be delivered")

	counts := make(map[string]int, total)
	for _, id := range received {
		counts[id]++
	}
	for id, n := range counts {
		assert.Equal(t, 1, n, "event %s delivered %d times", id, n)
	}

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

	configMap := postgresConfigMap(sharedPostgresDSN)
	configMap["broker.postgres.lease_duration"] = "500ms"
	topic := fmt.Sprintf("crash-topic-%d", time.Now().UnixNano())

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending

	var mu sync.Mutex
	deliveredByA := make(map[string]int)
	subA, err := broker.NewSubscriber(testutil.NewTestLogger(), "crash-sub", testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = subA.Close() }() // best-effort cleanup; the test is ending

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
	var deliveredCount int
	waitFor(10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		deliveredCount = len(deliveredByA)
		return deliveredCount >= 1
	})
	require.GreaterOrEqual(t, deliveredCount, 1, "subscriber A should deliver at least one event before blocking")

	// B joins the same subscription and must reclaim the expired range.
	deliveredByB := make(map[string]int)
	subB, err := broker.NewSubscriber(testutil.NewTestLogger(), "crash-sub", testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = subB.Close() }() // best-effort cleanup; the test is ending
	handlerB := func(_ context.Context, evt *event.Event) error {
		mu.Lock()
		deliveredByB[evt.ID()]++
		mu.Unlock()
		return nil
	}
	require.NoError(t, subB.Subscribe(context.Background(), topic, handlerB))

	waitFor(20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deliveredByB) >= numEvents
	})

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

	configMap := postgresConfigMap(sharedPostgresDSN)
	configMap["broker.postgres.grace_period"] = "500ms"
	topic := fmt.Sprintf("hole-topic-%d", time.Now().UnixNano())

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending

	received := make(chan string, numEvents)
	sub, err := broker.NewSubscriber(testutil.NewTestLogger(), "hole-sub", testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }() // best-effort cleanup; the test is ending

	handler := func(_ context.Context, evt *event.Event) error {
		received <- evt.ID()
		return nil
	}
	require.NoError(t, sub.Subscribe(context.Background(), topic, handler))
	time.Sleep(eventLogSetupSleep)

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
	defer pool.Close() // best-effort cleanup; the test is ending
	_, err = pool.Exec(context.Background(),
		`UPDATE broker_topic_seq SET next = next + 1 WHERE topic = $1`, topic)
	require.NoError(t, err)

	for i := 3; i < numEvents; i++ {
		publish(i)
	}

	// All events must arrive within a bound well below an unbounded stall;
	// the hole consumes at most the grace period plus poll cycles.
	got := collectUntil(received, numEvents, 10*time.Second)

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
	configMap := postgresConfigMap(sharedPgBouncerDSN)

	testutil.RunPublisherSubscriber(t, configMap, postgresScenarioConfig(10*time.Second))

	testutil.RunSharedSubscription(t, configMap, postgresScenarioConfig(20*time.Second))

	testutil.RunFanoutSubscription(t, configMap, postgresScenarioConfig(20*time.Second))
}

func TestDuplicateSubscribeRejected(t *testing.T) {
	sub, err := broker.NewSubscriber(
		testutil.NewTestLogger(), "duplicate-sub", testutil.NewTestMetrics(t), postgresConfigMap(sharedPostgresDSN),
	)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }() // best-effort cleanup; the test is ending

	handler := func(_ context.Context, _ *event.Event) error { return nil }
	topic := fmt.Sprintf("duplicate-topic-%d", time.Now().UnixNano())
	require.NoError(t, sub.Subscribe(context.Background(), topic, handler))
	err = sub.Subscribe(context.Background(), topic, handler)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func TestRetryScopePerSubscription(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)
	configMap["broker.postgres.max_attempts"] = "2"
	configMap["broker.postgres.retry_backoff"] = "100ms"
	topic := fmt.Sprintf("retry-scope-topic-%d", time.Now().UnixNano())

	failing, err := broker.NewSubscriber(testutil.NewTestLogger(), "failing-sub", testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = failing.Close() }() // best-effort cleanup; the test is ending
	require.NoError(t, failing.Subscribe(context.Background(), topic, func(context.Context, *event.Event) error {
		return errors.New("permanent failure")
	}))

	healthy, err := broker.NewSubscriber(testutil.NewTestLogger(), "healthy-sub", testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = healthy.Close() }() // best-effort cleanup; the test is ending
	delivered := make(chan string, 10)
	require.NoError(t, healthy.Subscribe(context.Background(), topic, func(_ context.Context, evt *event.Event) error {
		delivered <- evt.ID()
		return nil
	}))

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending
	for i := 1; i <= 10; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("retry-scope-%d", i))
		require.NoError(t, evt.SetData(event.ApplicationJSON, map[string]int{"i": i}))
		require.NoError(t, pub.Publish(context.Background(), topic, &evt))
	}

	got := collectUntil(delivered, 10, 20*time.Second)
	require.Len(t, got, 10, "the healthy subscription must receive every event")

	select {
	case subErr := <-failing.Errors():
		assert.Contains(t, subErr.Err.Error(), "event skipped after 2 delivery attempts")
	case <-time.After(5 * time.Second):
		t.Fatal("the failing subscription should surface a skipped event")
	}
}

// TestCursorReconciliationPastRetentionBoundary proves the reaper's
// retention-boundary reconciliation: a cursor stalled below events that
// were dropped with their partition advances past them, and the next
// published event is delivered without crawling hole by hole. The grace
// period is tuned long enough that the hole crawl cannot deliver within
// the test's timeout, so delivery can only come through reconciliation.
func TestCursorReconciliationPastRetentionBoundary(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)
	configMap["broker.postgres.grace_period"] = "10s"
	configMap["broker.postgres.retention"] = "720h"
	configMap["subscriber.parallelism"] = "1"

	topic := fmt.Sprintf("reconcile-topic-%d", time.Now().UnixNano())
	subID := fmt.Sprintf("reconcile-sub-%d", time.Now().UnixNano())

	// Seed five events in the three-day-old partition and set the topic's
	// counter past them.
	pool, err := pgxpool.New(context.Background(), sharedPostgresDSN)
	require.NoError(t, err)
	defer pool.Close()
	old := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour).Add(time.Hour)
	for i := 1; i <= 5; i++ {
		_, execErr := pool.Exec(context.Background(), `
			INSERT INTO broker_events (topic, seq, payload, created_at)
			VALUES ($1, $2, $3, $4)`,
			topic, int64(i), `{"id":"seed"}`, old)
		require.NoError(t, execErr)
	}
	_, err = pool.Exec(context.Background(), `
		INSERT INTO broker_topic_seq (topic, next) VALUES ($1, 6)
		ON CONFLICT (topic) DO NOTHING`, topic)
	require.NoError(t, err)

	// Drop the seeded partition through the maintenance routine itself,
	// with a retention window that expires it.
	_, err = pool.Exec(context.Background(), `SELECT broker_maintenance(1)`)
	require.NoError(t, err)
	var exists bool
	err = pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_inherits i ON i.inhrelid = c.oid
			WHERE i.inhparent = 'broker_events'::regclass AND c.relname = $1
		)`, "broker_events_"+old.Format("20060102")).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "the maintenance routine should have dropped the expired partition")

	received := make(chan string, 8)
	handler := func(_ context.Context, evt *event.Event) error {
		received <- evt.ID()
		return nil
	}

	sub, err := broker.NewSubscriber(testutil.NewTestLogger(), subID, testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }() // best-effort cleanup; the test is ending
	require.NoError(t, sub.Subscribe(context.Background(), topic, handler))

	// Rewind the cursor below the dropped events: the subscription now
	// stalls on holes the grace period would not clear in time.
	_, err = pool.Exec(context.Background(), `
		UPDATE broker_cursors SET acked = 0, reserved = 0
		WHERE topic = $1 AND subscription_id = $2`, topic, subID)
	require.NoError(t, err)

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending
	next := event.New()
	next.SetType("com.example.test.event")
	next.SetSource("test-source")
	next.SetID("reconcile-next")
	require.NoError(t, next.SetData(event.ApplicationJSON, map[string]int{"i": 6}))
	require.NoError(t, pub.Publish(context.Background(), topic, &next))

	// A second subscriber on the same subscription starts a reaper whose
	// immediate pass reconciles the cursor past the dropped events; the
	// first subscriber's own reaper already ran before the rewind.
	sub2, err := broker.NewSubscriber(testutil.NewTestLogger(), subID, testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub2.Close() }() // best-effort cleanup; the test is ending
	require.NoError(t, sub2.Subscribe(context.Background(), topic, handler))

	// Reconciliation must deliver the next published event well inside
	// what the hole crawl (five holes at the 10s grace) would take.
	got := collectUntil(received, 1, 15*time.Second)
	require.NotEmpty(t, got, "the next published event should be delivered after reconciliation")
	for _, id := range got {
		assert.Equal(t, "reconcile-next", id, "only the next published event may be delivered")
	}
}

// TestLeaseRenewedDuringHoleWait proves a live worker's range lease is
// renewed while it waits out a grace period longer than the lease, so a
// competing subscriber cannot reclaim the range underneath it: without
// renewal, the second subscriber would steal the expired range during
// the wait and deliver the same events again.
func TestLeaseRenewedDuringHoleWait(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)
	configMap["broker.postgres.lease_duration"] = "1s"
	configMap["broker.postgres.grace_period"] = "2s"
	configMap["broker.postgres.batch_size"] = "5"
	topic := fmt.Sprintf("lease-renew-topic-%d", time.Now().UnixNano())

	var mu sync.Mutex
	deliveries := make(map[string]int)
	handler := func(_ context.Context, evt *event.Event) error {
		mu.Lock()
		deliveries[evt.ID()]++
		mu.Unlock()
		return nil
	}
	newSub := func() broker.Subscriber {
		sub, err := broker.NewSubscriber(testutil.NewTestLogger(), "lease-renew-sub", testutil.NewTestMetrics(t), configMap)
		require.NoError(t, err)
		return sub
	}
	subA := newSub()
	defer func() { _ = subA.Close() }() // best-effort cleanup; the test is ending
	subB := newSub()
	defer func() { _ = subB.Close() }() // best-effort cleanup; the test is ending
	require.NoError(t, subA.Subscribe(context.Background(), topic, handler))
	require.NoError(t, subB.Subscribe(context.Background(), topic, handler))

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending
	publish := func(i int) {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("lease-renew-%d", i))
		require.NoError(t, evt.SetData(event.ApplicationJSON, map[string]int{"i": i}))
		require.NoError(t, pub.Publish(context.Background(), topic, &evt))
	}
	publish(1)

	// Inject a hole so the range holder waits out a grace period twice as
	// long as its lease, across many poll iterations.
	pool, err := pgxpool.New(context.Background(), sharedPostgresDSN)
	require.NoError(t, err)
	defer pool.Close() // best-effort cleanup; the test is ending
	_, err = pool.Exec(context.Background(),
		`UPDATE broker_topic_seq SET next = next + 1 WHERE topic = $1`, topic)
	require.NoError(t, err)
	for i := 2; i <= 4; i++ {
		publish(i)
	}

	waitFor(15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deliveries) >= 4
	})

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, deliveries, 4, "every event should be delivered past the hole")
	for id, n := range deliveries {
		assert.Equal(t, 1, n, "event %s delivered %d times; a live worker's range was reclaimed", id, n)
	}
}

// TestMetricsLabeledPerBackend proves the shared metrics carry the
// broker_type label for the postgres backend on a real connection.
func TestMetricsLabeledPerBackend(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)
	topic := fmt.Sprintf("metrics-label-topic-%d", time.Now().UnixNano())

	registry := prometheus.NewRegistry()
	metrics := broker.NewMetricsRecorder("label-check", "test", registry)

	received := make(chan string, 1)
	sub, err := broker.NewSubscriber(testutil.NewTestLogger(), "label-sub", metrics, configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }() // best-effort cleanup; the test is ending
	require.NoError(t, sub.Subscribe(context.Background(), topic, func(_ context.Context, evt *event.Event) error {
		received <- evt.ID()
		return nil
	}))

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), metrics, configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending
	evt := event.New()
	evt.SetType("com.example.test.event")
	evt.SetSource("test-source")
	evt.SetID("metrics-label-1")
	require.NoError(t, evt.SetData(event.ApplicationJSON, map[string]int{"i": 1}))
	require.NoError(t, pub.Publish(context.Background(), topic, &evt))

	got := collectUntil(received, 1, 10*time.Second)
	require.Len(t, got, 1, "the event should be delivered")

	for name, want := range map[string]float64{
		"hyperfleet_broker_messages_published_total": 1,
		"hyperfleet_broker_messages_consumed_total":  1,
	} {
		value, ok := counterWithBackendLabel(registry, name)
		require.True(t, ok, "%s should carry broker_type=\"postgres\"", name)
		assert.Equal(t, want, value, "%s with broker_type=\"postgres\"", name)
	}
}

func TestLostRangeOwnershipStopsProgress(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)
	configMap["broker.postgres.batch_size"] = "1"
	topic := fmt.Sprintf("lost-owner-topic-%d", time.Now().UnixNano())
	subID := fmt.Sprintf("lost-owner-sub-%d", time.Now().UnixNano())

	started := make(chan struct{})
	release := make(chan struct{})
	sub, err := broker.NewSubscriber(testutil.NewTestLogger(), subID, testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }() // best-effort cleanup; the test is ending
	require.NoError(t, sub.Subscribe(context.Background(), topic, func(ctx context.Context, _ *event.Event) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending
	evt := event.New()
	evt.SetType("com.example.test.event")
	evt.SetSource("test-source")
	evt.SetID("lost-owner-event")
	require.NoError(t, evt.SetData(event.ApplicationJSON, map[string]string{"id": evt.ID()}))
	require.NoError(t, pub.Publish(context.Background(), topic, &evt))

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not start")
	}

	pool, err := pgxpool.New(context.Background(), sharedPostgresDSN)
	require.NoError(t, err)
	defer pool.Close() // best-effort cleanup; the test is ending
	_, err = pool.Exec(context.Background(), `
		UPDATE broker_ranges SET owner = 'stolen-owner'
		WHERE topic = $1 AND subscription_id = $2`, topic, subID)
	require.NoError(t, err)
	close(release)

	time.Sleep(500 * time.Millisecond)
	var acked int64
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT acked FROM broker_cursors
		WHERE topic = $1 AND subscription_id = $2`, topic, subID).Scan(&acked))
	assert.Zero(t, acked, "a stale worker must not advance the cursor after losing ownership")
}

// counterWithBackendLabel returns the value of the named counter's
// postgres-labeled series, and whether such a series exists.
func counterWithBackendLabel(registry *prometheus.Registry, name string) (float64, bool) {
	families, err := registry.Gather()
	if err != nil {
		return 0, false
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "broker_type" && lp.GetValue() == "postgres" {
					return m.GetCounter().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// TestSkipAfterMaxAttempts proves the max_attempts exhaustion path: a
// persistently failing handler drives its subscription's per-event
// attempt counter to the cap, the skip surfaces on the error channel,
// and the events after the skipped one still deliver.
func TestSkipAfterMaxAttempts(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)
	configMap["broker.postgres.max_attempts"] = "2"
	configMap["broker.postgres.retry_backoff"] = testInterval
	topic := fmt.Sprintf("skip-topic-%d", time.Now().UnixNano())

	received := make(chan string, 8)
	sub, err := broker.NewSubscriber(testutil.NewTestLogger(), "skip-sub", testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }() // best-effort cleanup; the test is ending

	handler := func(_ context.Context, evt *event.Event) error {
		if evt.ID() == "skip-fail" {
			return errors.New("permanent failure")
		}
		received <- evt.ID()
		return nil
	}
	require.NoError(t, sub.Subscribe(context.Background(), topic, handler))

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending
	for _, id := range []string{"skip-fail", "skip-ok-1", "skip-ok-2"} {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(id)
		require.NoError(t, evt.SetData(event.ApplicationJSON, map[string]string{"id": id}))
		require.NoError(t, pub.Publish(context.Background(), topic, &evt))
	}

	got := collectUntil(received, 2, 15*time.Second)
	require.Len(t, got, 2, "the events after the skipped one should still deliver")
	assert.Equal(t, "skip-ok-1", got[0])
	assert.Equal(t, "skip-ok-2", got[1])

	select {
	case subErr := <-sub.Errors():
		assert.Contains(t, subErr.Err.Error(), "event skipped after 2 delivery attempts")
	case <-time.After(5 * time.Second):
		t.Fatal("the skip should surface on the error channel")
	}
}

// TestSkipAfterMaxAttemptsUndecodablePayload drives the same skip
// machinery through the conversion branch: an event whose payload is not
// a CloudEvent is skipped for the subscription after max_attempts, while
// the events around it still deliver.
func TestSkipAfterMaxAttemptsUndecodablePayload(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)
	configMap["broker.postgres.max_attempts"] = "2"
	configMap["broker.postgres.retry_backoff"] = testInterval
	topic := fmt.Sprintf("skip-payload-topic-%d", time.Now().UnixNano())
	subID := fmt.Sprintf("skip-payload-sub-%d", time.Now().UnixNano())

	pub, err := broker.NewPublisher(testutil.NewTestLogger(), testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }() // best-effort cleanup; the test is ending
	for _, id := range []string{"skip-p-1", "skip-p-2", "skip-p-3"} {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(id)
		require.NoError(t, evt.SetData(event.ApplicationJSON, map[string]string{"id": id}))
		require.NoError(t, pub.Publish(context.Background(), topic, &evt))
	}

	// Corrupt the middle event's payload with no consumer running, then
	// rewind the cursor to replay all three. The JSONB column requires
	// valid JSON, so the corruption is a JSON string — valid to store,
	// but not unmarshalable as a CloudEvent.
	pool, err := pgxpool.New(context.Background(), sharedPostgresDSN)
	require.NoError(t, err)
	defer pool.Close() // best-effort cleanup; the test is ending
	_, err = pool.Exec(context.Background(),
		`UPDATE broker_events SET payload = '"not-a-cloudevent"' WHERE topic = $1 AND seq = 2`, topic)
	require.NoError(t, err)

	received := make(chan string, 8)
	sub, err := broker.NewSubscriber(testutil.NewTestLogger(), subID, testutil.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }() // best-effort cleanup; the test is ending
	handler := func(_ context.Context, evt *event.Event) error {
		received <- evt.ID()
		return nil
	}
	require.NoError(t, sub.Subscribe(context.Background(), topic, handler))
	_, err = pool.Exec(context.Background(), `
		UPDATE broker_cursors SET acked = 0, reserved = 0
		WHERE topic = $1 AND subscription_id = $2`, topic, subID)
	require.NoError(t, err)

	got := collectUntil(received, 2, 15*time.Second)
	require.Len(t, got, 2, "the events around the undecodable one should still deliver")
	assert.Equal(t, "skip-p-1", got[0])
	assert.Equal(t, "skip-p-3", got[1])

	select {
	case subErr := <-sub.Errors():
		assert.Contains(t, subErr.Err.Error(), "event skipped after 2 delivery attempts")
	case <-time.After(5 * time.Second):
		t.Fatal("the skip should surface on the error channel")
	}
}
