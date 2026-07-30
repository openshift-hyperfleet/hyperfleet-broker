// Command loadgen drives load through the River broker backend and reports
// end-to-end delivery latency.
//
// It publishes CloudEvents at a fixed rate to a topic while N subscribers
// (distinct subscription IDs, so every event fans out N ways) record the
// delta between the publish timestamp carried in the event and the moment
// their handler runs. At the end it prints achieved throughput and latency
// percentiles per subscriber and overall.
//
// Example:
//
//	loadgen -dsn postgres://broker:broker@localhost:5432/broker?sslmode=disable \
//	        -rate 200 -duration 30s -subs 3 -parallelism 4
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
)

const publishedAtExtension = "loadgenpublishedat"

type latencyRecorder struct {
	mu        sync.Mutex
	latencies []time.Duration
}

func (r *latencyRecorder) record(d time.Duration) {
	r.mu.Lock()
	r.latencies = append(r.latencies, d)
	r.mu.Unlock()
}

func (r *latencyRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.latencies)
}

func (r *latencyRecorder) percentiles() (p50, p95, p99, max time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.latencies) == 0 {
		return 0, 0, 0, 0
	}
	sorted := make([]time.Duration, len(r.latencies))
	copy(sorted, r.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(q float64) time.Duration {
		idx := int(q * float64(len(sorted)-1))
		return sorted[idx]
	}
	return at(0.50), at(0.95), at(0.99), sorted[len(sorted)-1]
}

func buildConfigMap(dsn string, parallelism int, fetchCooldown time.Duration) map[string]string {
	return map[string]string{
		"broker.type":                           "river",
		"broker.river.dsn":                      dsn,
		"broker.river.create_schema_if_missing": "true",
		"broker.river.fetch_cooldown":           fetchCooldown.String(),
		"subscriber.parallelism":                fmt.Sprintf("%d", parallelism),
	}
}

func main() {
	var (
		mode        = flag.String("mode", "both", "publish, subscribe, or both")
		dsn         = flag.String("dsn", "", "Postgres DSN (postgres://...)")
		topic       = flag.String("topic", "load-topic", "topic to publish/subscribe on")
		rate        = flag.Int("rate", 100, "events per second to publish")
		duration    = flag.Duration("duration", 30*time.Second, "publish duration")
		subs        = flag.Int("subs", 3, "number of fan-out subscriptions")
		parallelism = flag.Int("parallelism", 4, "workers per subscriber")
		drain       = flag.Duration("drain", 30*time.Second, "max wait for deliveries after publishing ends")
		// FetchCooldown is the per-queue throughput governor: each subscriber
		// fetches at most every cooldown, so its ceiling is roughly
		// parallelism jobs per cooldown interval.
		fetchCooldown = flag.Duration("fetch-cooldown", 10*time.Millisecond, "river fetch cooldown per subscriber")
	)
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "-dsn is required")
		os.Exit(1)
	}

	log := logger.NewTestLogger(logger.WithLevel(slog.LevelError))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	configMap := buildConfigMap(*dsn, *parallelism, *fetchCooldown)

	// Subscribers first so the registry rows exist before publishing starts.
	recorders := make([]*latencyRecorder, 0, *subs)
	subscribers := make([]broker.Subscriber, 0, *subs)
	if *mode == "subscribe" || *mode == "both" {
		for i := 0; i < *subs; i++ {
			rec := &latencyRecorder{}
			recorders = append(recorders, rec)

			metrics := broker.NewMetricsRecorder("loadgen-sub", "v0", prometheus.NewRegistry())
			sub, err := broker.NewSubscriber(log, fmt.Sprintf("load-sub-%d", i+1), metrics, configMap)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to create subscriber %d: %v\n", i+1, err)
				os.Exit(1)
			}
			subscribers = append(subscribers, sub)

			handler := func(_ context.Context, evt *event.Event) error {
				if ts, ok := evt.Extensions()[publishedAtExtension].(string); ok {
					if published, err := time.Parse(time.RFC3339Nano, ts); err == nil {
						rec.record(time.Since(published))
						return nil
					}
				}
				rec.record(0)
				return nil
			}
			if err := sub.Subscribe(ctx, *topic, handler); err != nil {
				fmt.Fprintf(os.Stderr, "failed to subscribe %d: %v\n", i+1, err)
				os.Exit(1)
			}
		}
		fmt.Printf("subscribers ready: %d (parallelism %d each) on topic %q\n", *subs, *parallelism, *topic)
	}

	var published, publishErrors int64
	var publishWall time.Duration
	if *mode == "publish" || *mode == "both" {
		metrics := broker.NewMetricsRecorder("loadgen-pub", "v0", prometheus.NewRegistry())
		pub, err := broker.NewPublisher(log, metrics, configMap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create publisher: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = pub.Close() }()

		// Give subscriber registration a moment to land before the clock starts.
		time.Sleep(500 * time.Millisecond)

		interval := time.Second / time.Duration(*rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		deadline := time.After(*duration)
		start := time.Now()

		fmt.Printf("publishing at %d events/sec for %s...\n", *rate, *duration)
	publishLoop:
		for {
			select {
			case <-ctx.Done():
				break publishLoop
			case <-deadline:
				break publishLoop
			case <-ticker.C:
				evt := event.New()
				evt.SetID(uuid.NewString())
				evt.SetType("com.redhat.hyperfleet.loadgen")
				evt.SetSource("loadgen")
				evt.SetExtension(publishedAtExtension, time.Now().Format(time.RFC3339Nano))
				if err := evt.SetData(event.ApplicationJSON, map[string]string{"seq": fmt.Sprintf("%d", published)}); err != nil {
					atomic.AddInt64(&publishErrors, 1)
					continue
				}
				if err := pub.Publish(ctx, *topic, &evt); err != nil {
					atomic.AddInt64(&publishErrors, 1)
					continue
				}
				atomic.AddInt64(&published, 1)
			}
		}
		publishWall = time.Since(start)
		fmt.Printf("published %d events in %s (%.1f/sec achieved), %d errors\n",
			published, publishWall.Round(time.Millisecond),
			float64(published)/publishWall.Seconds(), publishErrors)
	}

	if len(subscribers) > 0 {
		expected := atomic.LoadInt64(&published)
		if *mode == "subscribe" {
			// Standalone subscribe mode has no publish count, just run until signal.
			fmt.Println("subscribe mode: waiting for SIGINT...")
			<-ctx.Done()
		} else {
			drainDeadline := time.After(*drain)
		drainLoop:
			for {
				done := true
				for _, rec := range recorders {
					if int64(rec.count()) < expected {
						done = false
						break
					}
				}
				if done {
					break
				}
				select {
				case <-drainDeadline:
					fmt.Println("drain timeout reached before all deliveries arrived")
					break drainLoop
				case <-ctx.Done():
					break drainLoop
				case <-time.After(200 * time.Millisecond):
				}
			}
		}

		fmt.Println()
		fmt.Println("=== delivery report ===")
		var totalReceived int64
		overall := &latencyRecorder{}
		for i, rec := range recorders {
			p50, p95, p99, maxL := rec.percentiles()
			received := rec.count()
			totalReceived += int64(received)
			rec.mu.Lock()
			overall.latencies = append(overall.latencies, rec.latencies...)
			rec.mu.Unlock()
			fmt.Printf("sub %d: received=%d p50=%s p95=%s p99=%s max=%s\n",
				i+1, received, p50.Round(time.Millisecond), p95.Round(time.Millisecond),
				p99.Round(time.Millisecond), maxL.Round(time.Millisecond))
		}
		if expected > 0 {
			fmt.Printf("fan-out total: %d received / %d expected (%d x %d subs)\n",
				totalReceived, expected*int64(len(recorders)), expected, len(recorders))
		}
		p50, p95, p99, maxL := overall.percentiles()
		fmt.Printf("overall e2e latency: p50=%s p95=%s p99=%s max=%s\n",
			p50.Round(time.Millisecond), p95.Round(time.Millisecond),
			p99.Round(time.Millisecond), maxL.Round(time.Millisecond))

		for _, sub := range subscribers {
			_ = sub.Close()
		}
	}
}
