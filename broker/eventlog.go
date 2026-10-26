package broker

// Event log backend: Postgres as the broker, purpose-built.
//
// POC backend evaluating the "append-only event log with per-subscription
// cursors" design from HYPERFLEET-1656. The broker contract requires two
// behaviors keyed off the subscription ID: same ID = competing consumers,
// different IDs = fanout. The event log gives every (topic, subscription)
// pair its own cursor over one shared append-only events table: fanout is
// independent cursor advancement, and competing consumers split work by
// reserving disjoint ranges of the topic's sequence.
//
// Publishing appends exactly one row per event regardless of subscription
// count. Sequences are allocated per topic from a counter table in the
// same transaction as the insert, so a missing sequence below the highest
// delivered sequence is unambiguously an in-flight or rolled-back insert:
// the cursor waits out such holes for a grace period and then skips them
// (contiguity plus grace).
//
// Consumption is range-based. A worker atomically reserves the next
// (reserved, reserved+batch] slice of the topic's sequence and delivers
// its visible events in order, acking only on handler success. A failed
// handler releases the range for any subscriber to retry after a backoff;
// a crashed worker's range lease expires and is reclaimed the same way.
// Delivery is therefore at-least-once within a subscription, safe because
// events are idempotent by generation. Events that keep failing across
// subscribers dead-letter after max_attempts, mirroring River.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
)

const (
	// DefaultEventLogPollInterval is how long a worker waits between polls
	// when no work is available.
	DefaultEventLogPollInterval = 1 * time.Second
	// DefaultEventLogFetchCooldown is the pause after finishing a range, so
	// replicas sharing a subscription interleave instead of one draining
	// every range. Mirrors River's fetch_cooldown governor.
	DefaultEventLogFetchCooldown = 100 * time.Millisecond
	// DefaultEventLogBatchSize is the maximum number of events reserved per
	// range claim.
	DefaultEventLogBatchSize = 10
	// DefaultEventLogGracePeriod is how long a sequence hole is treated as
	// in-flight before it is skipped as a rolled-back insert.
	DefaultEventLogGracePeriod = 5 * time.Second
	// DefaultEventLogLeaseDuration is how long a range claim is held by a
	// live worker before it can be reclaimed after a crash.
	DefaultEventLogLeaseDuration = 30 * time.Second
	// DefaultEventLogMaxAttempts is the number of delivery attempts before
	// an event dead-letters.
	DefaultEventLogMaxAttempts = 5
	// DefaultEventLogRetryBackoff is how long a released range waits before
	// it becomes reclaimable.
	DefaultEventLogRetryBackoff = 200 * time.Millisecond
	// DefaultEventLogRetention bounds how long delivered events are kept;
	// older partitions are dropped.
	DefaultEventLogRetention = 24 * time.Hour
	// DefaultEventLogPoolMaxConns is the default pgx pool size.
	DefaultEventLogPoolMaxConns = 10
	// eventLogSchemaTimeout bounds schema creation at construction time.
	eventLogSchemaTimeout = 30 * time.Second
	// eventLogHealthTimeout bounds Health when the caller's context has no
	// deadline.
	eventLogHealthTimeout = 5 * time.Second
	// eventLogReaperInterval is how often the retention reaper runs.
	eventLogReaperInterval = 1 * time.Minute
	// eventLogPartitionPrefix is the prefix of the daily broker_events
	// partitions.
	eventLogPartitionPrefix = "broker_events_"
)

// eventLogConfig holds event log backend configuration.
type eventLogConfig struct {
	// DSN is the Postgres connection string. URI form required
	// (postgres://user:pass@host:port/db), keyword/value form is rejected
	// so credential masking in config logging stays correct.
	DSN string `mapstructure:"dsn"`
	// CreateSchemaIfMissing creates the tables and daily partitions at
	// startup.
	CreateSchemaIfMissing bool `mapstructure:"create_schema_if_missing"`
	// PollInterval is how long a worker waits between polls when idle
	// (duration string).
	PollInterval string `mapstructure:"poll_interval"`
	// FetchCooldown is the pause after finishing a range (duration string).
	FetchCooldown string `mapstructure:"fetch_cooldown"`
	// BatchSize is the maximum number of events reserved per range claim.
	BatchSize int `mapstructure:"batch_size"`
	// GracePeriod is how long a sequence hole waits before being skipped
	// (duration string).
	GracePeriod string `mapstructure:"grace_period"`
	// LeaseDuration is how long a range claim survives a silent worker
	// (duration string).
	LeaseDuration string `mapstructure:"lease_duration"`
	// MaxAttempts is the number of delivery attempts before an event
	// dead-letters.
	MaxAttempts int `mapstructure:"max_attempts"`
	// RetryBackoff is how long a released range waits before becoming
	// reclaimable (duration string).
	RetryBackoff string `mapstructure:"retry_backoff"`
	// Retention bounds how long events are kept before their partition is
	// dropped (duration string).
	Retention string `mapstructure:"retention"`
	// PoolMaxConns caps the pgx connection pool.
	PoolMaxConns int `mapstructure:"pool_max_conns"`
}

func (c *eventLogConfig) pollInterval() time.Duration {
	if d, err := time.ParseDuration(c.PollInterval); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogPollInterval
}

func (c *eventLogConfig) fetchCooldown() time.Duration {
	if d, err := time.ParseDuration(c.FetchCooldown); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogFetchCooldown
}

func (c *eventLogConfig) batchSize() int64 {
	if c.BatchSize > 0 {
		return int64(c.BatchSize)
	}
	return DefaultEventLogBatchSize
}

func (c *eventLogConfig) gracePeriod() time.Duration {
	if d, err := time.ParseDuration(c.GracePeriod); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogGracePeriod
}

func (c *eventLogConfig) leaseDuration() time.Duration {
	if d, err := time.ParseDuration(c.LeaseDuration); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogLeaseDuration
}

func (c *eventLogConfig) maxAttempts() int {
	if c.MaxAttempts > 0 {
		return c.MaxAttempts
	}
	return DefaultEventLogMaxAttempts
}

func (c *eventLogConfig) retryBackoff() time.Duration {
	if d, err := time.ParseDuration(c.RetryBackoff); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogRetryBackoff
}

func (c *eventLogConfig) retention() time.Duration {
	if d, err := time.ParseDuration(c.Retention); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogRetention
}

func (c *eventLogConfig) poolMaxConns() int32 {
	if c.PoolMaxConns > 0 {
		return int32(c.PoolMaxConns)
	}
	return DefaultEventLogPoolMaxConns
}

// validateEventLogConfig validates the eventlog configuration block.
func validateEventLogConfig(cfg *config) error {
	ec := cfg.Broker.EventLog
	if ec.DSN == "" {
		return fmt.Errorf("eventlog DSN is required (broker.eventlog.dsn)")
	}
	if !strings.HasPrefix(ec.DSN, "postgres://") && !strings.HasPrefix(ec.DSN, "postgresql://") {
		return fmt.Errorf("eventlog DSN must be in URI form (postgres://...), keyword/value DSNs are not supported")
	}
	if _, err := pgxpool.ParseConfig(ec.DSN); err != nil {
		return fmt.Errorf("invalid eventlog DSN: %w", err)
	}
	if ec.BatchSize < 0 {
		return fmt.Errorf("eventlog batch_size must not be negative")
	}
	if ec.MaxAttempts < 0 {
		return fmt.Errorf("eventlog max_attempts must not be negative")
	}
	if ec.PoolMaxConns < 0 {
		return fmt.Errorf("eventlog pool_max_conns must not be negative")
	}
	for name, val := range map[string]string{
		"poll_interval":  ec.PollInterval,
		"fetch_cooldown": ec.FetchCooldown,
		"grace_period":   ec.GracePeriod,
		"lease_duration": ec.LeaseDuration,
		"retry_backoff":  ec.RetryBackoff,
		"retention":      ec.Retention,
	} {
		if val == "" {
			continue
		}
		if _, err := time.ParseDuration(val); err != nil {
			return fmt.Errorf("invalid eventlog %s duration %q: %w", name, val, err)
		}
	}
	return nil
}

// eventLogPartitionName returns the daily partition name for a UTC day.
func eventLogPartitionName(day time.Time) string {
	return eventLogPartitionPrefix + day.UTC().Format("20060102")
}

// ensureEventLogPartitions creates today's and tomorrow's daily partitions
// so inserts never land on a missing partition. Idempotent.
func ensureEventLogPartitions(ctx context.Context, pool *pgxpool.Pool) error {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for _, day := range []time.Time{today, today.AddDate(0, 0, 1)} {
		start := day.Format("2006-01-02")
		end := day.AddDate(0, 0, 1).Format("2006-01-02")
		stmt := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF broker_events FOR VALUES FROM ('%s') TO ('%s')`,
			eventLogPartitionName(day), start, end)
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to create event log partition %s: %w", eventLogPartitionName(day), err)
		}
	}
	return nil
}

// dropExpiredEventLogPartitions drops daily partitions whose range ended
// before the retention cutoff. Retention by rotation: bulk drops, few dead
// tuples.
func dropExpiredEventLogPartitions(ctx context.Context, pool *pgxpool.Pool, retention time.Duration) error {
	cutoff := time.Now().UTC().Add(-retention)

	rows, err := pool.Query(ctx, `
		SELECT c.relname
		FROM pg_class c
		JOIN pg_inherits i ON i.inhrelid = c.oid
		WHERE i.inhparent = 'broker_events'::regclass`)
	if err != nil {
		return fmt.Errorf("failed to list event log partitions: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("failed to scan partition name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read partition names: %w", err)
	}

	for _, name := range names {
		dayStr, ok := strings.CutPrefix(name, eventLogPartitionPrefix)
		if !ok {
			continue
		}
		day, err := time.ParseInLocation("20060102", dayStr, time.UTC)
		if err != nil {
			continue
		}
		if day.AddDate(0, 0, 1).Before(cutoff) {
			if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name)); err != nil {
				return fmt.Errorf("failed to drop event log partition %s: %w", name, err)
			}
		}
	}
	return nil
}

// ensureEventLogSchema creates the event log tables and the first daily
// partitions. Idempotent.
func ensureEventLogSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(ctx, eventLogSchemaTimeout)
	defer cancel()

	statements := []string{
		`CREATE TABLE IF NOT EXISTS broker_subscriptions (
			topic           TEXT NOT NULL,
			subscription_id TEXT NOT NULL,
			last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (topic, subscription_id)
		)`,
		`CREATE TABLE IF NOT EXISTS broker_topic_seq (
			topic TEXT NOT NULL PRIMARY KEY,
			next  BIGINT NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE IF NOT EXISTS broker_events (
			topic      TEXT NOT NULL,
			seq        BIGINT NOT NULL,
			payload    JSONB NOT NULL,
			attempts   INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		) PARTITION BY RANGE (created_at)`,
		`CREATE INDEX IF NOT EXISTS broker_events_topic_seq_idx ON broker_events (topic, seq)`,
		`CREATE TABLE IF NOT EXISTS broker_ranges (
			topic           TEXT NOT NULL,
			subscription_id TEXT NOT NULL,
			range_start     BIGINT NOT NULL,
			range_end       BIGINT NOT NULL,
			state           TEXT NOT NULL DEFAULT 'pending',
			lease_expires   TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (topic, subscription_id, range_start)
		)`,
		`CREATE TABLE IF NOT EXISTS broker_cursors (
			topic           TEXT NOT NULL,
			subscription_id TEXT NOT NULL,
			acked           BIGINT NOT NULL DEFAULT 0,
			reserved        BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (topic, subscription_id)
		)`,
	}

	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to create event log schema: %w", err)
		}
	}
	return ensureEventLogPartitions(ctx, pool)
}

// newEventLogPool builds the pgx pool shared by a publisher or subscriber.
func newEventLogPool(cfg *config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.Broker.EventLog.DSN)
	if err != nil {
		return nil, fmt.Errorf("invalid eventlog DSN: %w", err)
	}
	poolCfg.MaxConns = cfg.Broker.EventLog.poolMaxConns()

	ctx, cancel := context.WithTimeout(context.Background(), eventLogSchemaTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	return pool, nil
}

// eventLogHealth pings the pool, self-bounding when the caller has no
// deadline.
func eventLogHealth(ctx context.Context, pool *pgxpool.Pool) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, eventLogHealthTimeout)
		defer cancel()
	}
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Publisher
// ---------------------------------------------------------------------------

// eventLogPublisher publishes CloudEvents by appending one row per event.
type eventLogPublisher struct {
	pool    *pgxpool.Pool
	cfg     *config
	logger  logger.Logger
	metrics *MetricsRecorder
}

// newEventLogPublisher creates a Publisher backed by the event log.
func newEventLogPublisher(cfg *config, log logger.Logger, metrics *MetricsRecorder) (Publisher, error) {
	pool, err := newEventLogPool(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Broker.EventLog.CreateSchemaIfMissing {
		if err := ensureEventLogSchema(context.Background(), pool); err != nil {
			pool.Close()
			return nil, err
		}
	}

	return &eventLogPublisher{
		pool:    pool,
		cfg:     cfg,
		logger:  log,
		metrics: metrics,
	}, nil
}

// Publish implements Publisher. Exactly one row per event, regardless of
// how many subscriptions will read it: fan-out cost is borne by the
// readers' cursors, not the writer.
func (p *eventLogPublisher) Publish(ctx context.Context, topic string, evt *event.Event) error {
	p.logger.Infof(ctx, "Publishing event %s to topic %s", evt.ID(), topic)

	payload, err := json.Marshal(evt)
	if err != nil {
		p.logger.Errorf(ctx, "Failed to encode CloudEvent: %v", err)
		p.metrics.RecordError(topic, "conversion")
		return fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		p.logger.Errorf(ctx, "Failed to begin publish transaction: %v", err)
		p.metrics.RecordError(topic, "publish")
		return fmt.Errorf("failed to begin publish transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Allocate the topic's next sequence. The counter row is created on
	// first publish; the retry loop covers two first-ever publishes racing.
	var seq int64
	allocated := false
	for i := 0; i < 3; i++ {
		err = tx.QueryRow(ctx,
			`UPDATE broker_topic_seq SET next = next + 1 WHERE topic = $1 RETURNING next - 1`,
			topic).Scan(&seq)
		if errors.Is(err, pgx.ErrNoRows) {
			if _, err = tx.Exec(ctx,
				`INSERT INTO broker_topic_seq (topic, next) VALUES ($1, 1) ON CONFLICT (topic) DO NOTHING`,
				topic); err != nil {
				break
			}
			continue
		}
		allocated = err == nil
		break
	}
	if err != nil || !allocated {
		p.logger.Errorf(ctx, "Failed to allocate event sequence: %v", err)
		p.metrics.RecordError(topic, "publish")
		return fmt.Errorf("failed to allocate sequence for topic %q: %w", topic, err)
	}

	if _, err = tx.Exec(ctx,
		`INSERT INTO broker_events (topic, seq, payload) VALUES ($1, $2, $3)`,
		topic, seq, payload); err != nil {
		p.logger.Errorf(ctx, "Failed to append event: %v", err)
		p.metrics.RecordError(topic, "publish")
		return fmt.Errorf("failed to append event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		p.logger.Errorf(ctx, "Failed to commit publish transaction: %v", err)
		p.metrics.RecordError(topic, "publish")
		return fmt.Errorf("failed to commit publish transaction: %w", err)
	}

	p.metrics.RecordPublished(topic)
	p.logger.Debugf(ctx, "Successfully published event %s to topic %s (seq %d)", evt.ID(), topic, seq)
	return nil
}

// Health implements Publisher.
func (p *eventLogPublisher) Health(ctx context.Context) error {
	return eventLogHealth(ctx, p.pool)
}

// BrokerType implements Publisher.
func (p *eventLogPublisher) BrokerType() string { return "eventlog" }

// Close implements Publisher.
func (p *eventLogPublisher) Close() error {
	p.logger.Info(context.Background(), "Closing publisher")
	p.pool.Close()
	return nil
}

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

// eventRange is a reserved slice of a topic's sequence, (start, end].
type eventRange struct {
	start int64
	end   int64
}

// eventLogSubscriber consumes CloudEvents by advancing its subscription's
// cursor over the shared event log.
type eventLogSubscriber struct {
	pool           *pgxpool.Pool
	cfg            *config
	subscriptionID string
	parallelism    int
	logger         logger.Logger
	metrics        *MetricsRecorder

	handlers   map[string]HandlerFunc // topic -> handler
	handlersMu sync.RWMutex

	startedTopics map[string]bool
	startMu       sync.Mutex

	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup
	reaperDone   chan struct{}

	errorChan chan *SubscriberError
	closed    bool
	closeMu   sync.RWMutex
}

// newEventLogSubscriber creates a Subscriber backed by the event log.
func newEventLogSubscriber(cfg *config, log logger.Logger, subscriptionID string, metrics *MetricsRecorder) (Subscriber, error) {
	pool, err := newEventLogPool(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Broker.EventLog.CreateSchemaIfMissing {
		if err := ensureEventLogSchema(context.Background(), pool); err != nil {
			pool.Close()
			return nil, err
		}
	}

	parallelism := cfg.Subscriber.Parallelism
	if parallelism <= 0 {
		parallelism = DefaultSubscriberParallelism
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())

	return &eventLogSubscriber{
		pool:           pool,
		cfg:            cfg,
		subscriptionID: subscriptionID,
		parallelism:    parallelism,
		logger:         log,
		metrics:        metrics,
		handlers:       make(map[string]HandlerFunc),
		startedTopics:  make(map[string]bool),
		workerCtx:      workerCtx,
		workerCancel:   workerCancel,
		errorChan:      make(chan *SubscriberError, ErrorChannelBufferSize),
	}, nil
}

// Subscribe implements Subscriber. Registers the handler, records the
// subscription, initializes its cursor at the topic's current sequence
// (a new subscription starts from now, matching the other backends), and
// starts the poll workers for the topic.
func (s *eventLogSubscriber) Subscribe(ctx context.Context, topic string, handler HandlerFunc) error {
	if handler == nil {
		return fmt.Errorf("handler must be provided")
	}

	s.closeMu.RLock()
	if s.closed {
		s.closeMu.RUnlock()
		return fmt.Errorf("subscriber is closed")
	}
	s.closeMu.RUnlock()

	s.logger.Infof(ctx, "Starting subscription to topic %s", topic)

	s.handlersMu.Lock()
	if _, exists := s.handlers[topic]; exists {
		s.handlersMu.Unlock()
		return fmt.Errorf("already subscribed to topic %q", topic)
	}
	s.handlers[topic] = handler
	s.handlersMu.Unlock()

	// Register in the subscription registry for parity with the other
	// backends (bookkeeping; fan-out here is cursor-based, not insert-time).
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO broker_subscriptions (topic, subscription_id)
		VALUES ($1, $2)
		ON CONFLICT (topic, subscription_id) DO UPDATE SET last_seen = NOW()`,
		topic, s.subscriptionID); err != nil {
		return fmt.Errorf("failed to register subscription: %w", err)
	}

	// Initialize the cursor at the topic's current sequence. A missing
	// counter row (never-published topic) starts at zero.
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO broker_cursors (topic, subscription_id, acked, reserved)
		SELECT $1, $2, COALESCE(MAX(next - 1), 0), COALESCE(MAX(next - 1), 0)
		FROM broker_topic_seq WHERE topic = $1
		ON CONFLICT (topic, subscription_id) DO NOTHING`,
		topic, s.subscriptionID); err != nil {
		return fmt.Errorf("failed to initialize cursor: %w", err)
	}

	s.startMu.Lock()
	if !s.startedTopics[topic] {
		s.startedTopics[topic] = true
		for i := 0; i < s.parallelism; i++ {
			s.wg.Add(1)
			go s.worker(s.workerCtx, topic)
		}
	}
	if s.reaperDone == nil {
		s.reaperDone = make(chan struct{})
		go s.retentionReaper(s.workerCtx)
	}
	s.startMu.Unlock()

	s.logger.Infof(ctx, "Successfully subscribed to topic %s subscription %s", topic, s.subscriptionID)
	return nil
}

// retentionReaper keeps daily partitions ahead of the inserts and drops
// expired ones.
func (s *eventLogSubscriber) retentionReaper(ctx context.Context) {
	defer close(s.reaperDone)
	ticker := time.NewTicker(eventLogReaperInterval)
	defer ticker.Stop()
	for {
		if err := ensureEventLogPartitions(ctx, s.pool); err != nil {
			s.logger.Errorf(ctx, "Failed to ensure event log partitions: %v", err)
		}
		if err := dropExpiredEventLogPartitions(ctx, s.pool, s.cfg.Broker.EventLog.retention()); err != nil {
			s.logger.Errorf(ctx, "Failed to drop expired event log partitions: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// worker is the per-topic poll loop. It prefers reclaiming an expired
// range (crash or failure recovery) over reserving new work.
func (s *eventLogSubscriber) worker(ctx context.Context, topic string) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		r, ok, err := s.reclaimRange(ctx, topic)
		if err != nil {
			s.sendError(&SubscriberError{
				Op: "receive", Topic: topic, SubscriptionID: s.subscriptionID,
				Err: err, Timestamp: time.Now(), Fatal: false,
			})
			s.sleep(ctx, s.cfg.Broker.EventLog.pollInterval())
			continue
		}
		if ok {
			s.processRange(ctx, topic, r)
			s.sleep(ctx, s.cfg.Broker.EventLog.fetchCooldown())
			continue
		}

		r, ok, err = s.reserveRange(ctx, topic)
		if err != nil {
			s.sendError(&SubscriberError{
				Op: "receive", Topic: topic, SubscriptionID: s.subscriptionID,
				Err: err, Timestamp: time.Now(), Fatal: false,
			})
			s.sleep(ctx, s.cfg.Broker.EventLog.pollInterval())
			continue
		}
		if ok {
			s.processRange(ctx, topic, r)
			s.sleep(ctx, s.cfg.Broker.EventLog.fetchCooldown())
			continue
		}

		s.sleep(ctx, s.cfg.Broker.EventLog.pollInterval())
	}
}

// reclaimRange takes over the oldest expired pending range, if any. SKIP
// LOCKED so concurrent workers never take the same range.
func (s *eventLogSubscriber) reclaimRange(ctx context.Context, topic string) (eventRange, bool, error) {
	var r eventRange
	err := s.pool.QueryRow(ctx, `
		UPDATE broker_ranges
		SET lease_expires = NOW() + make_interval(secs => $3::double precision)
		WHERE ctid IN (
			SELECT ctid FROM broker_ranges
			WHERE topic = $1 AND subscription_id = $2 AND state = 'pending' AND lease_expires < NOW()
			ORDER BY range_start
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING range_start, range_end`,
		topic, s.subscriptionID, s.cfg.Broker.EventLog.leaseDuration().Seconds()).Scan(&r.start, &r.end)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, fmt.Errorf("failed to reclaim range: %w", err)
	}
	return r, true, nil
}

// reserveRange allocates the next unreserved slice of the topic's sequence,
// capped at the highest allocated sequence so workers never reserve events
// that do not exist yet. The reservation runs in one transaction holding
// the cursor row lock from the read through the range insert, so concurrent
// workers always derive disjoint range starts.
func (s *eventLogSubscriber) reserveRange(ctx context.Context, topic string) (eventRange, bool, error) {
	var r eventRange
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return r, false, fmt.Errorf("failed to begin reserve transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the cursor row; the lock is held until commit so every
	// concurrent reservation sees the committed reserved position.
	var reserved int64
	err = tx.QueryRow(ctx, `
		SELECT reserved FROM broker_cursors
		WHERE topic = $1 AND subscription_id = $2
		FOR UPDATE`,
		topic, s.subscriptionID).Scan(&reserved)
	if err != nil {
		return r, false, fmt.Errorf("failed to lock cursor: %w", err)
	}

	var maxSeq int64
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(next) - 1, 0) FROM broker_topic_seq WHERE topic = $1`,
		topic).Scan(&maxSeq)
	if err != nil {
		return r, false, fmt.Errorf("failed to read topic sequence: %w", err)
	}

	r.start = reserved
	r.end = reserved + s.cfg.Broker.EventLog.batchSize()
	if r.end > maxSeq {
		r.end = maxSeq
	}
	if r.end <= r.start {
		return r, false, nil // no unpublished events
	}

	if _, err := tx.Exec(ctx, `
		UPDATE broker_cursors SET reserved = $3
		WHERE topic = $1 AND subscription_id = $2`,
		topic, s.subscriptionID, r.end); err != nil {
		return r, false, fmt.Errorf("failed to advance reserved cursor: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO broker_ranges (topic, subscription_id, range_start, range_end, lease_expires)
		VALUES ($1, $2, $3, $4, NOW() + make_interval(secs => $5::double precision))`,
		topic, s.subscriptionID, r.start, r.end, s.cfg.Broker.EventLog.leaseDuration().Seconds()); err != nil {
		return r, false, fmt.Errorf("failed to insert range claim: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return r, false, fmt.Errorf("failed to commit reserve transaction: %w", err)
	}
	return r, true, nil
}

// releaseRange makes a range reclaimable after the retry backoff,
// redistributing its events to any subscriber.
func (s *eventLogSubscriber) releaseRange(ctx context.Context, topic string, r eventRange) {
	if _, err := s.pool.Exec(ctx, `
		UPDATE broker_ranges
		SET lease_expires = NOW() + make_interval(secs => $3::double precision)
		WHERE topic = $1 AND subscription_id = $2 AND range_start = $4`,
		topic, s.subscriptionID, s.cfg.Broker.EventLog.retryBackoff().Seconds(), r.start); err != nil {
		s.logger.Errorf(ctx, "Failed to release range (%d, %d]: %v", r.start, r.end, err)
	}
}

// completeRange marks a range done and advances the subscription's acked
// cursor to the start of the oldest still-pending range (the contiguous
// done high-water). Done ranges at or below the acked cursor are deleted.
func (s *eventLogSubscriber) completeRange(ctx context.Context, topic string, r eventRange) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin complete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE broker_ranges SET state = 'done'
		WHERE topic = $1 AND subscription_id = $2 AND range_start = $3`,
		topic, s.subscriptionID, r.start); err != nil {
		return fmt.Errorf("failed to mark range done: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE broker_cursors c
		SET acked = GREATEST(c.acked, LEAST(c.reserved, COALESCE((
			SELECT MIN(range_start) FROM broker_ranges r
			WHERE r.topic = c.topic AND r.subscription_id = c.subscription_id AND r.state = 'pending'
		), c.reserved)))
		WHERE c.topic = $1 AND c.subscription_id = $2`,
		topic, s.subscriptionID); err != nil {
		return fmt.Errorf("failed to advance acked cursor: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM broker_ranges
		WHERE topic = $1 AND subscription_id = $2 AND state = 'done'
			AND range_end <= (SELECT acked FROM broker_cursors WHERE topic = $1 AND subscription_id = $2)`,
		topic, s.subscriptionID); err != nil {
		return fmt.Errorf("failed to delete done ranges: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit complete transaction: %w", err)
	}
	return nil
}

// readEvents returns the visible events in (start, end] in sequence order.
func (s *eventLogSubscriber) readEvents(ctx context.Context, topic string, r eventRange) ([]eventRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq, payload FROM broker_events
		WHERE topic = $1 AND seq > $2 AND seq <= $3
		ORDER BY seq`,
		topic, r.start, r.end)
	if err != nil {
		return nil, fmt.Errorf("failed to read events: %w", err)
	}
	defer rows.Close()

	var events []eventRow
	for rows.Next() {
		var row eventRow
		if err := rows.Scan(&row.seq, &row.payload); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %w", err)
		}
		events = append(events, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read event rows: %w", err)
	}
	return events, nil
}

// eventRow is one event log row.
type eventRow struct {
	seq     int64
	payload []byte
}

// processRange delivers a reserved range's events in sequence order. A
// hole below a visible event waits out the grace period before being
// skipped as a rolled-back insert. Returning without completing leaves
// the range pending: its lease expires and another worker reclaims it.
func (s *eventLogSubscriber) processRange(ctx context.Context, topic string, r eventRange) {
	holes := make(map[int64]time.Time)
	delivered := r.start

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rows, err := s.readEvents(ctx, topic, r)
		if err != nil {
			s.logger.Errorf(ctx, "Failed to read range (%d, %d]: %v", r.start, r.end, err)
			return
		}

		for _, row := range rows {
			if row.seq <= delivered {
				continue // already delivered in a previous pass
			}
			if row.seq > delivered+1 {
				// A hole below this event: wait it out to preserve order.
				if _, ok := holes[delivered+1]; !ok {
					holes[delivered+1] = time.Now()
				}
				break
			}
			if !s.deliverEvent(ctx, topic, row) {
				return // range released for retry
			}
			delivered = row.seq
		}

		if delivered >= r.end {
			if err := s.completeRange(ctx, topic, r); err != nil && ctx.Err() == nil {
				s.logger.Errorf(ctx, "Failed to complete range (%d, %d]: %v", r.start, r.end, err)
			}
			return
		}

		// The next sequence is not visible: in flight or rolled back.
		hole := delivered + 1
		first, ok := holes[hole]
		if !ok {
			first = time.Now()
			holes[hole] = first
		}
		if time.Since(first) >= s.cfg.Broker.EventLog.gracePeriod() {
			s.logger.Warnf(ctx, "Event log hole %d on topic %s exceeded grace period, skipping as rolled back", hole, topic)
			delete(holes, hole)
			delivered++
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.Broker.EventLog.pollInterval()):
		}
	}
}

// deliverEvent handles one event: decode, invoke the handler with panic
// recovery, and on failure either dead-letter (attempt cap reached) or
// release the range for redistribution. Returns false when the range was
// released and processing must stop.
func (s *eventLogSubscriber) deliverEvent(ctx context.Context, topic string, row eventRow) bool {
	s.logger.Debugf(ctx, "Received message from topic %s (seq %d)", topic, row.seq)
	s.metrics.RecordConsumed(topic)

	var evt event.Event
	if err := json.Unmarshal(row.payload, &evt); err != nil {
		s.logger.Errorf(ctx, "Failed to convert message to CloudEvent: %v", err)
		s.metrics.RecordError(topic, "conversion")
		// A payload that does not decode can never succeed; it takes the
		// shared attempt counter to the same dead-letter path.
		return s.recordFailure(ctx, topic, row, fmt.Errorf("failed to unmarshal event from JSON: %w", err))
	}

	s.handlersMu.RLock()
	handler, ok := s.handlers[topic]
	s.handlersMu.RUnlock()
	if !ok {
		// An event for a topic this subscriber no longer handles: release
		// for a subscriber that does.
		s.releaseRange(ctx, topic, eventRange{start: row.seq - 1, end: row.seq})
		return false
	}

	start := time.Now()
	err := s.callHandler(ctx, handler, &evt)
	s.metrics.RecordDuration(topic, time.Since(start))

	if err != nil {
		s.logger.Errorf(ctx, "Handler failed to process event: %v", err)
		s.metrics.RecordError(topic, "handler")
		return s.recordFailure(ctx, topic, row, err)
	}

	s.logger.Debugf(ctx, "Successfully processed event %s from topic %s subscription %s", evt.ID(), topic, s.subscriptionID)
	return true
}

// recordFailure increments the event's shared attempt counter. At the cap
// the event dead-letters (skipped, logged, surfaced on the error channel)
// and delivery continues; below the cap the range is released for any
// subscriber to retry after the backoff.
func (s *eventLogSubscriber) recordFailure(ctx context.Context, topic string, row eventRow, cause error) bool {
	var attempts int
	err := s.pool.QueryRow(ctx, `
		UPDATE broker_events SET attempts = attempts + 1
		WHERE topic = $1 AND seq = $2
		RETURNING attempts`,
		topic, row.seq).Scan(&attempts)
	if err != nil {
		s.logger.Errorf(ctx, "Failed to record attempt for seq %d: %v", row.seq, err)
		return false
	}

	if attempts >= s.cfg.Broker.EventLog.maxAttempts() {
		s.logger.Errorf(ctx, "Event seq %d on topic %s dead-lettered after %d attempts: %v", row.seq, topic, attempts, cause)
		s.sendError(&SubscriberError{
			Op:             "receive",
			Topic:          topic,
			SubscriptionID: s.subscriptionID,
			Err:            fmt.Errorf("event dead-lettered after %d attempts: %w", attempts, cause),
			Timestamp:      time.Now(),
			Fatal:          false,
		})
		return true
	}

	s.releaseRange(ctx, topic, eventRange{start: row.seq - 1, end: row.seq})
	return false
}

// callHandler invokes the handler converting panics to errors, replicating
// the Watermill Recoverer middleware behavior.
func (s *eventLogSubscriber) callHandler(ctx context.Context, handler HandlerFunc, evt *event.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return handler(ctx, evt)
}

// sleep waits for d or until the worker context is cancelled.
func (s *eventLogSubscriber) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Errors implements Subscriber.
func (s *eventLogSubscriber) Errors() <-chan *SubscriberError {
	return s.errorChan
}

// BrokerType implements Subscriber.
func (s *eventLogSubscriber) BrokerType() string { return "eventlog" }

// sendError sends to the error channel without blocking, dropping the
// oldest buffered error when full.
func (s *eventLogSubscriber) sendError(err *SubscriberError) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()

	if s.closed {
		return
	}

	select {
	case s.errorChan <- err:
	default:
		s.logger.Errorf(context.Background(),
			"Error channel full, dropping error: topic=%s, subscription_id=%s, buffer_size=%d, error=%v",
			err.Topic, s.subscriptionID, ErrorChannelBufferSize, err.Err)
		select {
		case <-s.errorChan:
			select {
			case s.errorChan <- err:
			default:
			}
		default:
		}
	}
}

// Close implements Subscriber. Cancels the workers, waits for in-flight
// handlers to finish, then releases the pool and closes the error
// channel.
func (s *eventLogSubscriber) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	s.logger.Info(context.Background(), "Closing subscriber")

	// The worker lifecycle is owned by Close(), not by the Subscribe
	// caller's context, matching the other backends.
	s.workerCancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(DefaultShutdownTimeout):
		s.logger.Error(context.Background(), "Timed out waiting for event log workers to stop")
	}

	s.startMu.Lock()
	reaperDone := s.reaperDone
	s.startMu.Unlock()
	if reaperDone != nil {
		select {
		case <-reaperDone:
		case <-time.After(DefaultShutdownTimeout):
		}
	}

	s.pool.Close()
	close(s.errorChan)

	s.logger.Info(context.Background(), "Successfully closed subscriber")
	return nil
}
