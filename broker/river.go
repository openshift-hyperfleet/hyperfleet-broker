package broker

// River backend: Postgres as the broker, via riverqueue.com.
//
// POC backend evaluating "the DB is the queue". The broker contract requires
// two behaviors keyed off the subscription ID: same ID = competing consumers,
// different IDs = fanout (every subscription sees every event). River is a job
// queue (each job worked once), so fanout happens at insert time: a small
// subscription registry table records (topic, subscription_id) pairs, and
// Publish inserts one job per registered subscription in a single batch. Each
// (topic, subscription) pair maps to one River queue; replicas sharing a
// subscription ID work the same queue and compete for jobs.
//
// Spec is carried as an encoded CloudEvent in the job args. Handler errors
// return the error to River, which retries with a fixed backoff up to
// max_attempts and then discards. Malformed payloads are cancelled outright,
// they can never succeed.

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

const (
	// DefaultRiverMaxAttempts is the number of times a job is attempted before discard.
	DefaultRiverMaxAttempts = 5
	// DefaultRiverRetryBackoff is the fixed delay between job retries.
	DefaultRiverRetryBackoff = 1 * time.Second
	// DefaultRiverPollInterval is River's fetch poll fallback (LISTEN/NOTIFY is the fast path).
	DefaultRiverPollInterval = 1 * time.Second
	// DefaultRiverFetchCooldown is the minimum time between job fetches.
	DefaultRiverFetchCooldown = 100 * time.Millisecond
	// DefaultRiverPoolMaxConns is the default pgx pool size.
	DefaultRiverPoolMaxConns = 10
	// DefaultRiverSubsCacheTTL is how long Publish caches the subscription registry.
	DefaultRiverSubsCacheTTL = 5 * time.Second
	// riverSchemaTimeout bounds schema creation at construction time.
	riverSchemaTimeout = 30 * time.Second
	// riverHealthTimeout bounds Health when the caller's context has no deadline.
	riverHealthTimeout = 5 * time.Second
	// riverQueueNameMaxLen is River's queue name length limit.
	riverQueueNameMaxLen = 64
)

// riverConfig holds River backend configuration.
type riverConfig struct {
	// DSN is the Postgres connection string. URI form required
	// (postgres://user:pass@host:port/db), keyword/value form is rejected so
	// credential masking in config logging stays correct.
	DSN string `mapstructure:"dsn"`
	// CreateSchemaIfMissing runs River's migrations and creates the
	// subscription registry table at startup.
	CreateSchemaIfMissing bool `mapstructure:"create_schema_if_missing"`
	// MaxAttempts is the number of attempts before a job is discarded.
	MaxAttempts int `mapstructure:"max_attempts"`
	// RetryBackoff is the fixed delay between retries (duration string).
	RetryBackoff string `mapstructure:"retry_backoff"`
	// PollInterval is River's fetch poll fallback interval (duration string).
	PollInterval string `mapstructure:"poll_interval"`
	// FetchCooldown is the minimum time between fetches (duration string).
	FetchCooldown string `mapstructure:"fetch_cooldown"`
	// PoolMaxConns caps the pgx connection pool.
	PoolMaxConns int `mapstructure:"pool_max_conns"`
	// UniqueEvents deduplicates publishes of the same encoded event to the
	// same subscription while the prior job is still active.
	UniqueEvents bool `mapstructure:"unique_events"`
}

func (c *riverConfig) maxAttempts() int {
	if c.MaxAttempts <= 0 {
		return DefaultRiverMaxAttempts
	}
	return c.MaxAttempts
}

func (c *riverConfig) retryBackoff() time.Duration {
	if d, err := time.ParseDuration(c.RetryBackoff); err == nil && d > 0 {
		return d
	}
	return DefaultRiverRetryBackoff
}

func (c *riverConfig) pollInterval() time.Duration {
	if d, err := time.ParseDuration(c.PollInterval); err == nil && d > 0 {
		return d
	}
	return DefaultRiverPollInterval
}

func (c *riverConfig) fetchCooldown() time.Duration {
	if d, err := time.ParseDuration(c.FetchCooldown); err == nil && d > 0 {
		return d
	}
	return DefaultRiverFetchCooldown
}

func (c *riverConfig) poolMaxConns() int32 {
	if c.PoolMaxConns <= 0 {
		return DefaultRiverPoolMaxConns
	}
	return int32(c.PoolMaxConns)
}

// validateRiverConfig validates the river configuration block.
func validateRiverConfig(cfg *config) error {
	rc := cfg.Broker.River
	if rc.DSN == "" {
		return fmt.Errorf("river DSN is required (broker.river.dsn)")
	}
	if !strings.HasPrefix(rc.DSN, "postgres://") && !strings.HasPrefix(rc.DSN, "postgresql://") {
		return fmt.Errorf("river DSN must be in URI form (postgres://...), keyword/value DSNs are not supported")
	}
	if _, err := pgxpool.ParseConfig(rc.DSN); err != nil {
		return fmt.Errorf("invalid river DSN: %w", err)
	}
	if rc.MaxAttempts < 0 {
		return fmt.Errorf("river max_attempts must not be negative")
	}
	if rc.PoolMaxConns < 0 {
		return fmt.Errorf("river pool_max_conns must not be negative")
	}
	for name, val := range map[string]string{
		"retry_backoff":  rc.RetryBackoff,
		"poll_interval":  rc.PollInterval,
		"fetch_cooldown": rc.FetchCooldown,
	} {
		if val == "" {
			continue
		}
		if _, err := time.ParseDuration(val); err != nil {
			return fmt.Errorf("invalid river %s duration %q: %w", name, val, err)
		}
	}
	return nil
}

// riverNameSanitizer matches characters not allowed in River queue names.
var riverNameSanitizer = regexp.MustCompile(`[^a-z0-9_-]+`)

// sanitizeRiverName converts an arbitrary string into a legal River queue name
// fragment (lowercase alphanumerics separated by hyphens or underscores).
func sanitizeRiverName(name string) string {
	s := riverNameSanitizer.ReplaceAllString(strings.ToLower(name), "-")
	s = strings.Trim(s, "-_")
	if s == "" {
		s = "x"
	}
	return s
}

// riverQueueName derives the River queue for a (topic, subscription) pair.
// Names over River's 64-char limit are truncated with an FNV hash suffix so
// distinct pairs cannot collide after truncation.
func riverQueueName(topic, subscriptionID string) string {
	name := sanitizeRiverName(topic) + "-" + sanitizeRiverName(subscriptionID)
	if len(name) <= riverQueueNameMaxLen {
		return name
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	suffix := fmt.Sprintf("-%08x", h.Sum32())
	return name[:riverQueueNameMaxLen-len(suffix)] + suffix
}

// cloudEventArgs is the single River job kind carrying an encoded CloudEvent.
type cloudEventArgs struct {
	// Topic is the broker topic the event was published to.
	Topic string `json:"topic"`
	// Payload is the JSON-encoded CloudEvent (structured content mode).
	Payload []byte `json:"payload"`
}

// Kind implements river.JobArgs.
func (cloudEventArgs) Kind() string { return "cloudevent" }

// riverRetryPolicy retries failed jobs on a fixed interval. The broker
// contract has no ordering guarantees, so exponential backoff buys nothing
// and fixed backoff keeps redelivery behavior predictable.
type riverRetryPolicy struct {
	backoff time.Duration
}

func (p *riverRetryPolicy) NextRetry(*rivertype.JobRow) time.Time {
	return time.Now().Add(p.backoff)
}

// ensureRiverSchema runs River's migrations and creates the subscription
// registry table. Idempotent.
func ensureRiverSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(ctx, riverSchemaTimeout)
	defer cancel()

	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("failed to create river migrator: %w", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("failed to run river migrations: %w", err)
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS broker_subscriptions (
			topic           TEXT NOT NULL,
			subscription_id TEXT NOT NULL,
			last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (topic, subscription_id)
		)`)
	if err != nil {
		return fmt.Errorf("failed to create broker_subscriptions table: %w", err)
	}
	return nil
}

// newRiverPool builds the pgx pool shared by a publisher or subscriber.
func newRiverPool(cfg *config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.Broker.River.DSN)
	if err != nil {
		return nil, fmt.Errorf("invalid river DSN: %w", err)
	}
	poolCfg.MaxConns = cfg.Broker.River.poolMaxConns()

	ctx, cancel := context.WithTimeout(context.Background(), riverSchemaTimeout)
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

// riverHealth pings the pool, self-bounding when the caller has no deadline.
func riverHealth(ctx context.Context, pool *pgxpool.Pool) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, riverHealthTimeout)
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

type riverSubsCache struct {
	ids       []string
	fetchedAt time.Time
}

// riverPublisher publishes CloudEvents by inserting one River job per
// registered subscription of the topic.
type riverPublisher struct {
	pool    *pgxpool.Pool
	insert  *river.Client[pgx.Tx]
	cfg     *config
	logger  logger.Logger
	metrics *MetricsRecorder

	subsCache map[string]riverSubsCache
	subsMu    sync.Mutex
}

// newRiverPublisher creates a Publisher backed by River.
func newRiverPublisher(cfg *config, log logger.Logger, metrics *MetricsRecorder) (Publisher, error) {
	pool, err := newRiverPool(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Broker.River.CreateSchemaIfMissing {
		if err := ensureRiverSchema(context.Background(), pool); err != nil {
			pool.Close()
			return nil, err
		}
	}

	// Insert-only client: no queues, no workers, never started.
	insertClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create river client: %w", err)
	}

	return &riverPublisher{
		pool:      pool,
		insert:    insertClient,
		cfg:       cfg,
		logger:    log,
		metrics:   metrics,
		subsCache: make(map[string]riverSubsCache),
	}, nil
}

// subscriptions returns the registered subscription IDs for a topic, cached
// briefly so hot publish paths do not query the registry per event.
func (p *riverPublisher) subscriptions(ctx context.Context, topic string) ([]string, error) {
	p.subsMu.Lock()
	cached, ok := p.subsCache[topic]
	p.subsMu.Unlock()
	if ok && time.Since(cached.fetchedAt) < DefaultRiverSubsCacheTTL {
		return cached.ids, nil
	}

	rows, err := p.pool.Query(ctx, `SELECT subscription_id FROM broker_subscriptions WHERE topic = $1 ORDER BY subscription_id`, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to query subscriptions for topic %q: %w", topic, err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan subscription row: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read subscription rows: %w", err)
	}

	p.subsMu.Lock()
	p.subsCache[topic] = riverSubsCache{ids: ids, fetchedAt: time.Now()}
	p.subsMu.Unlock()
	return ids, nil
}

// Publish implements Publisher. One River job per registered subscription,
// inserted as a single batch.
func (p *riverPublisher) Publish(ctx context.Context, topic string, evt *event.Event) error {
	p.logger.Infof(ctx, "Publishing event %s to topic %s", evt.ID(), topic)

	payload, err := json.Marshal(evt)
	if err != nil {
		p.logger.Errorf(ctx, "Failed to encode CloudEvent: %v", err)
		p.metrics.RecordError(topic, "conversion")
		return fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	subs, err := p.subscriptions(ctx, topic)
	if err != nil {
		p.logger.Errorf(ctx, "Failed to resolve subscriptions: %v", err)
		p.metrics.RecordError(topic, "publish")
		return err
	}
	if len(subs) == 0 {
		// Matches Pub/Sub semantics: publishing to a topic with no
		// subscriptions succeeds and the event is dropped.
		p.metrics.RecordPublished(topic)
		p.logger.Debugf(ctx, "No subscriptions for topic %s, event %s dropped", topic, evt.ID())
		return nil
	}

	params := make([]river.InsertManyParams, 0, len(subs))
	for _, subID := range subs {
		opts := &river.InsertOpts{
			Queue:       riverQueueName(topic, subID),
			MaxAttempts: p.cfg.Broker.River.maxAttempts(),
		}
		if p.cfg.Broker.River.UniqueEvents {
			opts.UniqueOpts = river.UniqueOpts{ByArgs: true, ByQueue: true}
		}
		params = append(params, river.InsertManyParams{
			Args:       cloudEventArgs{Topic: topic, Payload: payload},
			InsertOpts: opts,
		})
	}

	if _, err := p.insert.InsertMany(ctx, params); err != nil {
		p.logger.Errorf(ctx, "Failed to insert river jobs: %v", err)
		p.metrics.RecordError(topic, "publish")
		return fmt.Errorf("failed to insert river jobs: %w", err)
	}

	p.metrics.RecordPublished(topic)
	p.logger.Debugf(ctx, "Successfully published event %s to topic %s (%d subscriptions)", evt.ID(), topic, len(subs))
	return nil
}

// Health implements Publisher.
func (p *riverPublisher) Health(ctx context.Context) error {
	return riverHealth(ctx, p.pool)
}

// BrokerType implements Publisher.
func (p *riverPublisher) BrokerType() string { return "river" }

// Close implements Publisher.
func (p *riverPublisher) Close() error {
	p.logger.Info(context.Background(), "Closing publisher")
	p.pool.Close()
	return nil
}

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

// riverSubscriber consumes CloudEvent jobs from the queues derived from its
// subscription ID.
type riverSubscriber struct {
	pool           *pgxpool.Pool
	client         *river.Client[pgx.Tx]
	cfg            *config
	subscriptionID string
	parallelism    int
	logger         logger.Logger
	metrics        *MetricsRecorder

	handlers   map[string]HandlerFunc // topic -> handler
	handlersMu sync.RWMutex

	started bool
	startMu sync.Mutex

	errorChan chan *SubscriberError
	closed    bool
	closeMu   sync.RWMutex
}

// riverErrorHandler surfaces discarded jobs on the subscriber error channel.
type riverErrorHandler struct {
	sub *riverSubscriber
}

func (h *riverErrorHandler) HandleError(_ context.Context, job *rivertype.JobRow, err error) *river.ErrorHandlerResult {
	if job.Attempt >= job.MaxAttempts {
		var args cloudEventArgs
		topic := ""
		if jsonErr := json.Unmarshal(job.EncodedArgs, &args); jsonErr == nil {
			topic = args.Topic
		}
		h.sub.sendError(&SubscriberError{
			Op:             "receive",
			Topic:          topic,
			SubscriptionID: h.sub.subscriptionID,
			Err:            fmt.Errorf("job discarded after %d attempts: %w", job.Attempt, err),
			Timestamp:      time.Now(),
			Fatal:          false,
		})
	}
	return nil
}

func (h *riverErrorHandler) HandlePanic(_ context.Context, job *rivertype.JobRow, panicVal any, _ string) *river.ErrorHandlerResult {
	h.sub.sendError(&SubscriberError{
		Op:             "receive",
		SubscriptionID: h.sub.subscriptionID,
		Err:            fmt.Errorf("panic in river worker for job %d: %v", job.ID, panicVal),
		Timestamp:      time.Now(),
		Fatal:          false,
	})
	return nil
}

// cloudEventWorker dispatches River jobs to the subscriber's topic handlers.
type cloudEventWorker struct {
	river.WorkerDefaults[cloudEventArgs]
	sub *riverSubscriber
}

// Work implements river.Worker. Mirrors the Watermill handler ordering:
// consumed metric, decode (permanent failure cancels), timed handler with
// panic recovery, duration metric, error metric on failure.
func (w *cloudEventWorker) Work(ctx context.Context, job *river.Job[cloudEventArgs]) error {
	s := w.sub
	topic := job.Args.Topic

	s.logger.Debugf(ctx, "Received message from topic %s", topic)
	s.metrics.RecordConsumed(topic)

	var evt event.Event
	if err := json.Unmarshal(job.Args.Payload, &evt); err != nil {
		s.logger.Errorf(ctx, "Failed to convert message to CloudEvent: %v", err)
		s.metrics.RecordError(topic, "conversion")
		// A payload that does not decode can never succeed, cancel instead of retrying.
		return river.JobCancel(fmt.Errorf("failed to unmarshal event from JSON: %w", err))
	}

	s.handlersMu.RLock()
	handler, ok := s.handlers[topic]
	s.handlersMu.RUnlock()
	if !ok {
		// A job for a topic this subscriber no longer handles, retry later in
		// case Subscribe has not run yet on this replica.
		return fmt.Errorf("no handler registered for topic %q", topic)
	}

	start := time.Now()
	err := s.callHandler(ctx, handler, &evt)
	s.metrics.RecordDuration(topic, time.Since(start))

	if err != nil {
		s.logger.Errorf(ctx, "Handler failed to process event: %v", err)
		s.metrics.RecordError(topic, "handler")
		return err
	}

	s.logger.Debugf(ctx, "Successfully processed event %s from topic %s subscription %s", evt.ID(), topic, s.subscriptionID)
	return nil
}

// callHandler invokes the handler converting panics to errors, replicating
// the Watermill Recoverer middleware behavior.
func (s *riverSubscriber) callHandler(ctx context.Context, handler HandlerFunc, evt *event.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return handler(ctx, evt)
}

// newRiverSubscriber creates a Subscriber backed by River.
func newRiverSubscriber(cfg *config, log logger.Logger, subscriptionID string, metrics *MetricsRecorder) (Subscriber, error) {
	pool, err := newRiverPool(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Broker.River.CreateSchemaIfMissing {
		if err := ensureRiverSchema(context.Background(), pool); err != nil {
			pool.Close()
			return nil, err
		}
	}

	parallelism := cfg.Subscriber.Parallelism
	if parallelism <= 0 {
		parallelism = DefaultSubscriberParallelism
	}

	s := &riverSubscriber{
		pool:           pool,
		cfg:            cfg,
		subscriptionID: subscriptionID,
		parallelism:    parallelism,
		logger:         log,
		metrics:        metrics,
		handlers:       make(map[string]HandlerFunc),
		errorChan:      make(chan *SubscriberError, ErrorChannelBufferSize),
	}

	return s, nil
}

// ensureClientStarted creates and starts the River client on first use. River
// requires at least one queue in the config at construction time to become a
// working client, and the first queue is only known at Subscribe.
func (s *riverSubscriber) ensureClientStarted(firstQueue string) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()

	if s.started {
		return s.client.Queues().Add(firstQueue, river.QueueConfig{MaxWorkers: s.parallelism})
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &cloudEventWorker{sub: s})

	client, err := river.NewClient(riverpgxv5.New(s.pool), &river.Config{
		Workers: workers,
		Queues: map[string]river.QueueConfig{
			firstQueue: {MaxWorkers: s.parallelism},
		},
		FetchCooldown:     s.cfg.Broker.River.fetchCooldown(),
		FetchPollInterval: s.cfg.Broker.River.pollInterval(),
		MaxAttempts:       s.cfg.Broker.River.maxAttempts(),
		RetryPolicy:       &riverRetryPolicy{backoff: s.cfg.Broker.River.retryBackoff()},
		ErrorHandler:      &riverErrorHandler{sub: s},
		Logger:            slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return fmt.Errorf("failed to create river client: %w", err)
	}

	// The client lifecycle is owned by Close(), not by the Subscribe caller's
	// context, matching the other backends.
	if err := client.Start(context.Background()); err != nil {
		return fmt.Errorf("failed to start river client: %w", err)
	}

	s.client = client
	s.started = true
	return nil
}

// Subscribe implements Subscriber. Registers the subscription in the registry
// so publishers fan out to it, adds the derived queue to the River client,
// and starts the client on first use.
func (s *riverSubscriber) Subscribe(ctx context.Context, topic string, handler HandlerFunc) error {
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

	// Register in the fan-out registry so publishers insert jobs for us.
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO broker_subscriptions (topic, subscription_id)
		VALUES ($1, $2)
		ON CONFLICT (topic, subscription_id) DO UPDATE SET last_seen = NOW()`,
		topic, s.subscriptionID); err != nil {
		return fmt.Errorf("failed to register subscription: %w", err)
	}

	if err := s.ensureClientStarted(riverQueueName(topic, s.subscriptionID)); err != nil {
		return err
	}

	s.logger.Infof(ctx, "Successfully subscribed to topic %s subscription %s", topic, s.subscriptionID)
	return nil
}

// Errors implements Subscriber.
func (s *riverSubscriber) Errors() <-chan *SubscriberError {
	return s.errorChan
}

// BrokerType implements Subscriber.
func (s *riverSubscriber) BrokerType() string { return "river" }

// sendError sends to the error channel without blocking, dropping the oldest
// buffered error when full.
func (s *riverSubscriber) sendError(err *SubscriberError) {
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

// Close implements Subscriber. Stops the River client, waiting for in-flight
// jobs to finish, then releases the pool and closes the error channel.
func (s *riverSubscriber) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	s.logger.Info(context.Background(), "Closing subscriber")

	s.startMu.Lock()
	started := s.started
	s.startMu.Unlock()

	if started {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
		defer cancel()
		if err := s.client.Stop(ctx); err != nil {
			s.logger.Errorf(context.Background(), "River client stop did not complete cleanly: %v", err)
		}
	}

	s.pool.Close()
	close(s.errorChan)

	s.logger.Info(context.Background(), "Successfully closed subscriber")
	return nil
}
