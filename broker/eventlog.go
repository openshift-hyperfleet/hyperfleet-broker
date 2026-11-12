package broker

// PostgreSQL event-log backend registered as broker type "postgres".
//
// Each (topic, subscription) has an independent cursor over one shared
// append-only event log. Different subscription IDs provide fan-out; the
// same ID lets replicas compete through leased sequence ranges. Delivery is
// at-least-once within a subscription, with retry attempts scoped to
// (topic, subscription, sequence); exhausted events are skipped for that
// subscription and reported through its error channel.
//
// The backend runs against pre-existing schema and invokes the maintenance
// routine for partition rotation; it does not create tables or partitions.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DefaultEventLogPollInterval  = 1 * time.Second
	DefaultEventLogFetchCooldown = 100 * time.Millisecond
	DefaultEventLogBatchSize     = 10
	DefaultEventLogGracePeriod   = 5 * time.Second
	DefaultEventLogLeaseDuration = 30 * time.Second
	DefaultEventLogMaxAttempts   = 5
	DefaultEventLogRetryBackoff  = 200 * time.Millisecond
	DefaultEventLogRetention     = 24 * time.Hour
	DefaultEventLogPoolMaxConns  = 10
	DefaultEventLogPort          = 5432

	eventLogPoolTimeout    = 30 * time.Second
	eventLogHealthTimeout  = 5 * time.Second
	eventLogReaperInterval = 1 * time.Minute
	// eventLogSeqAllocAttempts bounds the retry loop covering two
	// first-ever publishes racing to create a topic's counter row.
	eventLogSeqAllocAttempts = 3
	leaseRenewalDivisor      = 3
	// maxPostgresPort is the upper bound of valid TCP ports.
	maxPostgresPort = 65535
	// Range states, kept in sync with the schema's state column.
	rangeStatePending = "pending"
	rangeStateDone    = "done"
)

var errRangeOwnershipLost = errors.New("event range ownership lost")

// postgresConfig holds the postgres backend configuration: discrete
// connection fields assembled into the DSN in code (following the API's
// pkg/config/db.go pattern), plus the pool and behavior knobs from
// ADR-0031.
type postgresConfig struct {
	Host       string `mapstructure:"host" json:"host"`
	Database   string `mapstructure:"database" json:"database"`
	User       string `mapstructure:"user" json:"user"`
	Password   string `mapstructure:"password" json:"-"`
	CACertFile string `mapstructure:"ca_cert_file" json:"ca_cert_file"`
	// SSLMode has no default: an unset sslmode is a configuration error, so
	// an operator must name the connection's security posture explicitly.
	SSLMode       string `mapstructure:"sslmode" json:"sslmode"`
	PollInterval  string `mapstructure:"poll_interval" json:"poll_interval"`
	FetchCooldown string `mapstructure:"fetch_cooldown" json:"fetch_cooldown"`
	GracePeriod   string `mapstructure:"grace_period" json:"grace_period"`
	LeaseDuration string `mapstructure:"lease_duration" json:"lease_duration"`
	RetryBackoff  string `mapstructure:"retry_backoff" json:"retry_backoff"`
	Retention     string `mapstructure:"retention" json:"retention"`
	Port          int    `mapstructure:"port" json:"port"`
	// PoolMaxConns caps the pgx connection pool.
	PoolMaxConns int `mapstructure:"pool_max_conns" json:"pool_max_conns"`
	// BatchSize is the maximum number of events reserved per range claim.
	BatchSize int `mapstructure:"batch_size" json:"batch_size"`
	// MaxAttempts is the number of delivery attempts, per (topic,
	// subscription, sequence), before an event is skipped.
	MaxAttempts int `mapstructure:"max_attempts" json:"max_attempts"`
}

const redactedValue = "**REDACTED**"

func (c postgresConfig) MarshalJSON() ([]byte, error) {
	type alias postgresConfig
	return json.Marshal(&struct {
		*alias
		Password string `json:"password"`
	}{
		Password: redactIfSet(c.Password),
		alias:    (*alias)(&c),
	})
}

func redactIfSet(value string) string {
	if value == "" {
		return ""
	}
	return redactedValue
}

func (c *postgresConfig) pollInterval() time.Duration {
	if d, err := time.ParseDuration(c.PollInterval); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogPollInterval
}

func (c *postgresConfig) fetchCooldown() time.Duration {
	if d, err := time.ParseDuration(c.FetchCooldown); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogFetchCooldown
}

func (c *postgresConfig) batchSize() int64 {
	if c.BatchSize > 0 {
		return int64(c.BatchSize)
	}
	return DefaultEventLogBatchSize
}

func (c *postgresConfig) gracePeriod() time.Duration {
	if d, err := time.ParseDuration(c.GracePeriod); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogGracePeriod
}

func (c *postgresConfig) leaseDuration() time.Duration {
	if d, err := time.ParseDuration(c.LeaseDuration); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogLeaseDuration
}

func (c *postgresConfig) maxAttempts() int {
	if c.MaxAttempts > 0 {
		return c.MaxAttempts
	}
	return DefaultEventLogMaxAttempts
}

func (c *postgresConfig) retryBackoff() time.Duration {
	if d, err := time.ParseDuration(c.RetryBackoff); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogRetryBackoff
}

func (c *postgresConfig) retention() time.Duration {
	if d, err := time.ParseDuration(c.Retention); err == nil && d > 0 {
		return d
	}
	return DefaultEventLogRetention
}

func (c *postgresConfig) poolMaxConns() int32 {
	if c.PoolMaxConns > 0 {
		return int32(c.PoolMaxConns)
	}
	return DefaultEventLogPoolMaxConns
}

// simpleDSNValuePattern matches PostgreSQL DSN simple values that don't
// need quoting. Per the libpq spec, simple values contain only letters,
// digits, hyphens, and dots.
var simpleDSNValuePattern = regexp.MustCompile(`^[a-zA-Z0-9.-]+$`)

// escapeDSNValue escapes a PostgreSQL DSN parameter value according to
// libpq rules: simple values stay bare, everything else is single-quoted
// so the value cannot run into the next parameter; backslashes and single
// quotes are escaped inside the quotes.
func escapeDSNValue(value string) string {
	if value == "" {
		return "''"
	}
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	if !simpleDSNValuePattern.MatchString(escaped) {
		return fmt.Sprintf("'%s'", escaped)
	}
	return escaped
}

// dsn assembles the keyword/value connection string from the discrete
// connection fields. sslrootcert is only present when a CA bundle is
// configured.
func (c *postgresConfig) dsn() string {
	params := []string{
		fmt.Sprintf("host=%s", escapeDSNValue(c.Host)),
		fmt.Sprintf("port=%d", c.port()),
		fmt.Sprintf("dbname=%s", escapeDSNValue(c.Database)),
		fmt.Sprintf("user=%s", escapeDSNValue(c.User)),
		fmt.Sprintf("password=%s", escapeDSNValue(c.Password)),
		fmt.Sprintf("sslmode=%s", escapeDSNValue(c.SSLMode)),
	}
	if c.CACertFile != "" {
		params = append(params, fmt.Sprintf("sslrootcert=%s", escapeDSNValue(c.CACertFile)))
	}
	return strings.Join(params, " ")
}

func (c *postgresConfig) port() int {
	if c.Port > 0 {
		return c.Port
	}
	return DefaultEventLogPort
}

func validatePostgresConfig(cfg *config) error {
	pc := cfg.Broker.Postgres
	if pc.Host == "" {
		return fmt.Errorf("postgres host is required (broker.postgres.host)")
	}
	if pc.Database == "" {
		return fmt.Errorf("postgres database is required (broker.postgres.database)")
	}
	if pc.User == "" {
		return fmt.Errorf("postgres user is required (broker.postgres.user)")
	}
	if pc.SSLMode == "" {
		return fmt.Errorf("postgres sslmode is required (broker.postgres.sslmode)")
	}
	switch pc.SSLMode {
	case "disable", "require", "verify-ca", "verify-full":
	default:
		return fmt.Errorf("invalid postgres sslmode %q: one of disable, require, verify-ca, verify-full", pc.SSLMode)
	}
	if pc.Port < 0 || pc.Port > maxPostgresPort {
		return fmt.Errorf(
			"postgres port must be between 0 and %d (0 uses the default %d) (broker.postgres.port)",
			maxPostgresPort, DefaultEventLogPort,
		)
	}
	if pc.BatchSize < 0 {
		return fmt.Errorf("postgres batch_size must not be negative (broker.postgres.batch_size)")
	}
	if pc.MaxAttempts < 0 {
		return fmt.Errorf("postgres max_attempts must not be negative (broker.postgres.max_attempts)")
	}
	if pc.PoolMaxConns < 0 {
		return fmt.Errorf("postgres pool_max_conns must not be negative (broker.postgres.pool_max_conns)")
	}
	for name, val := range map[string]string{
		"poll_interval":  pc.PollInterval,
		"fetch_cooldown": pc.FetchCooldown,
		"grace_period":   pc.GracePeriod,
		"lease_duration": pc.LeaseDuration,
		"retry_backoff":  pc.RetryBackoff,
		"retention":      pc.Retention,
	} {
		if val == "" {
			continue
		}
		d, err := time.ParseDuration(val)
		if err != nil || d <= 0 {
			if err == nil {
				err = fmt.Errorf("must be positive")
			}
			return fmt.Errorf("invalid postgres %s duration %q (broker.postgres.%s): %w", name, val, name, err)
		}
	}
	return nil
}

// rollbackTx rolls back an event log transaction unless it already
// completed, logging unexpected failures. Deferred around every event
// log transaction.
func rollbackTx(ctx context.Context, tx pgx.Tx, logger *slog.Logger) {
	if err := tx.Rollback(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		logger.ErrorContext(ctx, "failed to rollback event log transaction", "error", err)
	}
}

func newEventLogPool(cfg *config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.Broker.Postgres.dsn())
	if err != nil {
		return nil, fmt.Errorf("invalid postgres DSN: %w", err)
	}
	poolCfg.MaxConns = cfg.Broker.Postgres.poolMaxConns()

	ctx, cancel := context.WithTimeout(context.Background(), eventLogPoolTimeout)
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

type eventLogPublisher struct {
	pool    *pgxpool.Pool
	cfg     *config
	logger  *slog.Logger
	metrics *MetricsRecorder
}

func newEventLogPublisher(cfg *config, log *slog.Logger, metrics *MetricsRecorder) (Publisher, error) {
	pool, err := newEventLogPool(cfg)
	if err != nil {
		return nil, err
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
	p.logger.InfoContext(ctx, "publishing event", "event_id", evt.ID(), "topic", topic)

	payload, err := json.Marshal(evt)
	if err != nil {
		p.metrics.RecordErrorForBroker(topic, brokerTypePostgres, "conversion")
		return fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		p.metrics.RecordErrorForBroker(topic, brokerTypePostgres, "publish")
		return fmt.Errorf("failed to begin publish transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, p.logger)

	seq, err := allocateEventSequence(ctx, tx, topic)
	if err != nil {
		p.metrics.RecordErrorForBroker(topic, brokerTypePostgres, "publish")
		return fmt.Errorf("failed to allocate sequence for topic %q: %w", topic, err)
	}

	if _, err = tx.Exec(ctx,
		`INSERT INTO broker_events (topic, seq, payload) VALUES ($1, $2, $3)`,
		topic, seq, payload); err != nil {
		p.metrics.RecordErrorForBroker(topic, brokerTypePostgres, "publish")
		return fmt.Errorf("failed to append event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		p.metrics.RecordErrorForBroker(topic, brokerTypePostgres, "publish")
		return fmt.Errorf("failed to commit publish transaction: %w", err)
	}

	p.metrics.RecordPublishedForBroker(topic, brokerTypePostgres)
	p.logger.DebugContext(ctx, "successfully published event", "event_id", evt.ID(), "topic", topic, "seq", seq)
	return nil
}

// allocateEventSequence allocates the topic's next sequence inside the
// publish transaction. The counter row is created on first publish; the
// retry loop covers two first-ever publishes racing.
func allocateEventSequence(ctx context.Context, tx pgx.Tx, topic string) (int64, error) {
	for i := 0; i < eventLogSeqAllocAttempts; i++ {
		var seq int64
		err := tx.QueryRow(ctx,
			`UPDATE broker_topic_seq SET next = next + 1 WHERE topic = $1 RETURNING next - 1`,
			topic).Scan(&seq)
		if errors.Is(err, pgx.ErrNoRows) {
			if _, err = tx.Exec(ctx,
				`INSERT INTO broker_topic_seq (topic, next) VALUES ($1, 1) ON CONFLICT (topic) DO NOTHING`,
				topic); err != nil {
				return 0, err
			}
			continue
		}
		if err != nil {
			return 0, err
		}
		return seq, nil
	}
	return 0, errors.New("sequence allocation did not converge")
}

func (p *eventLogPublisher) Health(ctx context.Context) error {
	return eventLogHealth(ctx, p.pool)
}

func (p *eventLogPublisher) BrokerType() string { return brokerTypePostgres }

func (p *eventLogPublisher) Close() error {
	p.logger.InfoContext(context.Background(), "closing publisher")
	p.pool.Close()
	return nil
}

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

type eventRange struct {
	owner     string
	start     int64
	end       int64
	delivered int64
}

func newRangeOwner() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate range owner token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

type eventLogSubscriber struct {
	pool          *pgxpool.Pool
	cfg           *config
	logger        *slog.Logger
	metrics       *MetricsRecorder
	handlers      map[string]HandlerFunc // topic -> handler
	startedTopics map[string]bool
	workerCtx     context.Context
	workerCancel  context.CancelFunc
	reaperDone    chan struct{}
	errorChan     chan *SubscriberError
	// subscriptionID trails the pointer cluster: a string is a pointer
	// plus a length, and the length word belongs with the non-pointer tail.
	subscriptionID string
	handlersMu     sync.RWMutex
	startMu        sync.Mutex
	closeMu        sync.RWMutex
	wg             sync.WaitGroup
	parallelism    int
	closed         bool
}

func newEventLogSubscriber(
	cfg *config, log *slog.Logger, subscriptionID string, metrics *MetricsRecorder,
) (Subscriber, error) {
	pool, err := newEventLogPool(cfg)
	if err != nil {
		return nil, err
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

	s.logger.InfoContext(ctx, "starting subscription", "topic", topic, "subscription_id", s.subscriptionID)

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

	// Register the handler only after both writes succeed, so a failed
	// Subscribe can be retried instead of being rejected forever with
	// "already subscribed".
	s.handlersMu.Lock()
	if _, exists := s.handlers[topic]; exists {
		s.handlersMu.Unlock()
		return fmt.Errorf("already subscribed to topic %q", topic)
	}
	s.handlers[topic] = handler
	s.handlersMu.Unlock()

	if err := s.ensureTopicStarted(topic); err != nil {
		return err
	}

	s.logger.InfoContext(ctx, "successfully subscribed to topic", "topic", topic, "subscription_id", s.subscriptionID)
	return nil
}

func (s *eventLogSubscriber) ensureTopicStarted(topic string) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()

	s.closeMu.RLock()
	closed := s.closed
	s.closeMu.RUnlock()
	if closed {
		return fmt.Errorf("subscriber is closed")
	}
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
	return nil
}

func (s *eventLogSubscriber) retentionReaper(ctx context.Context) {
	defer close(s.reaperDone)
	ticker := time.NewTicker(eventLogReaperInterval)
	defer ticker.Stop()
	for {
		if err := s.runMaintenance(ctx); err != nil {
			s.logger.ErrorContext(ctx, "failed to run event log maintenance", "error", err)
		}
		if err := s.reconcileCursors(ctx); err != nil {
			s.logger.ErrorContext(ctx, "failed to reconcile event log cursors", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *eventLogSubscriber) runMaintenance(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `SELECT broker_maintenance($1)`,
		s.cfg.Broker.Postgres.retention().Seconds()); err != nil {
		return fmt.Errorf("failed to invoke event log maintenance routine: %w", err)
	}
	return nil
}

// reconcileCursors advances cursor positions that fell below the
// retention boundary and sweeps attempts rows whose events are no longer
// retained.
func (s *eventLogSubscriber) reconcileCursors(ctx context.Context) error {
	// Topics that still hold rows: advance positions below the oldest
	// retained sequence to one below it.
	if _, err := s.pool.Exec(ctx, `
		UPDATE broker_cursors c
		SET acked = GREATEST(c.acked, o.oldest - 1),
		    reserved = GREATEST(c.reserved, o.oldest - 1)
		FROM broker_cursors c2
		CROSS JOIN LATERAL (
			SELECT e.seq AS oldest
			FROM broker_events e
			WHERE e.topic = c2.topic
			ORDER BY e.seq
			LIMIT 1
		) o
		WHERE c.topic = c2.topic
		  AND c.subscription_id = c2.subscription_id
		  AND (c.acked < o.oldest - 1 OR c.reserved < o.oldest - 1)`); err != nil {
		return fmt.Errorf("failed to advance cursors past the retention boundary: %w", err)
	}

	// Attempts rows whose events were dropped with their partition.
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM broker_event_attempts a
		WHERE NOT EXISTS (
			SELECT 1 FROM broker_events e
			WHERE e.topic = a.topic AND e.seq = a.seq)`); err != nil {
		return fmt.Errorf("failed to sweep event log attempts rows: %w", err)
	}

	// Topics with no retained rows at all: reconcile under the counter-row
	// lock (the same lock publishing takes), held across the empty check,
	// sequence-head read, and cursor updates.
	rows, err := s.pool.Query(ctx, `
		SELECT t.topic FROM broker_topic_seq t
		WHERE NOT EXISTS (SELECT 1 FROM broker_events e WHERE e.topic = t.topic)`)
	if err != nil {
		return fmt.Errorf("failed to list empty event log topics: %w", err)
	}
	defer rows.Close()

	var topics []string
	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic); err != nil {
			return fmt.Errorf("failed to scan empty event log topic: %w", err)
		}
		topics = append(topics, topic)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read empty event log topics: %w", err)
	}

	// Empty topics reconcile one transaction each: a single set-based
	// transaction would hold every topic's counter-row lock at once,
	// contending with publishers across the whole topic set.
	for _, topic := range topics {
		if err := s.reconcileEmptyTopic(ctx, topic); err != nil {
			return err
		}
	}
	return nil
}

// reconcileEmptyTopic advances an empty topic's cursors to its sequence
// head under the counter-row lock, so the next published event is the
// first delivered.
func (s *eventLogSubscriber) reconcileEmptyTopic(ctx context.Context, topic string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin empty-topic reconciliation: %w", err)
	}
	defer rollbackTx(ctx, tx, s.logger)

	var head int64
	err = tx.QueryRow(ctx, `
		SELECT next - 1 FROM broker_topic_seq WHERE topic = $1 FOR UPDATE`,
		topic).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // never-published topic: positions are already at zero
	}
	if err != nil {
		return fmt.Errorf("failed to lock topic sequence counter: %w", err)
	}

	// Re-check emptiness under the lock.
	var hasRows bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM broker_events WHERE topic = $1)`,
		topic).Scan(&hasRows); err != nil {
		return fmt.Errorf("failed to check event log topic rows: %w", err)
	}
	if hasRows {
		return nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE broker_cursors
		SET acked = GREATEST(acked, $2), reserved = GREATEST(reserved, $2)
		WHERE topic = $1 AND (acked < $2 OR reserved < $2)`,
		topic, head); err != nil {
		return fmt.Errorf("failed to advance cursors to the sequence head: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit empty-topic reconciliation: %w", err)
	}
	return nil
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
			s.sendError(ctx, &SubscriberError{
				Op: "receive", Topic: topic, SubscriptionID: s.subscriptionID,
				Err: err, Timestamp: time.Now(), Fatal: false,
			})
			s.sleep(ctx, s.cfg.Broker.Postgres.pollInterval())
			continue
		}
		if ok {
			s.processRange(ctx, topic, r)
			s.sleep(ctx, s.cfg.Broker.Postgres.fetchCooldown())
			continue
		}

		r, ok, err = s.reserveRange(ctx, topic)
		if err != nil {
			s.sendError(ctx, &SubscriberError{
				Op: "receive", Topic: topic, SubscriptionID: s.subscriptionID,
				Err: err, Timestamp: time.Now(), Fatal: false,
			})
			s.sleep(ctx, s.cfg.Broker.Postgres.pollInterval())
			continue
		}
		if ok {
			s.processRange(ctx, topic, r)
			s.sleep(ctx, s.cfg.Broker.Postgres.fetchCooldown())
			continue
		}

		s.sleep(ctx, s.cfg.Broker.Postgres.pollInterval())
	}
}

// reclaimRange takes over the oldest expired pending range, if any. SKIP
// LOCKED so concurrent workers never take the same range.
func (s *eventLogSubscriber) reclaimRange(ctx context.Context, topic string) (eventRange, bool, error) {
	var r eventRange
	owner, err := newRangeOwner()
	if err != nil {
		return r, false, err
	}
	err = s.pool.QueryRow(ctx, `
		UPDATE broker_ranges
		SET lease_expires = NOW() + make_interval(secs => $3::double precision), owner = $4
		WHERE ctid IN (
			SELECT ctid FROM broker_ranges
			WHERE topic = $1 AND subscription_id = $2 AND state = $5 AND lease_expires < NOW()
			ORDER BY range_start
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING range_start, range_end, delivered, owner`,
		topic, s.subscriptionID, s.cfg.Broker.Postgres.leaseDuration().Seconds(), owner, rangeStatePending).
		Scan(&r.start, &r.end, &r.delivered, &r.owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, fmt.Errorf("failed to reclaim range: %w", err)
	}
	return r, true, nil
}

// reserveRange allocates the next unreserved slice of the topic's
// sequence, capped at the allocated head. The idle check takes no locks;
// the reservation transaction holds the cursor row lock so concurrent
// workers derive disjoint ranges.
func (s *eventLogSubscriber) reserveRange(ctx context.Context, topic string) (eventRange, bool, error) {
	var r eventRange

	// Idle check: no locks, one round trip.
	var reserved, maxSeq int64
	err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT reserved FROM broker_cursors WHERE topic = $1 AND subscription_id = $2),
			(SELECT COALESCE(MAX(next) - 1, 0) FROM broker_topic_seq WHERE topic = $1)`,
		topic, s.subscriptionID).Scan(&reserved, &maxSeq)
	if err != nil {
		return r, false, fmt.Errorf("failed to read cursor and topic sequence: %w", err)
	}
	if reserved >= maxSeq {
		return r, false, nil // no unpublished events
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return r, false, fmt.Errorf("failed to begin reserve transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, s.logger)

	// Lock the cursor row until commit.
	err = tx.QueryRow(ctx, `
		SELECT reserved FROM broker_cursors
		WHERE topic = $1 AND subscription_id = $2
		FOR UPDATE`,
		topic, s.subscriptionID).Scan(&reserved)
	if err != nil {
		return r, false, fmt.Errorf("failed to lock cursor: %w", err)
	}

	// Re-read the head under the lock.
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(next) - 1, 0) FROM broker_topic_seq WHERE topic = $1`,
		topic).Scan(&maxSeq)
	if err != nil {
		return r, false, fmt.Errorf("failed to read topic sequence: %w", err)
	}

	r.start = reserved
	r.end = reserved + s.cfg.Broker.Postgres.batchSize()
	r.delivered = r.start
	r.owner, err = newRangeOwner()
	if err != nil {
		return r, false, err
	}
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
		INSERT INTO broker_ranges (topic, subscription_id, range_start, range_end, delivered, owner, lease_expires)
		VALUES ($1, $2, $3, $4, $5, $6, NOW() + make_interval(secs => $7::double precision))`,
		topic, s.subscriptionID, r.start, r.end, r.delivered, r.owner,
		s.cfg.Broker.Postgres.leaseDuration().Seconds()); err != nil {
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
		WHERE topic = $1 AND subscription_id = $2 AND range_start = $4 AND owner = $5`,
		topic, s.subscriptionID, s.cfg.Broker.Postgres.retryBackoff().Seconds(), r.start, r.owner); err != nil {
		s.logger.ErrorContext(ctx, "failed to release range",
			"error", err, "topic", topic, "range_start", r.start, "range_end", r.end)
		// The range remains leased and will be reclaimed after lease expiry.
	}
}

// completeRange marks a range done, deletes its attempts rows, advances
// the acked cursor to the contiguous done high-water, and deletes done
// ranges below it.
func (s *eventLogSubscriber) completeRange(ctx context.Context, topic string, r eventRange) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin complete transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, s.logger)

	result, err := tx.Exec(ctx, `
		UPDATE broker_ranges SET state = $3
		WHERE topic = $1 AND subscription_id = $2 AND range_start = $4 AND owner = $5`,
		topic, s.subscriptionID, rangeStateDone, r.start, r.owner)
	if err != nil {
		return fmt.Errorf("failed to mark range done: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errRangeOwnershipLost
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM broker_event_attempts
		WHERE topic = $1 AND subscription_id = $2 AND seq > $3 AND seq <= $4`,
		topic, s.subscriptionID, r.start, r.end); err != nil {
		return fmt.Errorf("failed to delete attempts rows: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE broker_cursors c
		SET acked = GREATEST(c.acked, LEAST(c.reserved, COALESCE((
			SELECT MIN(range_start) FROM broker_ranges r
			WHERE r.topic = c.topic AND r.subscription_id = c.subscription_id AND r.state = $3
		), c.reserved)))
		WHERE c.topic = $1 AND c.subscription_id = $2`,
		topic, s.subscriptionID, rangeStatePending); err != nil {
		return fmt.Errorf("failed to advance acked cursor: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM broker_ranges
		WHERE topic = $1 AND subscription_id = $2 AND state = $3
			AND range_end <= (SELECT acked FROM broker_cursors WHERE topic = $1 AND subscription_id = $2)`,
		topic, s.subscriptionID, rangeStateDone); err != nil {
		return fmt.Errorf("failed to delete done ranges: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit complete transaction: %w", err)
	}
	return nil
}

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

	events := make([]eventRow, 0, int(r.end-r.start))
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

type eventRow struct {
	payload []byte
	seq     int64
}

// processRange delivers a reserved range's events in order, waiting out
// holes for the grace period before skipping them as rolled-back
// inserts. Returning without completing leaves the range pending for
// lease-expiry reclaim. Handler calls and ack writes run on a drain
// context that outlives worker cancellation, so an in-flight range
// commits while Close() drains, bounded by the shutdown timeout.
func (s *eventLogSubscriber) processRange(ctx context.Context, topic string, r eventRange) {
	holes := make(map[int64]time.Time)
	delivered := r.delivered

	drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), DefaultShutdownTimeout)
	defer cancelDrain()

	pollTimer := time.NewTimer(s.cfg.Broker.Postgres.pollInterval())
	defer pollTimer.Stop()

	// The claim just set a fresh lease; renewal is only needed once the
	// handling outlasts a third of the lease period.
	lastLeaseRenew := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Renew the lease while this worker still holds the range, so
		// handling that outlasts lease_duration is not reclaimed
		// underneath a live worker.
		if time.Since(lastLeaseRenew) >= s.cfg.Broker.Postgres.leaseDuration()/leaseRenewalDivisor {
			if !s.renewRangeLease(ctx, topic, r) {
				return
			}
			lastLeaseRenew = time.Now()
		}

		// A range the cursor has reconciled past is done: complete it
		// rather than crawl through dropped events.
		if s.rangeReconciledPast(ctx, topic, r) {
			if err := s.completeRange(drainCtx, topic, r); err != nil && ctx.Err() == nil {
				s.logger.ErrorContext(ctx, "failed to complete reconciled range",
					"error", err, "topic", topic, "range_start", r.start, "range_end", r.end)
			}
			return
		}

		rows, err := s.readEvents(ctx, topic, r)
		if err != nil {
			s.logger.ErrorContext(ctx, "failed to read range",
				"error", err, "topic", topic, "range_start", r.start, "range_end", r.end)
			return
		}

		var continueRange bool
		delivered, continueRange = s.deliverRangeEvents(ctx, drainCtx, topic, r, rows, delivered, holes)
		if !continueRange {
			return
		}

		if delivered >= r.end {
			if err := s.completeRange(drainCtx, topic, r); err != nil && ctx.Err() == nil {
				s.logger.ErrorContext(ctx, "failed to complete range",
					"error", err, "topic", topic, "range_start", r.start, "range_end", r.end)
			}
			return
		}

		// The next sequence is not visible: in flight, rolled back, or
		// dropped with its partition.
		hole := delivered + 1
		first, ok := holes[hole]
		if !ok {
			first = time.Now()
			holes[hole] = first
		}
		if time.Since(first) >= s.cfg.Broker.Postgres.gracePeriod() {
			s.logger.WarnContext(ctx, "event log hole exceeded grace period, skipping as rolled back",
				"topic", topic, "seq", hole, "grace_period", s.cfg.Broker.Postgres.gracePeriod().String())
			delete(holes, hole)
			delivered++
			continue
		}

		if !pollTimer.Stop() {
			select {
			case <-pollTimer.C:
			default:
			}
		}
		pollTimer.Reset(s.cfg.Broker.Postgres.pollInterval())
		select {
		case <-ctx.Done():
			return
		case <-pollTimer.C:
		}
	}
}

func (s *eventLogSubscriber) deliverRangeEvents(
	workerCtx, handlerCtx context.Context,
	topic string,
	r eventRange,
	rows []eventRow,
	delivered int64,
	holes map[int64]time.Time,
) (int64, bool) {
	for _, row := range rows {
		if row.seq <= delivered {
			continue
		}
		if row.seq > delivered+1 {
			if _, ok := holes[delivered+1]; !ok {
				holes[delivered+1] = time.Now()
			}
			break
		}
		select {
		case <-workerCtx.Done():
			return delivered, false
		default:
		}
		if !s.deliverEvent(handlerCtx, topic, r, row) {
			return delivered, false
		}
		delivered = row.seq
	}
	return delivered, true
}

func (s *eventLogSubscriber) renewRangeLease(ctx context.Context, topic string, r eventRange) bool {
	result, err := s.pool.Exec(ctx, `
		UPDATE broker_ranges
		SET lease_expires = NOW() + make_interval(secs => $3::double precision)
		WHERE topic = $1 AND subscription_id = $2 AND range_start = $4 AND owner = $5 AND state = $6`,
		topic, s.subscriptionID, s.cfg.Broker.Postgres.leaseDuration().Seconds(), r.start, r.owner, rangeStatePending)
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to renew range lease",
			"error", err, "topic", topic, "range_start", r.start)
		// A missed renewal is bounded by lease-expiry reclaim.
		return false
	}
	return result.RowsAffected() == 1
}

func (s *eventLogSubscriber) rangeReconciledPast(ctx context.Context, topic string, r eventRange) bool {
	var acked int64
	err := s.pool.QueryRow(ctx, `
		SELECT acked FROM broker_cursors
		WHERE topic = $1 AND subscription_id = $2`,
		topic, s.subscriptionID).Scan(&acked)
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to read acked cursor", "error", err, "topic", topic)
		return false
	}
	return acked > r.start
}

// deliverEvent decodes and handles one event. On failure it either skips
// the event (attempt cap) or releases the range; it returns false when
// the range was released and processing must stop.
func (s *eventLogSubscriber) deliverEvent(ctx context.Context, topic string, r eventRange, row eventRow) bool {
	s.logger.DebugContext(ctx, "received message", "topic", topic, "seq", row.seq)
	s.metrics.RecordConsumedForBroker(topic, brokerTypePostgres)

	var evt event.Event
	if err := json.Unmarshal(row.payload, &evt); err != nil {
		s.logger.ErrorContext(ctx, "failed to convert message to CloudEvent", "error", err)
		s.metrics.RecordErrorForBroker(topic, brokerTypePostgres, "conversion")
		// A payload that does not decode can never succeed; it takes the
		// attempt counter to the same skip path.
		return s.recordFailure(ctx, topic, r, row, fmt.Errorf("failed to unmarshal event from JSON: %w", err))
	}

	s.handlersMu.RLock()
	handler, ok := s.handlers[topic]
	s.handlersMu.RUnlock()
	if !ok {
		// An event for a topic this subscriber no longer handles: release
		// for a subscriber that does.
		s.releaseRange(ctx, topic, r)
		return false
	}

	start := time.Now()
	handlerCtx, cancelHandler := context.WithCancel(ctx)
	renewDone := make(chan struct{})
	renewLost := make(chan struct{}, 1)
	go func() {
		renewInterval := s.cfg.Broker.Postgres.leaseDuration() / leaseRenewalDivisor
		if renewInterval <= 0 {
			renewInterval = time.Millisecond
		}
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		defer close(renewDone)
		for {
			select {
			case <-handlerCtx.Done():
				return
			case <-ticker.C:
				if !s.renewRangeLease(handlerCtx, topic, r) {
					select {
					case renewLost <- struct{}{}:
					default:
					}
					cancelHandler()
					return
				}
			}
		}
	}()
	err := s.callHandler(handlerCtx, handler, &evt)
	cancelHandler()
	<-renewDone
	s.metrics.RecordDurationForBroker(topic, brokerTypePostgres, time.Since(start))
	select {
	case <-renewLost:
		return false
	default:
	}

	if err != nil {
		s.logger.ErrorContext(ctx, "handler failed to process event", "error", err)
		s.metrics.RecordErrorForBroker(topic, brokerTypePostgres, "handler")
		return s.recordFailure(ctx, topic, r, row, err)
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE broker_ranges
		SET delivered = $5
		WHERE topic = $1 AND subscription_id = $2 AND range_start = $3 AND state = $4 AND owner = $6`,
		topic, s.subscriptionID, r.start, rangeStatePending, row.seq, r.owner)
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to persist range progress", "error", err,
			"topic", topic, "range_start", r.start, "seq", row.seq)
		s.releaseRange(ctx, topic, r)
		return false
	}
	if result.RowsAffected() != 1 {
		s.logger.WarnContext(ctx, "range ownership lost while persisting progress",
			"topic", topic, "range_start", r.start, "seq", row.seq)
		return false
	}

	s.logger.DebugContext(ctx, "successfully processed event",
		"event_id", evt.ID(), "topic", topic, "subscription_id", s.subscriptionID)
	return true
}

// recordFailure counts the failed delivery per (topic, subscription,
// sequence). At the cap the event is skipped for this subscription and
// delivery continues; below it the range is released for retry after the
// backoff.
func (s *eventLogSubscriber) recordFailure(
	ctx context.Context, topic string, r eventRange, row eventRow, cause error,
) bool {
	var attempts int
	err := s.pool.QueryRow(ctx, `
		INSERT INTO broker_event_attempts (topic, subscription_id, seq, attempts)
		VALUES ($1, $2, $3, 1)
		ON CONFLICT (topic, subscription_id, seq)
		DO UPDATE SET attempts = broker_event_attempts.attempts + 1
		RETURNING attempts`,
		topic, s.subscriptionID, row.seq).Scan(&attempts)
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to record attempt", "error", err, "topic", topic, "seq", row.seq)
		// Recording failed: release for retry rather than hold the lease
		// to expiry, accepting this attempt was not counted.
		s.releaseRange(ctx, topic, r)
		return false
	}

	if attempts >= s.cfg.Broker.Postgres.maxAttempts() {
		s.logger.ErrorContext(ctx, "event skipped after exhausting delivery attempts",
			"error", cause, "topic", topic, "seq", row.seq,
			"attempts", attempts, "subscription_id", s.subscriptionID)
		s.sendError(ctx, &SubscriberError{
			Op:             "receive",
			Topic:          topic,
			SubscriptionID: s.subscriptionID,
			Err:            fmt.Errorf("event skipped after %d delivery attempts: %w", attempts, cause),
			Timestamp:      time.Now(),
			Fatal:          false,
		})
		return true
	}

	s.releaseRange(ctx, topic, r)
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

// sleep waits for d or until the worker context is canceled.
func (s *eventLogSubscriber) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (s *eventLogSubscriber) Errors() <-chan *SubscriberError {
	return s.errorChan
}

func (s *eventLogSubscriber) BrokerType() string { return brokerTypePostgres }

func (s *eventLogSubscriber) sendError(ctx context.Context, err *SubscriberError) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()

	if s.closed {
		return
	}

	select {
	case s.errorChan <- err:
	default:
		s.logger.ErrorContext(ctx, "error channel full, dropping oldest buffered error",
			"topic", err.Topic, "subscription_id", s.subscriptionID,
			"buffer_size", ErrorChannelBufferSize, "error", err.Err)
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

	s.logger.InfoContext(context.Background(), "closing subscriber")

	// The worker lifecycle is owned by Close(), not by the Subscribe
	// caller's context, matching the other backends. startMu is held
	// across cancel and wait, and Subscribe re-checks s.closed inside its
	// startMu block, so no Subscribe can spawn workers after Close's
	// Wait returns.
	s.startMu.Lock()
	s.workerCancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(DefaultShutdownTimeout):
		s.logger.WarnContext(context.Background(), "timed out waiting for event log workers to stop")
	}
	reaperDone := s.reaperDone
	s.startMu.Unlock()

	if reaperDone != nil {
		select {
		case <-reaperDone:
		case <-time.After(DefaultShutdownTimeout):
			s.logger.WarnContext(context.Background(), "timed out waiting for the event log retention reaper to stop")
		}
	}

	s.pool.Close()
	close(s.errorChan)

	s.logger.InfoContext(context.Background(), "successfully closed subscriber")
	return nil
}
