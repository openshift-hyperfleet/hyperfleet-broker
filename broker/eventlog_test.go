package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresConfigValidation(t *testing.T) {
	valid := minimalPostgresConfig()

	tests := []struct {
		name        string
		configMap   map[string]string
		expectError string
	}{
		{
			name:      "valid minimal config",
			configMap: valid,
		},
		{
			name: "valid full config",
			configMap: func() map[string]string {
				m := map[string]string{
					"broker.type":                    "postgres",
					"broker.postgres.host":           "localhost",
					"broker.postgres.port":           "5432",
					"broker.postgres.database":       "event_store",
					"broker.postgres.user":           "event_store_app",
					"broker.postgres.password":       "secret",
					"broker.postgres.sslmode":        "verify-full",
					"broker.postgres.ca_cert_file":   "/etc/pki/ca.crt",
					"broker.postgres.pool_max_conns": "4",
					"broker.postgres.poll_interval":  "100ms",
					"broker.postgres.fetch_cooldown": "50ms",
					"broker.postgres.batch_size":     "5",
					"broker.postgres.grace_period":   "1s",
					"broker.postgres.lease_duration": "10s",
					"broker.postgres.max_attempts":   "50",
					"broker.postgres.retry_backoff":  "200ms",
					"broker.postgres.retention":      "24h",
				}
				return m
			}(),
		},
		{
			name:      "valid empty password",
			configMap: testPostgresConfig(map[string]string{"broker.postgres.password": ""}),
		},
		{
			name:        "missing host",
			configMap:   testPostgresConfigWithout("broker.postgres.host"),
			expectError: "host is required",
		},
		{
			name:        "missing database",
			configMap:   testPostgresConfigWithout("broker.postgres.database"),
			expectError: "database is required",
		},
		{
			name:        "missing user",
			configMap:   testPostgresConfigWithout("broker.postgres.user"),
			expectError: "user is required",
		},
		{
			name:        "missing sslmode has no default",
			configMap:   testPostgresConfigWithout("broker.postgres.sslmode"),
			expectError: "sslmode is required",
		},
		{
			name:        "unsupported sslmode",
			configMap:   testPostgresConfig(map[string]string{"broker.postgres.sslmode": "verify-identity"}),
			expectError: "invalid postgres sslmode",
		},
		{
			name:        "port out of range",
			configMap:   testPostgresConfig(map[string]string{"broker.postgres.port": "70000"}),
			expectError: "port must be between",
		},
		{
			name:        "invalid duration",
			configMap:   testPostgresConfig(map[string]string{"broker.postgres.grace_period": "not-a-duration"}),
			expectError: "grace_period",
		},
		{
			name:        "non-positive duration",
			configMap:   testPostgresConfig(map[string]string{"broker.postgres.grace_period": "0s"}),
			expectError: "grace_period",
		},
		{
			name:        "negative batch size",
			configMap:   testPostgresConfig(map[string]string{"broker.postgres.batch_size": "-1"}),
			expectError: "batch_size",
		},
		{
			name:        "negative max attempts",
			configMap:   testPostgresConfig(map[string]string{"broker.postgres.max_attempts": "-2"}),
			expectError: "max_attempts",
		},
		{
			name:        "negative pool max connections",
			configMap:   testPostgresConfig(map[string]string{"broker.postgres.pool_max_conns": "-1"}),
			expectError: "pool_max_conns",
		},
		{
			name:        "negative port",
			configMap:   testPostgresConfig(map[string]string{"broker.postgres.port": "-1"}),
			expectError: "port must be between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildConfigFromMap(tt.configMap)
			if tt.expectError == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			}
		})
	}
}

func minimalPostgresConfig() map[string]string {
	return map[string]string{
		"broker.type":              "postgres",
		"broker.postgres.host":     "localhost",
		"broker.postgres.database": "broker",
		"broker.postgres.user":     "broker",
		"broker.postgres.sslmode":  "verify-full",
	}
}

func testPostgresConfig(overrides map[string]string) map[string]string {
	config := minimalPostgresConfig()
	for key, value := range overrides {
		config[key] = value
	}
	return config
}

func testPostgresConfigWithout(keys ...string) map[string]string {
	config := minimalPostgresConfig()
	for _, key := range keys {
		delete(config, key)
	}
	return config
}

func TestPostgresConfigDefaults(t *testing.T) {
	cfg, err := buildConfigFromMap(map[string]string{
		"broker.type":              "postgres",
		"broker.postgres.host":     "localhost",
		"broker.postgres.database": "broker",
		"broker.postgres.user":     "broker",
		"broker.postgres.sslmode":  "verify-full",
	})
	require.NoError(t, err)

	pc := cfg.Broker.Postgres
	assert.Equal(t, DefaultEventLogPollInterval, pc.pollInterval())
	assert.Equal(t, DefaultEventLogFetchCooldown, pc.fetchCooldown())
	assert.Equal(t, int64(DefaultEventLogBatchSize), pc.batchSize())
	assert.Equal(t, DefaultEventLogGracePeriod, pc.gracePeriod())
	assert.Equal(t, DefaultEventLogLeaseDuration, pc.leaseDuration())
	assert.Equal(t, DefaultEventLogMaxAttempts, pc.maxAttempts())
	assert.Equal(t, DefaultEventLogRetryBackoff, pc.retryBackoff())
	assert.Equal(t, DefaultEventLogRetention, pc.retention())
	assert.Equal(t, int32(DefaultEventLogPoolMaxConns), pc.poolMaxConns())
	assert.Equal(t, DefaultEventLogPort, pc.port())
}

func TestPostgresDurationHelpersFallBackOnGarbage(t *testing.T) {
	pc := postgresConfig{
		PollInterval:  "garbage",
		FetchCooldown: "",
		GracePeriod:   "-5s",
		LeaseDuration: "0s",
		RetryBackoff:  "nope",
		Retention:     "",
	}
	assert.Equal(t, DefaultEventLogPollInterval, pc.pollInterval())
	assert.Equal(t, DefaultEventLogFetchCooldown, pc.fetchCooldown())
	assert.Equal(t, DefaultEventLogGracePeriod, pc.gracePeriod())
	assert.Equal(t, DefaultEventLogLeaseDuration, pc.leaseDuration())
	assert.Equal(t, DefaultEventLogRetryBackoff, pc.retryBackoff())
	assert.Equal(t, DefaultEventLogRetention, pc.retention())
}

// An unquoted empty password would swallow the following sslmode token.
func TestPostgresDSNEmptyPasswordQuoted(t *testing.T) {
	cfg, err := buildConfigFromMap(map[string]string{
		"broker.type":              "postgres",
		"broker.postgres.host":     "localhost",
		"broker.postgres.database": "event_store",
		"broker.postgres.user":     "event_store_app",
		"broker.postgres.sslmode":  "verify-full",
	})
	require.NoError(t, err)
	pc := cfg.Broker.Postgres

	dsn := pc.dsn()
	assert.Contains(t, dsn, "password=''")

	parsed, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	assert.Empty(t, parsed.ConnConfig.Password,
		"the empty password must not consume the parameters that follow it")
	assert.Equal(t, "event_store", parsed.ConnConfig.Database)
	assert.Equal(t, "event_store_app", parsed.ConnConfig.User)

	// The unquoted form demonstrates the failure mode this guards: the
	// parser consumes the next token after an unquoted empty value, so the
	// password swallows sslmode.
	unquoted := strings.Replace(dsn, "password=''", "password=", 1)
	parsedUnquoted, err := pgxpool.ParseConfig(unquoted)
	require.NoError(t, err)
	assert.Equal(t, "sslmode=verify-full", parsedUnquoted.ConnConfig.Password,
		"pgx must still exhibit the token-swallowing behavior this quoting guards")
}

func TestPostgresDSNEscaping(t *testing.T) {
	cfg, err := buildConfigFromMap(map[string]string{
		"broker.type":              "postgres",
		"broker.postgres.host":     "db.internal",
		"broker.postgres.port":     "5432",
		"broker.postgres.database": "event_store",
		"broker.postgres.user":     "event_store_app",
		"broker.postgres.password": "p@ss word='quoted'",
		"broker.postgres.sslmode":  "verify-full",
	})
	require.NoError(t, err)
	pc := cfg.Broker.Postgres

	dsn := pc.dsn()

	// A password with spaces, quotes, and equals survives as one value,
	// and the parameters after it keep their own values.
	parsed, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	assert.Equal(t, "p@ss word='quoted'", parsed.ConnConfig.Password)
	assert.Equal(t, "db.internal", parsed.ConnConfig.Host)
	assert.Equal(t, "event_store", parsed.ConnConfig.Database)
	assert.Equal(t, "event_store_app", parsed.ConnConfig.User)

	// The CA bundle path lands in sslrootcert. Path values contain slashes
	// (not simple characters), so the path is quoted; pgx reads the file
	// at parse time, so the assertion stays on the assembled DSN.
	pc.CACertFile = "/etc/pki/ca.crt"
	assert.Contains(t, pc.dsn(), "sslrootcert='/etc/pki/ca.crt'")

	// A password containing a backslash — and one combining a backslash
	// with a single quote — round-trips through the escaped, quoted form.
	for _, password := range []string{`back\slash`, `mix\'ed"`} {
		cfg, err := buildConfigFromMap(map[string]string{
			"broker.type":              "postgres",
			"broker.postgres.host":     "db.internal",
			"broker.postgres.database": "event_store",
			"broker.postgres.user":     "event_store_app",
			"broker.postgres.password": password,
			"broker.postgres.sslmode":  "verify-full",
		})
		require.NoError(t, err)
		parsed, err := pgxpool.ParseConfig(cfg.Broker.Postgres.dsn())
		require.NoError(t, err)
		assert.Equal(t, password, parsed.ConnConfig.Password)
	}
}

func TestSendErrorDropsOldestWhenChannelFull(t *testing.T) {
	s := &eventLogSubscriber{
		logger:         discardLogger,
		subscriptionID: "overflow-sub",
		errorChan:      make(chan *SubscriberError, 2),
	}
	for i := range 2 {
		s.errorChan <- &SubscriberError{Err: fmt.Errorf("buffered %d", i)}
	}

	s.sendError(context.Background(), &SubscriberError{Err: errors.New("newest")})

	first := <-s.errorChan
	second := <-s.errorChan
	assert.EqualError(t, first.Err, "buffered 1", "the oldest buffered error is dropped")
	assert.EqualError(t, second.Err, "newest", "the newest error is retained")
}

func TestPostgresConfigMarshalRedactsPassword(t *testing.T) {
	pc := postgresConfig{
		Host:     "localhost",
		Database: "event_store",
		User:     "event_store_app",
		Password: "secret",
		SSLMode:  "verify-full",
	}
	data, err := json.Marshal(pc)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "secret", "the password must be redacted when the configuration is logged")
	assert.Contains(t, string(data), redactedValue)
	assert.Contains(t, string(data), "localhost")
	assert.Contains(t, string(data), "verify-full")
}
