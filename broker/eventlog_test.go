package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventLogConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		configMap   map[string]string
		expectError string
	}{
		{
			name: "valid minimal config",
			configMap: map[string]string{
				"broker.type":         "eventlog",
				"broker.eventlog.dsn": "postgres://user:pass@localhost:5432/broker",
			},
		},
		{
			name: "valid full config",
			configMap: map[string]string{
				"broker.type":         "eventlog",
				"broker.eventlog.dsn": "postgres://user:pass@localhost:5432/broker",
				"broker.eventlog.create_schema_if_missing": "true",
				"broker.eventlog.poll_interval":            "100ms",
				"broker.eventlog.fetch_cooldown":           "50ms",
				"broker.eventlog.batch_size":               "5",
				"broker.eventlog.grace_period":             "1s",
				"broker.eventlog.lease_duration":           "10s",
				"broker.eventlog.max_attempts":             "50",
				"broker.eventlog.retry_backoff":            "200ms",
				"broker.eventlog.retention":                "24h",
				"broker.eventlog.pool_max_conns":           "4",
			},
		},
		{
			name: "missing dsn",
			configMap: map[string]string{
				"broker.type": "eventlog",
			},
			expectError: "eventlog DSN is required",
		},
		{
			name: "keyword value dsn rejected",
			configMap: map[string]string{
				"broker.type":         "eventlog",
				"broker.eventlog.dsn": "host=localhost user=u password=p dbname=broker",
			},
			expectError: "URI form",
		},
		{
			name: "invalid dsn",
			configMap: map[string]string{
				"broker.type":         "eventlog",
				"broker.eventlog.dsn": "postgres://not a uri",
			},
			expectError: "invalid eventlog DSN",
		},
		{
			name: "invalid duration",
			configMap: map[string]string{
				"broker.type":                  "eventlog",
				"broker.eventlog.dsn":          "postgres://user:pass@localhost:5432/broker",
				"broker.eventlog.grace_period": "not-a-duration",
			},
			expectError: "grace_period",
		},
		{
			name: "negative batch size",
			configMap: map[string]string{
				"broker.type":                "eventlog",
				"broker.eventlog.dsn":        "postgres://user:pass@localhost:5432/broker",
				"broker.eventlog.batch_size": "-1",
			},
			expectError: "batch_size",
		},
		{
			name: "negative max attempts",
			configMap: map[string]string{
				"broker.type":                  "eventlog",
				"broker.eventlog.dsn":          "postgres://user:pass@localhost:5432/broker",
				"broker.eventlog.max_attempts": "-2",
			},
			expectError: "max_attempts",
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

func TestEventLogConfigDefaults(t *testing.T) {
	cfg, err := buildConfigFromMap(map[string]string{
		"broker.type":         "eventlog",
		"broker.eventlog.dsn": "postgres://user:pass@localhost:5432/broker",
	})
	require.NoError(t, err)

	ec := cfg.Broker.EventLog
	assert.Equal(t, DefaultEventLogPollInterval, ec.pollInterval())
	assert.Equal(t, DefaultEventLogFetchCooldown, ec.fetchCooldown())
	assert.Equal(t, int64(DefaultEventLogBatchSize), ec.batchSize())
	assert.Equal(t, DefaultEventLogGracePeriod, ec.gracePeriod())
	assert.Equal(t, DefaultEventLogLeaseDuration, ec.leaseDuration())
	assert.Equal(t, DefaultEventLogMaxAttempts, ec.maxAttempts())
	assert.Equal(t, DefaultEventLogRetryBackoff, ec.retryBackoff())
	assert.Equal(t, DefaultEventLogRetention, ec.retention())
	assert.Equal(t, int32(DefaultEventLogPoolMaxConns), ec.poolMaxConns())
}

func TestEventLogDurationHelpersFallBackOnGarbage(t *testing.T) {
	ec := eventLogConfig{
		PollInterval:  "garbage",
		FetchCooldown: "",
		GracePeriod:   "-5s",
		LeaseDuration: "0s",
		RetryBackoff:  "nope",
		Retention:     "",
	}
	assert.Equal(t, DefaultEventLogPollInterval, ec.pollInterval())
	assert.Equal(t, DefaultEventLogFetchCooldown, ec.fetchCooldown())
	assert.Equal(t, DefaultEventLogGracePeriod, ec.gracePeriod())
	assert.Equal(t, DefaultEventLogLeaseDuration, ec.leaseDuration())
	assert.Equal(t, DefaultEventLogRetryBackoff, ec.retryBackoff())
	assert.Equal(t, DefaultEventLogRetention, ec.retention())
}

func TestEventLogPartitionName(t *testing.T) {
	day := time.Date(2026, 10, 23, 15, 4, 5, 0, time.UTC)
	assert.Equal(t, "broker_events_20261023", eventLogPartitionName(day))

	// Non-UTC input is normalized to the UTC calendar day.
	ist := time.Date(2026, 10, 24, 1, 0, 0, 0, time.FixedZone("IST", 5*3600+30*60))
	assert.Equal(t, "broker_events_20261023", eventLogPartitionName(ist))
}
