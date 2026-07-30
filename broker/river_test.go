package broker

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func riverTestMetrics() *MetricsRecorder {
	return NewMetricsRecorder("river-test", "v0.0.0", prometheus.NewRegistry())
}

func TestSanitizeRiverName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"simple", "simple"},
		{"With.Dots.And.Caps", "with-dots-and-caps"},
		{"under_scores-kept", "under_scores-kept"},
		{"--trimmed--", "trimmed"},
		{"weird!@#chars", "weird-chars"},
		{"", "x"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, sanitizeRiverName(tt.in), "input %q", tt.in)
	}
}

func TestRiverQueueName(t *testing.T) {
	// Short names pass through.
	assert.Equal(t, "clusters-my-adapter", riverQueueName("clusters", "my-adapter"))

	// Long names are truncated to the limit with a hash suffix.
	longTopic := strings.Repeat("verylongtopic", 5)
	longSub := strings.Repeat("verylongsubscription", 5)
	name := riverQueueName(longTopic, longSub)
	assert.LessOrEqual(t, len(name), riverQueueNameMaxLen)

	// Distinct long pairs must not collide after truncation.
	other := riverQueueName(longTopic, longSub+"x")
	assert.NotEqual(t, name, other)
}

func TestRiverConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		configMap   map[string]string
		expectError string
	}{
		{
			name: "valid minimal config",
			configMap: map[string]string{
				"broker.type":      "river",
				"broker.river.dsn": "postgres://user:pass@localhost:5432/broker",
			},
		},
		{
			name: "missing dsn",
			configMap: map[string]string{
				"broker.type": "river",
			},
			expectError: "river DSN is required",
		},
		{
			name: "keyword value dsn rejected",
			configMap: map[string]string{
				"broker.type":      "river",
				"broker.river.dsn": "host=localhost user=u password=p dbname=broker",
			},
			expectError: "URI form",
		},
		{
			name: "invalid duration",
			configMap: map[string]string{
				"broker.type":                "river",
				"broker.river.dsn":           "postgres://user:pass@localhost:5432/broker",
				"broker.river.retry_backoff": "not-a-duration",
			},
			expectError: "retry_backoff",
		},
		{
			name: "negative max attempts",
			configMap: map[string]string{
				"broker.type":               "river",
				"broker.river.dsn":          "postgres://user:pass@localhost:5432/broker",
				"broker.river.max_attempts": "-1",
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

func TestRiverConfigDefaults(t *testing.T) {
	cfg, err := buildConfigFromMap(map[string]string{
		"broker.type":      "river",
		"broker.river.dsn": "postgres://user:pass@localhost:5432/broker",
	})
	require.NoError(t, err)

	rc := cfg.Broker.River
	assert.Equal(t, DefaultRiverMaxAttempts, rc.maxAttempts())
	assert.Equal(t, DefaultRiverRetryBackoff, rc.retryBackoff())
	assert.Equal(t, DefaultRiverPollInterval, rc.pollInterval())
	assert.Equal(t, DefaultRiverFetchCooldown, rc.fetchCooldown())
	assert.Equal(t, int32(DefaultRiverPoolMaxConns), rc.poolMaxConns())
}

func TestRiverDurationHelpersFallBackOnGarbage(t *testing.T) {
	rc := riverConfig{
		RetryBackoff:  "garbage",
		PollInterval:  "",
		FetchCooldown: "-5s",
	}
	assert.Equal(t, DefaultRiverRetryBackoff, rc.retryBackoff())
	assert.Equal(t, DefaultRiverPollInterval, rc.pollInterval())
	assert.Equal(t, DefaultRiverFetchCooldown, rc.fetchCooldown())
}

func TestRiverConfigEnvOverride(t *testing.T) {
	dir := t.TempDir()
	configPath := dir + "/broker.yaml"
	yaml := "broker:\n  type: river\n  river:\n    dsn: postgres://file:file@filehost:5432/file\n"
	require.NoError(t, os.WriteFile(configPath, []byte(yaml), 0o600))

	t.Setenv("BROKER_CONFIG_FILE", configPath)
	t.Setenv("BROKER_RIVER_DSN", "postgres://env:env@envhost:5432/env")

	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Equal(t, "postgres://env:env@envhost:5432/env", cfg.Broker.River.DSN,
		"environment variable should override the config file")
}

func TestRiverDSNMasking(t *testing.T) {
	masked := maskPassword("postgres://user:secret@host:5432/db")
	assert.NotContains(t, masked, "secret")
	assert.Contains(t, masked, "user:***@host")
}

func TestNewRiverPublisherConnectionFailure(t *testing.T) {
	configMap := map[string]string{
		"broker.type":      "river",
		"broker.river.dsn": "postgres://user:pass@127.0.0.1:1/broker?connect_timeout=1",
	}
	start := time.Now()
	_, err := NewPublisher(logger.NewMockLogger(), riverTestMetrics(), configMap)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 15*time.Second, "connection failure should fail fast")
}

func TestNewRiverSubscriberConnectionFailure(t *testing.T) {
	configMap := map[string]string{
		"broker.type":      "river",
		"broker.river.dsn": "postgres://user:pass@127.0.0.1:1/broker?connect_timeout=1",
	}
	start := time.Now()
	_, err := NewSubscriber(logger.NewMockLogger(), "river-fail-sub", riverTestMetrics(), configMap)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 15*time.Second, "connection failure should fail fast")
}
