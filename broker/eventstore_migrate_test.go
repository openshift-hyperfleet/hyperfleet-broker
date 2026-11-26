package broker

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateEventStoreRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		logger    *slog.Logger
		configMap map[string]string
		name      string
		wantErr   string
	}{
		{
			name:      "nil logger",
			configMap: map[string]string{"broker.type": "postgres"},
			wantErr:   "logger is required",
		},
		{
			name:   "non-postgres broker type",
			logger: discardLogger,
			configMap: map[string]string{
				"broker.type":         "rabbitmq",
				"broker.rabbitmq.url": "amqp://guest:guest@localhost:5672/",
			},
			wantErr: `event_store migrations require broker.type "postgres", got "rabbitmq"`,
		},
		{
			name:   "invalid postgres config",
			logger: discardLogger,
			configMap: map[string]string{
				"broker.type": "postgres",
				// Explicit, so BROKER_POSTGRES_HOST cannot supply a host.
				"broker.postgres.host":     "",
				"broker.postgres.database": "event_store",
				"broker.postgres.user":     "event_store_migrate",
				"broker.postgres.sslmode":  "disable",
			},
			wantErr: "postgres host is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := MigrateEventStore(context.Background(), tt.logger, tt.configMap)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
