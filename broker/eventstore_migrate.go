package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker/eventstore"
)

// MigrateEventStore applies the event_store schema migrations for the
// postgres backend. It is the entry point for a migration executor, such as
// a deployment's init container. Publishers and subscribers do not call it,
// because the backend issues no DDL.
//
// It reads configuration the same way as NewPublisher and NewSubscriber:
// broker.yaml, environment variables, or configMap. With the runtime's
// broker.yaml, it uses the same host, database, sslmode, and CA file.
// broker.type must be postgres. The configured user must be
// event_store_migrate; set it with BROKER_POSTGRES_USER and
// BROKER_POSTGRES_PASSWORD from the migration Secret.
// Concurrent runs serialize on an advisory lock, and a rerun changes nothing.
// A run that waits more than five minutes for the lock fails. When ctx has no
// deadline, the connection attempt is bounded at 30 seconds.
// Usage:
//   - MigrateEventStore(ctx, logger) - loads config from file and environment
//   - MigrateEventStore(ctx, logger, configMap) - uses the config map
func MigrateEventStore(ctx context.Context, log *slog.Logger, configMap ...map[string]string) error {
	if log == nil {
		return errors.New("logger is required")
	}

	cfg, err := resolveConfig(configMap...)
	if err != nil {
		return err
	}
	if cfg.Broker.Type != brokerTypePostgres {
		return fmt.Errorf("event_store migrations require broker.type %q, got %q", brokerTypePostgres, cfg.Broker.Type)
	}

	pc := cfg.Broker.Postgres
	connCfg, err := pgx.ParseConfig(pc.dsn())
	if err != nil {
		return errInvalidPostgresDSN
	}

	log.InfoContext(ctx, "applying event_store migrations",
		"host", pc.Host, "database", pc.Database, "user", pc.User)
	conn, err := connectForMigration(ctx, connCfg)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(context.WithoutCancel(ctx)); closeErr != nil {
			log.ErrorContext(ctx, "failed to close event_store migration connection", "error", closeErr)
		}
	}()

	if err := eventstore.Migrate(ctx, conn); err != nil {
		return err
	}
	log.InfoContext(ctx, "event_store migrations applied")
	return nil
}

// connectForMigration connects with ctx, bounded at eventLogPoolTimeout when
// ctx has no deadline, so an unreachable host fails instead of hanging.
func connectForMigration(ctx context.Context, cfg *pgx.ConnConfig) (*pgx.Conn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, eventLogPoolTimeout)
		defer cancel()
	}
	return pgx.ConnectConfig(ctx, cfg)
}
