package testutil

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
)

//go:embed schema.sql
var eventLogSchema string

const (
	eventLogFixtureRetention = 24 * time.Hour
	eventLogOldPartitionAge  = 3 * 24 * time.Hour
)

// StartPostgresContainer starts a PostgreSQL container from the caller's
// request and returns its local DSN.
func StartPostgresContainer(
	ctx context.Context, request testcontainers.ContainerRequest,
) (testcontainers.Container, string, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("create container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		if cleanupErr := container.Terminate(ctx); cleanupErr != nil {
			return nil, "", fmt.Errorf("get container host: %w (cleanup: %v)", err, cleanupErr)
		}
		return nil, "", fmt.Errorf("get container host: %w", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		if cleanupErr := container.Terminate(ctx); cleanupErr != nil {
			return nil, "", fmt.Errorf("get container port: %w (cleanup: %v)", err, cleanupErr)
		}
		return nil, "", fmt.Errorf("get container port: %w", err)
	}

	dsn := fmt.Sprintf("postgres://broker:broker@%s:%s/broker?sslmode=disable", host, port.Port())
	return container, dsn, nil
}

// ApplyEventLogSchema applies the event log schema fixture (tables,
// index, and minimal maintenance routine), creates the live daily
// partitions, and adds an old partition for the retention tests. The
// production schema comes from the event_store migration artifacts.
func ApplyEventLogSchema(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse postgres DSN: %w", err)
	}
	// The fixture is a multi-statement script: run it through the simple
	// protocol, which executes it as one implicit transaction.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer func() {
		if err := conn.Close(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close postgres connection: %v\n", err)
		}
	}()

	if _, err := conn.Exec(ctx, eventLogSchema); err != nil {
		return fmt.Errorf("failed to apply event log schema fixture: %w", err)
	}

	if _, err := conn.Exec(ctx, `SELECT broker_maintenance($1)`, eventLogFixtureRetention.Seconds()); err != nil {
		return fmt.Errorf("failed to run event log maintenance: %w", err)
	}

	old := time.Now().UTC().Add(-eventLogOldPartitionAge).Truncate(24 * time.Hour)
	stmt := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS broker_events_%s PARTITION OF broker_events FOR VALUES FROM ('%s') TO ('%s')`,
		old.Format("20060102"), old.Format("2006-01-02"), old.AddDate(0, 0, 1).Format("2006-01-02"))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("failed to create old event log partition: %w", err)
	}
	return nil
}
