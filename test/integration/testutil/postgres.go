package testutil

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

//go:embed maintenance_routine.sql
var maintenanceRoutineSQL string

const (
	eventLogFixtureRetention = 24 * time.Hour
	eventLogOldPartitionAge  = 3 * 24 * time.Hour
)

// PostgresContainerRequest returns the request for the suites' Postgres
// container: postgres:16-alpine with the broker admin user, trust
// authentication, and a wait for the server to accept connections.
// Callers add fields such as networks before StartPostgresContainer.
func PostgresContainerRequest() testcontainers.ContainerRequest {
	return testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":             "broker",
			"POSTGRES_PASSWORD":         "broker",
			"POSTGRES_DB":               "broker",
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}
}

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

// ProvisionEventStore prepares the database behind adminDSN the way a
// deployment does and returns the DSN for the runtime role: it performs the
// platform admin's steps (ProvisionEventStoreDatabase), runs the event_store
// migrations through broker.MigrateEventStore as event_store_migrate,
// installs the maintenance routine stand-in, creates the live daily
// partitions, and adds an old partition for the retention tests. In this
// database only, the admin role's search_path is pinned to event_store, so
// tests can inspect the tables through adminDSN with unqualified names.
func ProvisionEventStore(ctx context.Context, adminDSN string) (string, error) {
	admin, err := connectSimple(ctx, adminDSN)
	if err != nil {
		return "", err
	}
	defer CloseConn(admin)
	if err = ProvisionEventStoreDatabase(ctx, admin, false); err != nil {
		return "", err
	}
	if _, err = admin.Exec(ctx, `DO $$ BEGIN
		EXECUTE format('ALTER ROLE CURRENT_USER IN DATABASE %I SET search_path = event_store, public',
			current_database());
	END $$`); err != nil {
		return "", fmt.Errorf("failed to pin the admin search_path: %w", err)
	}

	migrateDSN, err := dsnWithUser(adminDSN, EventStoreMigrateRole)
	if err != nil {
		return "", err
	}
	if err = broker.MigrateEventStore(ctx, NewTestLogger(), BuildPostgresConfigMap(migrateDSN)); err != nil {
		return "", fmt.Errorf("failed to migrate event_store: %w", err)
	}

	// The routine and every partition belong to the migration role, so
	// the routine (SECURITY DEFINER) can drop the partitions it rotates.
	owner, err := connectSimple(ctx, migrateDSN)
	if err != nil {
		return "", err
	}
	defer CloseConn(owner)
	// TODO(HYPERFLEET-1878): drop this install when the routine ships as a
	// migration; MigrateEventStore above then installs it.
	if _, err = owner.Exec(ctx, maintenanceRoutineSQL); err != nil {
		return "", fmt.Errorf("failed to install the maintenance routine stand-in: %w", err)
	}
	if _, err = owner.Exec(ctx, `SELECT event_store.broker_maintenance($1)`,
		eventLogFixtureRetention.Seconds()); err != nil {
		return "", fmt.Errorf("failed to run event log maintenance: %w", err)
	}
	old := time.Now().UTC().Add(-eventLogOldPartitionAge).Truncate(24 * time.Hour)
	stmt := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS event_store.broker_events_%s PARTITION OF event_store.broker_events
			FOR VALUES FROM ('%s') TO ('%s')`,
		old.Format("20060102"), old.Format("2006-01-02"), old.AddDate(0, 0, 1).Format("2006-01-02"))
	if _, err = owner.Exec(ctx, stmt); err != nil {
		return "", fmt.Errorf("failed to create old event log partition: %w", err)
	}

	return dsnWithUser(adminDSN, EventStoreAppRole)
}

// connectSimple connects through the simple protocol, which runs a
// multi-statement script as one implicit transaction.
func connectSimple(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres DSN: %w", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	return conn, nil
}

// CloseConn closes conn and reports a failure on stderr; it suits deferred
// cleanup where the error has no caller to return to.
func CloseConn(conn *pgx.Conn) {
	if err := conn.Close(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close postgres connection: %v\n", err)
	}
}
