package eventlog_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/testutil"
)

// sharedPostgresDSN holds the connection string for the shared Postgres
// container created once in TestMain and reused across all tests.
// sharedPgBouncerDSN routes the same Postgres through PgBouncer in
// transaction mode.
var (
	sharedPostgresDSN  string
	sharedPgBouncerDSN string
)

// TestMain starts one Postgres container and one PgBouncer container (in
// transaction mode, networked to Postgres) for the package, applies the
// event log schema fixture, and runs the tests.
func TestMain(m *testing.M) {
	testutil.SetupTestEnvironment()

	ctx := context.Background()

	testNetwork, err := network.New(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create test network: %v\n", err)
		os.Exit(1)
	}
	networkName := testNetwork.Name

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":             "broker",
			"POSTGRES_PASSWORD":         "broker",
			"POSTGRES_DB":               "broker",
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"eventlog-pg"}},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}
	container, postgresDSN, err := testutil.StartPostgresContainer(ctx, req)
	if err != nil {
		_ = testNetwork.Remove(ctx)
		fmt.Fprintf(os.Stderr, "failed to start postgres container: %v\n", err)
		os.Exit(1)
	}
	sharedPostgresDSN = postgresDSN

	pgbouncer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "edoburu/pgbouncer@sha256:88769b6bbae0fbe0a198c26dc4c2f6bc3bf95bec4cab7d18c6a4348fa36cccc2",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"DB_HOST":     "eventlog-pg",
				"DB_PORT":     "5432",
				"DB_USER":     "broker",
				"DB_PASSWORD": "broker",
				"DB_NAME":     "broker",
				"POOL_MODE":   "transaction",
				"AUTH_TYPE":   "trust",
			},
			Networks: []string{networkName},
			WaitingFor: wait.ForListeningPort("5432/tcp").
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		_ = container.Terminate(ctx) // best-effort cleanup; the process exits below
		_ = testNetwork.Remove(ctx)  // best-effort cleanup; the process exits below
		fmt.Fprintf(os.Stderr, "failed to start pgbouncer container: %v\n", err)
		os.Exit(1)
	}

	pbHost, err := pgbouncer.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx) // best-effort cleanup; the process exits below
		_ = testNetwork.Remove(ctx)  // best-effort cleanup; the process exits below
		fmt.Fprintf(os.Stderr, "failed to get pgbouncer host: %v\n", err)
		os.Exit(1)
	}
	pbPort, err := pgbouncer.MappedPort(ctx, "5432")
	if err != nil {
		_ = container.Terminate(ctx) // best-effort cleanup; the process exits below
		_ = testNetwork.Remove(ctx)  // best-effort cleanup; the process exits below
		fmt.Fprintf(os.Stderr, "failed to get pgbouncer port: %v\n", err)
		os.Exit(1)
	}

	sharedPgBouncerDSN = fmt.Sprintf("postgres://broker:broker@%s:%s/broker?sslmode=disable", pbHost, pbPort.Port())

	// The backend creates no schema: the suite applies the fixture before
	// any test runs.
	if err := testutil.ApplyEventLogSchema(ctx, sharedPostgresDSN); err != nil {
		_ = container.Terminate(ctx) // best-effort cleanup; the process exits below
		_ = testNetwork.Remove(ctx)  // best-effort cleanup; the process exits below
		fmt.Fprintf(os.Stderr, "failed to apply event log schema: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := pgbouncer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate pgbouncer container: %v\n", err)
	}
	if err := container.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate postgres container: %v\n", err)
	}
	if err := testNetwork.Remove(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to remove test network: %v\n", err)
	}

	os.Exit(code)
}

// postgresConfigMap returns the postgres backend configuration against the
// given DSN, tuned for test convergence: fast polls so competing
// subscribers interleave, one event per range so distribution is
// deterministic, short backoff and a high attempt ceiling so error
// redistribution converges within test timeouts.
func postgresConfigMap(dsn string) map[string]string {
	configMap := testutil.BuildPostgresConfigMap(dsn)
	configMap["broker.postgres.poll_interval"] = testInterval
	configMap["broker.postgres.fetch_cooldown"] = testInterval
	configMap["broker.postgres.batch_size"] = "1"
	configMap["broker.postgres.grace_period"] = "1s"
	configMap["broker.postgres.max_attempts"] = "50"
	configMap["broker.postgres.retry_backoff"] = "200ms"
	// A long retention keeps the fixture's old partition alive across every
	// reaper in the suite; only the reconciliation test rotates partitions,
	// through its own explicit maintenance invocation.
	configMap["broker.postgres.retention"] = "720h"
	return configMap
}

const eventLogSetupSleep = 500 * time.Millisecond

func postgresScenarioConfig(receiveTimeout time.Duration) testutil.BrokerTestConfig {
	return testutil.BrokerTestConfig{
		BrokerType:     "postgres",
		SetupSleep:     eventLogSetupSleep,
		ReceiveTimeout: receiveTimeout,
	}
}
