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

// sharedPostgresDSN is the admin connection to the shared Postgres
// container created once in TestMain; tests use it to stage and inspect
// rows. The backend under test connects as event_store_app through
// sharedAppDSN, on the schema the event_store migrations created.
// sharedPgBouncerDSN routes the runtime role through PgBouncer in
// transaction mode.
var (
	sharedPostgresDSN  string
	sharedAppDSN       string
	sharedPgBouncerDSN string
)

const eventLogSetupSleep = 500 * time.Millisecond

// eventLogRetention protects the fixture's partitions from every test's
// reaper; only the reconciliation tests rotate partitions, through their
// own explicit maintenance invocations.
const eventLogRetention = "720h"

// TestMain starts one Postgres container and one PgBouncer container (in
// transaction mode, networked to Postgres) for the package, provisions and
// migrates event_store, and runs the tests.
func TestMain(m *testing.M) {
	testutil.SetupTestEnvironment()

	ctx := context.Background()

	testNetwork, err := network.New(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create test network: %v\n", err)
		os.Exit(1)
	}
	networkName := testNetwork.Name

	req := testutil.PostgresContainerRequest()
	req.Networks = []string{networkName}
	req.NetworkAliases = map[string][]string{networkName: {"eventlog-pg"}}
	container, postgresDSN, err := testutil.StartPostgresContainer(ctx, req)
	if err != nil {
		_ = testNetwork.Remove(ctx)
		fmt.Fprintf(os.Stderr, "failed to start postgres container: %v\n", err)
		os.Exit(1)
	}
	sharedPostgresDSN = postgresDSN

	pgbouncer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "edoburu/pgbouncer@sha256:b6a9f888f4a28a6c843528b8a8bafdb46ef1921efee8672138ad35731edc7da3",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"DB_HOST":     "eventlog-pg",
				"DB_PORT":     "5432",
				"DB_USER":     testutil.EventStoreAppRole,
				"DB_PASSWORD": testutil.EventStoreAppRole,
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
	cleanupSetupFailure := func() {
		_ = pgbouncer.Terminate(ctx) // best-effort cleanup; the process exits below
		_ = container.Terminate(ctx) // best-effort cleanup; the process exits below
		_ = testNetwork.Remove(ctx)  // best-effort cleanup; the process exits below
	}

	pbHost, err := pgbouncer.Host(ctx)
	if err != nil {
		cleanupSetupFailure()
		fmt.Fprintf(os.Stderr, "failed to get pgbouncer host: %v\n", err)
		os.Exit(1)
	}
	pbPort, err := pgbouncer.MappedPort(ctx, "5432")
	if err != nil {
		cleanupSetupFailure()
		fmt.Fprintf(os.Stderr, "failed to get pgbouncer port: %v\n", err)
		os.Exit(1)
	}

	sharedPgBouncerDSN = fmt.Sprintf("postgres://%s@%s:%s/broker?sslmode=disable",
		testutil.EventStoreAppRole, pbHost, pbPort.Port())

	// The backend creates no schema: the suite migrates event_store before
	// any test runs and runs the backend as the runtime role.
	sharedAppDSN, err = testutil.ProvisionEventStore(ctx, sharedPostgresDSN)
	if err != nil {
		cleanupSetupFailure()
		fmt.Fprintf(os.Stderr, "failed to provision event_store: %v\n", err)
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
	configMap["broker.postgres.retention"] = eventLogRetention
	return configMap
}

func postgresScenarioConfig(receiveTimeout time.Duration) testutil.BrokerTestConfig {
	return testutil.BrokerTestConfig{
		BrokerType:     "postgres",
		SetupSleep:     eventLogSetupSleep,
		ReceiveTimeout: receiveTimeout,
	}
}
