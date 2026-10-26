package eventlog_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/common"
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
// transaction mode, networked to Postgres) for the package, mirroring the
// river integration test setup.
func TestMain(m *testing.M) {
	common.SetupTestEnvironment()

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
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		_ = testNetwork.Remove(ctx)
		fmt.Fprintf(os.Stderr, "failed to start postgres container: %v\n", err)
		os.Exit(1)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		_ = testNetwork.Remove(ctx)
		fmt.Fprintf(os.Stderr, "failed to get postgres host: %v\n", err)
		os.Exit(1)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		_ = container.Terminate(ctx)
		_ = testNetwork.Remove(ctx)
		fmt.Fprintf(os.Stderr, "failed to get postgres port: %v\n", err)
		os.Exit(1)
	}

	sharedPostgresDSN = fmt.Sprintf("postgres://broker:broker@%s:%s/broker?sslmode=disable", host, port.Port())

	pgbouncer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "edoburu/pgbouncer:latest",
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
		_ = container.Terminate(ctx)
		_ = testNetwork.Remove(ctx)
		fmt.Fprintf(os.Stderr, "failed to start pgbouncer container: %v\n", err)
		os.Exit(1)
	}

	pbHost, err := pgbouncer.Host(ctx)
	if err != nil {
		_ = pgbouncer.Terminate(ctx)
		_ = container.Terminate(ctx)
		_ = testNetwork.Remove(ctx)
		fmt.Fprintf(os.Stderr, "failed to get pgbouncer host: %v\n", err)
		os.Exit(1)
	}
	pbPort, err := pgbouncer.MappedPort(ctx, "5432")
	if err != nil {
		_ = pgbouncer.Terminate(ctx)
		_ = container.Terminate(ctx)
		_ = testNetwork.Remove(ctx)
		fmt.Fprintf(os.Stderr, "failed to get pgbouncer port: %v\n", err)
		os.Exit(1)
	}

	sharedPgBouncerDSN = fmt.Sprintf("postgres://broker:broker@%s:%s/broker?sslmode=disable", pbHost, pbPort.Port())

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

func eventLogConfigMap() map[string]string {
	return common.BuildConfigMap("eventlog", sharedPostgresDSN, "")
}

// TestPublisherSubscriber tests the full publish/subscribe flow with the event log
func TestPublisherSubscriber(t *testing.T) {
	common.RunPublisherSubscriber(t, eventLogConfigMap(), common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestMultipleEvents tests that multiple events are processed correctly
func TestMultipleEvents(t *testing.T) {
	common.RunMultipleEvents(t, eventLogConfigMap(), common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestSharedSubscription tests that two subscribers with the same subscriptionID share messages
func TestSharedSubscription(t *testing.T) {
	common.RunSharedSubscription(t, eventLogConfigMap(), common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 20 * time.Second,
	})
}

// TestFanoutSubscription tests that two subscribers with different subscriptionIDs each get all messages
func TestFanoutSubscription(t *testing.T) {
	common.RunFanoutSubscription(t, eventLogConfigMap(), common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 20 * time.Second,
	})
}

// TestSlowSubscriber tests that a slow subscriber processes fewer messages than a fast one
func TestSlowSubscriber(t *testing.T) {
	configMap := eventLogConfigMap()

	subscriptionID := fmt.Sprintf("slow-subscription-%d", time.Now().UnixNano())
	sub1, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), subscriptionID, common.NewTestMetrics(t), configMap)
	if err != nil {
		t.Fatalf("failed to create subscriber 1: %v", err)
	}
	sub2, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), subscriptionID, common.NewTestMetrics(t), configMap)
	if err != nil {
		t.Fatalf("failed to create subscriber 2: %v", err)
	}

	common.RunSlowSubscriber(t, configMap, common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 30 * time.Second,
		PublishDelay:   50 * time.Millisecond,
	}, sub1, sub2)
}

// TestErrorSubscriber tests that messages are redistributed when one subscriber fails
func TestErrorSubscriber(t *testing.T) {
	common.RunErrorSubscriber(t, eventLogConfigMap(), common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 15 * time.Second,
	})
}

// TestCloseWaitsForInFlightMessages tests that Close() waits for in-flight processing
func TestCloseWaitsForInFlightMessages(t *testing.T) {
	configMap := eventLogConfigMap()
	// One event per range and enough workers that all published messages
	// are in flight together, matching the shared scenario's parallelism.
	configMap["broker.eventlog.batch_size"] = "1"
	configMap["subscriber.parallelism"] = "6"

	common.RunCloseWaitsForInFlightMessages(t, configMap, common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 20 * time.Second,
	})
}

// TestPanicHandler tests that a panicking handler does not hang Close()
func TestPanicHandler(t *testing.T) {
	common.RunPanicHandler(t, eventLogConfigMap(), common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 15 * time.Second,
	})
}

// TestErrorChannelNotification tests error channel lifecycle
func TestErrorChannelNotification(t *testing.T) {
	common.RunErrorChannelNotification(t, eventLogConfigMap(), common.BrokerTestConfig{
		BrokerType:     "eventlog",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})
}
