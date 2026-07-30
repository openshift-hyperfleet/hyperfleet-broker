package river_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/common"
)

// sharedPostgresDSN holds the connection string for the shared Postgres
// container created once in TestMain and reused across all tests.
var sharedPostgresDSN string

// TestMain starts one Postgres container for the package, mirroring the
// rabbitmq integration test setup.
func TestMain(m *testing.M) {
	common.SetupTestEnvironment()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "broker",
			"POSTGRES_PASSWORD": "broker",
			"POSTGRES_DB":       "broker",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start postgres container: %v\n", err)
		os.Exit(1)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get postgres host: %v\n", err)
		os.Exit(1)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get postgres port: %v\n", err)
		os.Exit(1)
	}

	sharedPostgresDSN = fmt.Sprintf("postgres://broker:broker@%s:%s/broker?sslmode=disable", host, port.Port())

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate postgres container: %v\n", err)
	}

	os.Exit(code)
}

func riverConfigMap() map[string]string {
	return common.BuildConfigMap("river", sharedPostgresDSN, "")
}

// TestPublisherSubscriber tests the full publish/subscribe flow with River
func TestPublisherSubscriber(t *testing.T) {
	common.RunPublisherSubscriber(t, riverConfigMap(), common.BrokerTestConfig{
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestMultipleEvents tests that multiple events are processed correctly
func TestMultipleEvents(t *testing.T) {
	common.RunMultipleEvents(t, riverConfigMap(), common.BrokerTestConfig{
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestSharedSubscription tests that two subscribers with the same subscriptionID share messages
func TestSharedSubscription(t *testing.T) {
	common.RunSharedSubscription(t, riverConfigMap(), common.BrokerTestConfig{
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 20 * time.Second,
	})
}

// TestFanoutSubscription tests that two subscribers with different subscriptionIDs each get all messages
func TestFanoutSubscription(t *testing.T) {
	common.RunFanoutSubscription(t, riverConfigMap(), common.BrokerTestConfig{
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 20 * time.Second,
	})
}

// TestSlowSubscriber tests that a slow subscriber processes fewer messages than a fast one
func TestSlowSubscriber(t *testing.T) {
	configMap := riverConfigMap()

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
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 30 * time.Second,
		PublishDelay:   50 * time.Millisecond,
	}, sub1, sub2)
}

// TestErrorSubscriber tests that messages are redistributed when one subscriber fails
func TestErrorSubscriber(t *testing.T) {
	common.RunErrorSubscriber(t, riverConfigMap(), common.BrokerTestConfig{
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 15 * time.Second,
	})
}

// TestCloseWaitsForInFlightMessages tests that Close() waits for in-flight processing
func TestCloseWaitsForInFlightMessages(t *testing.T) {
	configMap := riverConfigMap()
	// Enough workers that all published messages are in flight together,
	// matching the parallelism the shared scenario configures for rabbitmq.
	configMap["subscriber.parallelism"] = "6"

	common.RunCloseWaitsForInFlightMessages(t, configMap, common.BrokerTestConfig{
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 20 * time.Second,
	})
}

// TestPanicHandler tests that a panicking handler does not hang Close()
func TestPanicHandler(t *testing.T) {
	common.RunPanicHandler(t, riverConfigMap(), common.BrokerTestConfig{
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 15 * time.Second,
	})
}

// TestErrorChannelNotification tests error channel lifecycle
func TestErrorChannelNotification(t *testing.T) {
	common.RunErrorChannelNotification(t, riverConfigMap(), common.BrokerTestConfig{
		BrokerType:     "river",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})
}
