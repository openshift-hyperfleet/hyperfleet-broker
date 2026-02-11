package googlepubsub_test

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
	"github.com/stretchr/testify/require"
)

// sharedProjectID holds the project ID for the shared PubSub emulator container
// created once in TestMain and reused across all tests in this package.
var sharedProjectID string

// TestMain sets up the test environment for Podman and disables Ryuk,
// creates a shared PubSub emulator container, runs all tests, then terminates the container.
func TestMain(m *testing.M) {
	common.SetupTestEnvironment()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
		ExposedPorts: []string{"8085/tcp"},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"},
		WaitingFor: wait.ForLog("Server started").
			WithOccurrence(1).
			WithStartupTimeout(60 * time.Second),
	}

	pubsubContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start pubsub emulator container: %v\n", err)
		os.Exit(1)
	}

	sharedProjectID = "test-project"

	host, err := pubsubContainer.Host(ctx)
	if err != nil {
		_ = pubsubContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get pubsub container host: %v\n", err)
		os.Exit(1)
	}

	mappedPort, err := pubsubContainer.MappedPort(ctx, "8085")
	if err != nil {
		_ = pubsubContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get pubsub container mapped port: %v\n", err)
		os.Exit(1)
	}

	emulatorHost := fmt.Sprintf("%s:%s", host, mappedPort.Port())

	if err := os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost); err != nil {
		_ = pubsubContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to set PUBSUB_EMULATOR_HOST: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	_ = os.Unsetenv("PUBSUB_EMULATOR_HOST")
	if err := pubsubContainer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate pubsub container: %v\n", err)
	}

	os.Exit(code)
}

// SetupPubSubEmulator starts a Google Pub/Sub emulator testcontainer and returns the project ID and emulator host.
// This is exported so it can be used by other test packages (e.g., integration_test).
func SetupPubSubEmulator(t *testing.T) (string, string) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
		ExposedPorts: []string{"8085/tcp"},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"},
		WaitingFor: wait.ForLog("Server started").
			WithOccurrence(1).
			WithStartupTimeout(60 * time.Second),
	}

	pubsubContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pubsubContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate pubsub container: %v", err)
		}
	})

	projectID := "test-project"

	host, err := pubsubContainer.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := pubsubContainer.MappedPort(ctx, "8085")
	require.NoError(t, err)

	emulatorHost := fmt.Sprintf("%s:%s", host, mappedPort.Port())

	if err := os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost); err != nil {
		t.Fatalf("failed to set PUBSUB_EMULATOR_HOST: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Unsetenv("PUBSUB_EMULATOR_HOST"); err != nil {
			t.Logf("failed to unset PUBSUB_EMULATOR_HOST: %v", err)
		}
	})

	return projectID, emulatorHost
}

// TestPublisherSubscriber tests the full publish/subscribe flow with Google Pub/Sub
func TestPublisherSubscriber(t *testing.T) {
	configMap := common.BuildConfigMap("googlepubsub", "", sharedProjectID)

	common.RunPublisherSubscriber(t, configMap, common.BrokerTestConfig{
		BrokerType:     "googlepubsub",
		SetupSleep:     2 * time.Second,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestMultipleEvents tests that multiple events are processed correctly
func TestMultipleEvents(t *testing.T) {
	configMap := common.BuildConfigMap("googlepubsub", "", sharedProjectID)

	common.RunMultipleEvents(t, configMap, common.BrokerTestConfig{
		BrokerType:     "googlepubsub",
		SetupSleep:     2 * time.Second,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestSharedSubscription tests that two subscribers with the same subscriptionID share messages
func TestSharedSubscription(t *testing.T) {
	configMap := common.BuildConfigMap("googlepubsub", "", sharedProjectID)

	common.RunSharedSubscription(t, configMap, common.BrokerTestConfig{
		BrokerType:     "googlepubsub",
		SetupSleep:     2 * time.Second,
		ReceiveTimeout: 15 * time.Second,
	})
}

// TestFanoutSubscription tests that two subscribers with different subscriptionIDs each get all messages
func TestFanoutSubscription(t *testing.T) {
	configMap := common.BuildConfigMap("googlepubsub", "", sharedProjectID)

	common.RunFanoutSubscription(t, configMap, common.BrokerTestConfig{
		BrokerType:     "googlepubsub",
		SetupSleep:     2 * time.Second,
		ReceiveTimeout: 15 * time.Second,
	})
}

// TestSlowSubscriber tests that a slow subscriber processes fewer messages than a fast one
func TestSlowSubscriber(t *testing.T) {
	configMap := common.BuildConfigMap("googlepubsub", "", sharedProjectID)

	// Set MaxOutstandingMessages to 1 to limit how many messages can be in-flight
	configMap["broker.googlepubsub.max_outstanding_messages"] = "1"
	// Set NumGoroutines to 1 to use a single streaming pull stream
	configMap["subscriber.parallelism"] = "1"

	// Create two subscribers with the same subscriptionID (shared subscription)
	// but with different num_goroutines to simulate fast vs slow
	subscriptionID := fmt.Sprintf("slow-subscription-%d", time.Now().UnixNano())
	configMap["broker.googlepubsub.num_goroutines"] = "5"
	sub1, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), subscriptionID, configMap)
	require.NoError(t, err)

	configMap["broker.googlepubsub.num_goroutines"] = "1"
	sub2, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), subscriptionID, configMap)
	require.NoError(t, err)

	common.RunSlowSubscriber(t, configMap, common.BrokerTestConfig{
		SetupSleep:     2 * time.Second,
		ReceiveTimeout: 15 * time.Second,
		PublishDelay:   100 * time.Millisecond, // Gradual publishing for Google Pub/Sub
	}, sub1, sub2)
}

// TestErrorSubscriber tests that messages are redistributed when one subscriber fails
func TestErrorSubscriber(t *testing.T) {
	configMap := common.BuildConfigMap("googlepubsub", "", sharedProjectID)

	common.RunErrorSubscriber(t, configMap, common.BrokerTestConfig{
		BrokerType:     "googlepubsub",
		SetupSleep:     2 * time.Second,
		ReceiveTimeout: 50 * time.Second,
	})
}

// TestCloseWaitsForInFlightMessages tests that Close() waits for in-flight message processing
func TestCloseWaitsForInFlightMessages(t *testing.T) {
	configMap := common.BuildConfigMap("googlepubsub", "", sharedProjectID)

	common.RunCloseWaitsForInFlightMessages(t, configMap, common.BrokerTestConfig{
		BrokerType:     "googlepubsub",
		SetupSleep:     2 * time.Second,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestPanicHandler tests that a handler that panics doesn't cause Close() to hang
func TestPanicHandler(t *testing.T) {
	configMap := common.BuildConfigMap("googlepubsub", "", sharedProjectID)

	common.RunPanicHandler(t, configMap, common.BrokerTestConfig{
		BrokerType:     "googlepubsub",
		SetupSleep:     2 * time.Second,
		ReceiveTimeout: 10 * time.Second,
	})
}
