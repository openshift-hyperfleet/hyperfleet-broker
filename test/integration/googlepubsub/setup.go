package googlepubsub

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SetupPubSubEmulator starts a Google Pub/Sub emulator testcontainer and returns the project ID and emulator host
// This is exported so it can be used by other test packages (e.g., performance tests)
func SetupPubSubEmulator(t *testing.T) (string, string) {
	ctx := context.Background()

	// Create a generic container for Pub/Sub emulator
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

	// Get the container host and port
	host, err := pubsubContainer.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := pubsubContainer.MappedPort(ctx, "8085")
	require.NoError(t, err)

	emulatorHost := fmt.Sprintf("%s:%s", host, mappedPort.Port())

	// Set environment variable for Pub/Sub emulator
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


