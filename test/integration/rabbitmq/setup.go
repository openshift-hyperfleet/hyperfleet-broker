package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SetupRabbitMQContainer starts a RabbitMQ testcontainer and returns the connection URL
// This is exported so it can be used by other test packages (e.g., performance tests)
func SetupRabbitMQContainer(t *testing.T) string {
	ctx := context.Background()

	rabbitmqContainer, err := rabbitmq.Run(ctx,
		"rabbitmq:3-management-alpine",
		rabbitmq.WithAdminUsername("guest"),
		rabbitmq.WithAdminPassword("guest"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := rabbitmqContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate rabbitmq container: %v", err)
		}
	})

	connectionString, err := rabbitmqContainer.AmqpURL(ctx)
	require.NoError(t, err)

	return connectionString
}


