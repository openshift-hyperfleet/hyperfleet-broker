package rabbitmq_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/common"
)

// sharedRabbitMQURL holds the connection URL for the shared RabbitMQ container
// created once in TestMain and reused across all tests in this package.
var sharedRabbitMQURL string

// TestMain sets up the test environment for Podman and disables Ryuk,
// creates a shared RabbitMQ container, runs all tests, then terminates the container.
func TestMain(m *testing.M) {
	common.SetupTestEnvironment()

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
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start rabbitmq container: %v\n", err)
		os.Exit(1)
	}

	connectionString, err := rabbitmqContainer.AmqpURL(ctx)
	if err != nil {
		_ = rabbitmqContainer.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get rabbitmq AMQP URL: %v\n", err)
		os.Exit(1)
	}

	sharedRabbitMQURL = connectionString

	code := m.Run()

	if err := rabbitmqContainer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate rabbitmq container: %v\n", err)
	}

	os.Exit(code)
}

// TestPublisherSubscriber tests the full publish/subscribe flow with RabbitMQ
func TestPublisherSubscriber(t *testing.T) {
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	common.RunPublisherSubscriber(t, configMap, common.BrokerTestConfig{
		BrokerType:     "rabbitmq",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 5 * time.Second,
	})
}

// TestMultipleEvents tests that multiple events are processed correctly
func TestMultipleEvents(t *testing.T) {
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	common.RunMultipleEvents(t, configMap, common.BrokerTestConfig{
		BrokerType:     "rabbitmq",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 5 * time.Second,
	})
}

// TestSharedSubscription tests that two subscribers with the same subscriptionID share messages
func TestSharedSubscription(t *testing.T) {
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	common.RunSharedSubscription(t, configMap, common.BrokerTestConfig{
		BrokerType:     "rabbitmq",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestFanoutSubscription tests that two subscribers with different subscriptionIDs each get all messages
func TestFanoutSubscription(t *testing.T) {
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	common.RunFanoutSubscription(t, configMap, common.BrokerTestConfig{
		BrokerType:     "rabbitmq",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
	})
}

// TestSubscriptionID tests that subscription IDs work correctly
func TestSubscriptionID(t *testing.T) {
	topic := fmt.Sprintf("fanout-topic-%d", time.Now().UnixNano())
	ctx := context.Background()
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	pub, err := broker.NewPublisher(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	// Create two subscribers with different subscription IDs
	sub1, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "subscription-1", common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub1.Close(); err != nil {
			t.Logf("failed to close subscriber 1: %v", err)
		}
	}()

	sub2, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), "subscription-2", common.NewTestMetrics(t), configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub2.Close(); err != nil {
			t.Logf("failed to close subscriber 2: %v", err)
		}
	}()

	evt := event.New()
	evt.SetType("com.example.test.event")
	evt.SetSource("test-source")
	evt.SetID("test-id")
	if err := evt.SetData(event.ApplicationJSON, map[string]string{"message": "test"}); err != nil {
		require.NoError(t, err, "failed to set event data")
	}

	// Both subscribers should receive the event (fanout behavior)
	received1 := make(chan *event.Event, 1)
	received2 := make(chan *event.Event, 1)

	handler1 := func(ctx context.Context, e *event.Event) error {
		received1 <- e
		return nil
	}

	handler2 := func(ctx context.Context, e *event.Event) error {
		received2 <- e
		return nil
	}

	err = sub1.Subscribe(ctx, topic, handler1)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, topic, handler2)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	err = pub.Publish(ctx, topic, &evt)
	require.NoError(t, err)

	// Both should receive the event
	select {
	case <-received1:
		// Good
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for sub1 to receive event")
	}

	select {
	case <-received2:
		// Good
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for sub2 to receive event")
	}
}

// TestSlowSubscriber tests that a slow subscriber processes fewer messages than a fast one
func TestSlowSubscriber(t *testing.T) {
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	subscriptionID := fmt.Sprintf("slow-subscription-%d", time.Now().UnixNano())
	sub1, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), subscriptionID, common.NewTestMetrics(t), configMap)
	require.NoError(t, err)

	sub2, err := broker.NewSubscriber(logger.NewTestLogger(logger.WithLevel(slog.LevelWarn)), subscriptionID, common.NewTestMetrics(t), configMap)
	require.NoError(t, err)

	common.RunSlowSubscriber(t, configMap, common.BrokerTestConfig{
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 10 * time.Second,
		PublishDelay:   0, // No delay for RabbitMQ
	}, sub1, sub2)
}

// TestErrorSubscriber tests that messages are redistributed when one subscriber fails
func TestErrorSubscriber(t *testing.T) {
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	common.RunErrorSubscriber(t, configMap, common.BrokerTestConfig{
		BrokerType:     "rabbitmq",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 5 * time.Second,
	})
}

// TestCloseWaitsForInFlightMessages tests that Close() waits for in-flight message processing
func TestCloseWaitsForInFlightMessages(t *testing.T) {
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	common.RunCloseWaitsForInFlightMessages(t, configMap, common.BrokerTestConfig{
		BrokerType:     "rabbitmq",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 5 * time.Second,
	})
}

// TestPanicHandler tests that a handler that panics doesn't cause Close() to hang
func TestPanicHandler(t *testing.T) {
	configMap := common.BuildConfigMap("rabbitmq", sharedRabbitMQURL, "")

	common.RunPanicHandler(t, configMap, common.BrokerTestConfig{
		BrokerType:     "rabbitmq",
		SetupSleep:     500 * time.Millisecond,
		ReceiveTimeout: 5 * time.Second,
	})
}
