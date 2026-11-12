package eventlog_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/testutil"
)

func TestPublisherSubscriber(t *testing.T) {
	testutil.RunPublisherSubscriber(t, postgresConfigMap(sharedPostgresDSN), postgresScenarioConfig(10*time.Second))
}

func TestMultipleEvents(t *testing.T) {
	testutil.RunMultipleEvents(t, postgresConfigMap(sharedPostgresDSN), postgresScenarioConfig(10*time.Second))
}

func TestSharedSubscription(t *testing.T) {
	testutil.RunSharedSubscription(t, postgresConfigMap(sharedPostgresDSN), postgresScenarioConfig(20*time.Second))
}

func TestFanoutSubscription(t *testing.T) {
	testutil.RunFanoutSubscription(t, postgresConfigMap(sharedPostgresDSN), postgresScenarioConfig(20*time.Second))
}

func TestSlowSubscriber(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)

	subscriptionID := fmt.Sprintf("slow-subscription-%d", time.Now().UnixNano())
	sub1, err := broker.NewSubscriber(testutil.NewTestLogger(), subscriptionID, testutil.NewTestMetrics(t), configMap)
	if err != nil {
		t.Fatalf("failed to create subscriber 1: %v", err)
	}
	sub2, err := broker.NewSubscriber(testutil.NewTestLogger(), subscriptionID, testutil.NewTestMetrics(t), configMap)
	if err != nil {
		t.Fatalf("failed to create subscriber 2: %v", err)
	}

	testutil.RunSlowSubscriber(t, configMap, testutil.BrokerTestConfig{
		BrokerType:     "postgres",
		SetupSleep:     eventLogSetupSleep,
		ReceiveTimeout: 30 * time.Second,
		PublishDelay:   50 * time.Millisecond,
	}, sub1, sub2)
}

func TestErrorSubscriber(t *testing.T) {
	testutil.RunErrorSubscriber(t, postgresConfigMap(sharedPostgresDSN), postgresScenarioConfig(15*time.Second))
}

func TestCloseWaitsForInFlightMessages(t *testing.T) {
	configMap := postgresConfigMap(sharedPostgresDSN)
	// One event per range and enough workers that all published messages
	// are in flight together, matching the shared scenario's parallelism.
	configMap["broker.postgres.batch_size"] = "1"
	configMap["subscriber.parallelism"] = "6"

	testutil.RunCloseWaitsForInFlightMessages(t, configMap, testutil.BrokerTestConfig{
		BrokerType:     "postgres",
		SetupSleep:     eventLogSetupSleep,
		ReceiveTimeout: 20 * time.Second,
	})
}

func TestPanicHandler(t *testing.T) {
	testutil.RunPanicHandler(t, postgresConfigMap(sharedPostgresDSN), postgresScenarioConfig(15*time.Second))
}

func TestErrorChannelNotification(t *testing.T) {
	testutil.RunErrorChannelNotification(t, postgresConfigMap(sharedPostgresDSN), postgresScenarioConfig(10*time.Second))
}
