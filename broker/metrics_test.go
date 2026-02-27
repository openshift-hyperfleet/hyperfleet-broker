package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRegistry(t *testing.T) (*MetricsRecorder, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := NewMetricsRecorder("test-component", "v0.1.0", reg)
	return m, reg
}

func getCounterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if matchLabels(metric.GetLabel(), labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
	return 0
}

func getHistogramCount(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if matchLabels(metric.GetLabel(), labels) {
				return metric.GetHistogram().GetSampleCount()
			}
		}
	}
	t.Fatalf("histogram %s with labels %v not found", name, labels)
	return 0
}

func matchLabels(metricLabels []*dto.LabelPair, expected map[string]string) bool {
	if len(metricLabels) != len(expected) {
		return false
	}
	for _, lp := range metricLabels {
		v, ok := expected[lp.GetName()]
		if !ok || v != lp.GetValue() {
			return false
		}
	}
	return true
}

func TestNewMetricsRecorderRegistersAllMetrics(t *testing.T) {
	// Trigger one of each metric type so Gather() returns their families.
	m, reg := newTestRegistry(t)
	m.RecordConsumed("test-topic")
	m.RecordPublished("test-topic")
	m.RecordError("test-topic", "test-error")
	m.RecordDuration("test-topic", 100*time.Millisecond)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	names := make(map[string]bool)
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}

	assert.True(t, names["hyperfleet_broker_messages_consumed_total"], "messages_consumed_total should be registered")
	assert.True(t, names["hyperfleet_broker_messages_published_total"], "messages_published_total should be registered")
	assert.True(t, names["hyperfleet_broker_errors_total"], "errors_total should be registered")
	assert.True(t, names["hyperfleet_broker_message_duration_seconds"], "message_duration_seconds should be registered")
}

func TestRecordConsumed(t *testing.T) {
	m, reg := newTestRegistry(t)

	m.RecordConsumed("orders")
	m.RecordConsumed("orders")
	m.RecordConsumed("events")

	labels := map[string]string{"topic": "orders", "component": "test-component", "version": "v0.1.0"}
	assert.Equal(t, float64(2), getCounterValue(t, reg, "hyperfleet_broker_messages_consumed_total", labels))

	labels["topic"] = "events"
	assert.Equal(t, float64(1), getCounterValue(t, reg, "hyperfleet_broker_messages_consumed_total", labels))
}

func TestRecordPublished(t *testing.T) {
	m, reg := newTestRegistry(t)

	m.RecordPublished("orders")
	m.RecordPublished("orders")
	m.RecordPublished("orders")

	labels := map[string]string{"topic": "orders", "component": "test-component", "version": "v0.1.0"}
	assert.Equal(t, float64(3), getCounterValue(t, reg, "hyperfleet_broker_messages_published_total", labels))
}

func TestRecordError(t *testing.T) {
	m, reg := newTestRegistry(t)

	m.RecordError("orders", "handler")
	m.RecordError("orders", "conversion")
	m.RecordError("orders", "handler")

	handlerLabels := map[string]string{"topic": "orders", "error_type": "handler", "component": "test-component", "version": "v0.1.0"}
	assert.Equal(t, float64(2), getCounterValue(t, reg, "hyperfleet_broker_errors_total", handlerLabels))

	conversionLabels := map[string]string{"topic": "orders", "error_type": "conversion", "component": "test-component", "version": "v0.1.0"}
	assert.Equal(t, float64(1), getCounterValue(t, reg, "hyperfleet_broker_errors_total", conversionLabels))
}

func TestRecordDuration(t *testing.T) {
	m, reg := newTestRegistry(t)

	m.RecordDuration("orders", 500*time.Millisecond)
	m.RecordDuration("orders", 1500*time.Millisecond)

	labels := map[string]string{"topic": "orders", "component": "test-component", "version": "v0.1.0"}
	assert.Equal(t, uint64(2), getHistogramCount(t, reg, "hyperfleet_broker_message_duration_seconds", labels))
}

func TestPublisherMetricsIntegration(t *testing.T) {
	mockLogger := logger.NewMockLogger()
	reg := prometheus.NewRegistry()
	metrics := NewMetricsRecorder("test-publisher", "v1.0.0", reg)

	t.Run("successful publish increments published counter", func(t *testing.T) {
		p := &publisher{
			pub:     &fakeWatermillPublisher{},
			logger:  mockLogger,
			metrics: metrics,
		}

		evt := event.New()
		evt.SetID("test-1")
		evt.SetType("test.type")
		evt.SetSource("test-source")

		err := p.Publish(context.Background(), "test-topic", &evt)
		assert.NoError(t, err)

		labels := map[string]string{"topic": "test-topic", "component": "test-publisher", "version": "v1.0.0"}
		assert.Equal(t, float64(1), getCounterValue(t, reg, "hyperfleet_broker_messages_published_total", labels))
	})

	t.Run("failed publish increments error counter", func(t *testing.T) {
		p := &publisher{
			pub:     &failingWatermillPublisher{},
			logger:  mockLogger,
			metrics: metrics,
		}

		evt := event.New()
		evt.SetID("test-2")
		evt.SetType("test.type")
		evt.SetSource("test-source")

		err := p.Publish(context.Background(), "fail-topic", &evt)
		assert.Error(t, err)

		labels := map[string]string{"topic": "fail-topic", "error_type": "publish", "component": "test-publisher", "version": "v1.0.0"}
		assert.Equal(t, float64(1), getCounterValue(t, reg, "hyperfleet_broker_errors_total", labels))
	})
}

// failingWatermillPublisher always returns an error on Publish
type failingWatermillPublisher struct{}

func (f *failingWatermillPublisher) Publish(_ string, _ ...*message.Message) error {
	return fmt.Errorf("publish failed")
}

func (f *failingWatermillPublisher) Close() error {
	return nil
}
