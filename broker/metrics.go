package broker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricsRecorder holds Prometheus metrics for the broker library.
type MetricsRecorder struct {
	component string
	version   string

	messagesConsumed  *prometheus.CounterVec
	messagesPublished *prometheus.CounterVec
	errors            *prometheus.CounterVec
	messageDuration   *prometheus.HistogramVec
}

// NewMetricsRecorder creates a new MetricsRecorder and registers all metrics
// with the provided prometheus.Registerer. If registerer is nil, the default
// prometheus.DefaultRegisterer is used.
//
// The component and version parameters are used as constant label values
// on all metrics, per the HyperFleet Metrics Standard.
func NewMetricsRecorder(component, version string, registerer prometheus.Registerer) *MetricsRecorder {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	m := &MetricsRecorder{
		component: component,
		version:   version,

		messagesConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hyperfleet_broker_messages_consumed_total",
			Help: "Total number of messages consumed from the broker.",
		}, []string{"topic", "component", "version"}),

		messagesPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hyperfleet_broker_messages_published_total",
			Help: "Total number of messages published to the broker.",
		}, []string{"topic", "component", "version"}),

		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hyperfleet_broker_errors_total",
			Help: "Total number of message processing errors.",
		}, []string{"topic", "error_type", "component", "version"}),

		messageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "hyperfleet_broker_message_duration_seconds",
			Help: "Duration of message processing in seconds.",
			// Event processing buckets per HyperFleet Metrics Standard
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		}, []string{"topic", "component", "version"}),
	}

	registerer.MustRegister(
		m.messagesConsumed,
		m.messagesPublished,
		m.errors,
		m.messageDuration,
	)

	return m
}

// RecordConsumed increments the messages consumed counter for the given topic.
func (m *MetricsRecorder) RecordConsumed(topic string) {
	m.messagesConsumed.WithLabelValues(topic, m.component, m.version).Inc()
}

// RecordPublished increments the messages published counter for the given topic.
func (m *MetricsRecorder) RecordPublished(topic string) {
	m.messagesPublished.WithLabelValues(topic, m.component, m.version).Inc()
}

// RecordError increments the errors counter for the given topic and error type.
func (m *MetricsRecorder) RecordError(topic, errorType string) {
	m.errors.WithLabelValues(topic, errorType, m.component, m.version).Inc()
}

// RecordDuration observes the message processing duration for the given topic.
func (m *MetricsRecorder) RecordDuration(topic string, duration time.Duration) {
	m.messageDuration.WithLabelValues(topic, m.component, m.version).Observe(duration.Seconds())
}
