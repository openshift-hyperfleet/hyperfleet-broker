package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPublisher_BrokerType(t *testing.T) {
	tests := []struct {
		brokerType string
	}{
		{"rabbitmq"},
		{"googlepubsub"},
		{""},
	}
	for _, tt := range tests {
		p := &publisher{brokerType: tt.brokerType}
		assert.Equal(t, tt.brokerType, p.BrokerType())
	}
}

func TestSubscriber_BrokerType(t *testing.T) {
	tests := []struct {
		brokerType string
	}{
		{"rabbitmq"},
		{"googlepubsub"},
		{""},
	}
	for _, tt := range tests {
		s := &subscriber{brokerType: tt.brokerType}
		assert.Equal(t, tt.brokerType, s.BrokerType())
	}
}
