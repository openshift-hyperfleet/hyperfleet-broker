package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageToEvent(t *testing.T) {
	tests := []struct {
		name        string
		setupMsg    func() *message.Message
		expectError bool
		validate    func(*testing.T, *event.Event)
	}{
		{
			name: "valid message with all attributes",
			setupMsg: func() *message.Message {
				evt := event.New()
				evt.SetType("com.example.test.event")
				evt.SetSource("test-source")
				evt.SetID("test-id-123")
				evt.SetSpecVersion("1.0")
				evt.SetTime(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC))
				evt.SetSubject("test-subject")
				evt.SetDataSchema("http://example.com/schema")
				evt.SetDataContentType("application/json")
				if err := evt.SetData(cloudevents.ApplicationJSON, map[string]string{"key": "value"}); err != nil {
					panic(err)
				}

				payload, _ := json.Marshal(evt)
				msg := message.NewMessage("test-uuid", payload)
				return msg
			},
			expectError: false,
			validate: func(t *testing.T, evt *event.Event) {
				assert.Equal(t, "com.example.test.event", evt.Type())
				assert.Equal(t, "test-source", evt.Source())
				assert.Equal(t, "test-id-123", evt.ID())
				assert.Equal(t, "1.0", evt.SpecVersion())
				assert.Equal(t, "test-subject", evt.Subject())
				assert.Equal(t, "http://example.com/schema", evt.DataSchema())
				assert.Equal(t, "application/json", evt.DataContentType())

				expectedTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
				assert.Equal(t, expectedTime, evt.Time())

				// Verify data
				var data map[string]interface{}
				err := json.Unmarshal(evt.Data(), &data)
				require.NoError(t, err)
				assert.Equal(t, "value", data["key"])
			},
		},
		{
			name: "message with invalid JSON payload",
			setupMsg: func() *message.Message {
				msg := message.NewMessage("test-uuid", []byte("not valid json"))
				return msg
			},
			expectError: true,
			validate:    nil,
		},
		{
			name: "message with empty payload",
			setupMsg: func() *message.Message {
				msg := message.NewMessage("test-uuid", []byte{})
				return msg
			},
			expectError: true, // unmarshal empty bytes usually fails or returns empty struct which might be invalid CloudEvent if required fields missing
			validate:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.setupMsg()
			evt, err := messageToEvent(msg)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, evt)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, evt)
				if tt.validate != nil {
					tt.validate(t, evt)
				}
			}
		})
	}
}

func TestEventToMessage(t *testing.T) {
	tests := []struct {
		name        string
		setupEvent  func() *event.Event
		expectError bool
		validate    func(*testing.T, *message.Message)
	}{
		{
			name: "valid event with all attributes",
			setupEvent: func() *event.Event {
				evt := event.New()
				evt.SetType("com.example.test.event")
				evt.SetSource("test-source")
				evt.SetID("test-id-123")
				evt.SetSpecVersion("1.0")
				evt.SetTime(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC))
				evt.SetSubject("test-subject")
				evt.SetDataSchema("http://example.com/schema")
				evt.SetDataContentType("application/json")
				if err := evt.SetData(cloudevents.ApplicationJSON, map[string]string{"key": "value"}); err != nil {
					panic(err)
				}
				return &evt
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.Message) {
				assert.Equal(t, "test-id-123", msg.UUID)
				
				// Payload should be a JSON representation of the event
				var evt event.Event
				err := json.Unmarshal(msg.Payload, &evt)
				require.NoError(t, err)

				assert.Equal(t, "com.example.test.event", evt.Type())
				assert.Equal(t, "test-source", evt.Source())
				assert.Equal(t, "test-id-123", evt.ID())
				
				// Verify data inside the unmarshaled event
				var data map[string]string
				err = json.Unmarshal(evt.Data(), &data)
				require.NoError(t, err)
				assert.Equal(t, "value", data["key"])
			},
		},
		{
			name: "event with minimal attributes",
			setupEvent: func() *event.Event {
				evt := event.New()
				evt.SetType("com.example.test.event")
				evt.SetID("test-id")
				evt.SetSource("test-source") // source is required for valid CE
				return &evt
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.Message) {
				assert.Equal(t, "test-id", msg.UUID)
				
				var evt event.Event
				err := json.Unmarshal(msg.Payload, &evt)
				require.NoError(t, err)
				assert.Equal(t, "com.example.test.event", evt.Type())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := tt.setupEvent()
			msg, err := eventToMessage(evt)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, msg)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, msg)
				if tt.validate != nil {
					tt.validate(t, msg)
				}
			}
		})
	}
}

func TestRoundTripConversion(t *testing.T) {
	// Test that converting Event -> Message -> Event preserves data
	originalEvt := event.New()
	originalEvt.SetType("com.example.test.event")
	originalEvt.SetSource("test-source")
	originalEvt.SetID("test-id-123")
	originalEvt.SetSpecVersion("1.0")
	originalEvt.SetTime(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC))
	originalEvt.SetSubject("test-subject")
	originalEvt.SetDataSchema("http://example.com/schema")
	originalEvt.SetDataContentType("application/json")
	if err := originalEvt.SetData(cloudevents.ApplicationJSON, map[string]string{"key": "value"}); err != nil {
		require.NoError(t, err, "failed to set event data")
	}

	// Convert to message
	msg, err := eventToMessage(&originalEvt)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Convert back to event
	convertedEvt, err := messageToEvent(msg)
	require.NoError(t, err)
	require.NotNil(t, convertedEvt)

	// Verify all attributes are preserved
	assert.Equal(t, originalEvt.Type(), convertedEvt.Type())
	assert.Equal(t, originalEvt.Source(), convertedEvt.Source())
	assert.Equal(t, originalEvt.ID(), convertedEvt.ID())
	assert.Equal(t, originalEvt.SpecVersion(), convertedEvt.SpecVersion())
	assert.Equal(t, originalEvt.Subject(), convertedEvt.Subject())
	assert.Equal(t, originalEvt.DataSchema(), convertedEvt.DataSchema())
	assert.Equal(t, originalEvt.DataContentType(), convertedEvt.DataContentType())
	assert.Equal(t, originalEvt.Time(), convertedEvt.Time())

	// Verify data
	var originalData map[string]string
	err = json.Unmarshal(originalEvt.Data(), &originalData)
	require.NoError(t, err)

	var convertedData map[string]string
	err = json.Unmarshal(convertedEvt.Data(), &convertedData)
	require.NoError(t, err)

	assert.Equal(t, originalData, convertedData)
}
