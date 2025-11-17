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
				msg := message.NewMessage("test-uuid", []byte(`{"key": "value"}`))
				msg.Metadata.Set(metadataTypeKey, "com.example.test.event")
				msg.Metadata.Set(metadataSourceKey, "test-source")
				msg.Metadata.Set(metadataIDKey, "test-id-123")
				msg.Metadata.Set(metadataSpecVersionKey, "1.0")
				msg.Metadata.Set(metadataTimeKey, "2023-01-01T12:00:00Z")
				msg.Metadata.Set(metadataSubjectKey, "test-subject")
				msg.Metadata.Set(metadataDataSchemaKey, "http://example.com/schema")
				msg.Metadata.Set(metadataDataContentTypeKey, "application/json")
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

				expectedTime, _ := time.Parse(time.RFC3339, "2023-01-01T12:00:00Z")
				assert.Equal(t, expectedTime, evt.Time())

				// Verify data
				var data map[string]interface{}
				err := json.Unmarshal(evt.Data(), &data)
				require.NoError(t, err)
				assert.Equal(t, "value", data["key"])
			},
		},
		{
			name: "message with minimal required attributes",
			setupMsg: func() *message.Message {
				msg := message.NewMessage("test-uuid", []byte(`{"data": "test"}`))
				msg.Metadata.Set(metadataTypeKey, "com.example.test.event")
				return msg
			},
			expectError: false,
			validate: func(t *testing.T, evt *event.Event) {
				assert.Equal(t, "com.example.test.event", evt.Type())
				assert.Equal(t, "test-uuid", evt.ID())                              // Should use Watermill UUID as fallback
				assert.Equal(t, event.CloudEventsVersionV1, evt.SpecVersion())      // Default version
				assert.Equal(t, cloudevents.ApplicationJSON, evt.DataContentType()) // Default content type
			},
		},
		{
			name: "missing required type attribute",
			setupMsg: func() *message.Message {
				msg := message.NewMessage("test-uuid", []byte(`{"data": "test"}`))
				// Missing type - should error
				return msg
			},
			expectError: true,
			validate:    nil,
		},
		{
			name: "message with invalid JSON data",
			setupMsg: func() *message.Message {
				msg := message.NewMessage("test-uuid", []byte("not valid json"))
				msg.Metadata.Set(metadataTypeKey, "com.example.test.event")
				return msg
			},
			expectError: true,
			validate:    nil,
		},
		{
			name: "message with RFC3339Nano time format",
			setupMsg: func() *message.Message {
				msg := message.NewMessage("test-uuid", []byte(`{}`))
				msg.Metadata.Set(metadataTypeKey, "com.example.test.event")
				msg.Metadata.Set(metadataTimeKey, "2023-01-01T12:00:00.123456789Z")
				return msg
			},
			expectError: false,
			validate: func(t *testing.T, evt *event.Event) {
				assert.Equal(t, "com.example.test.event", evt.Type())
				expectedTime, _ := time.Parse(time.RFC3339Nano, "2023-01-01T12:00:00.123456789Z")
				assert.Equal(t, expectedTime, evt.Time())
			},
		},
		{
			name: "message with invalid time format",
			setupMsg: func() *message.Message {
				msg := message.NewMessage("test-uuid", []byte(`{}`))
				msg.Metadata.Set(metadataTypeKey, "com.example.test.event")
				msg.Metadata.Set(metadataTimeKey, "invalid-time-format")
				return msg
			},
			expectError: false,
			validate: func(t *testing.T, evt *event.Event) {
				assert.Equal(t, "com.example.test.event", evt.Type())
				// Time should be zero if parsing failed
				assert.True(t, evt.Time().IsZero())
			},
		},
		{
			name: "message with empty payload",
			setupMsg: func() *message.Message {
				msg := message.NewMessage("test-uuid", nil)
				msg.Metadata.Set(metadataTypeKey, "com.example.test.event")
				return msg
			},
			expectError: false,
			validate: func(t *testing.T, evt *event.Event) {
				assert.Equal(t, "com.example.test.event", evt.Type())
				assert.Nil(t, evt.Data())
			},
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
				evt.SetData(cloudevents.ApplicationJSON, map[string]string{"key": "value"})
				return &evt
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.Message) {
				assert.Equal(t, "test-id-123", msg.UUID)
				assert.Equal(t, "com.example.test.event", msg.Metadata.Get(metadataTypeKey))
				assert.Equal(t, "test-source", msg.Metadata.Get(metadataSourceKey))
				assert.Equal(t, "test-id-123", msg.Metadata.Get(metadataIDKey))
				assert.Equal(t, "1.0", msg.Metadata.Get(metadataSpecVersionKey))
				assert.Equal(t, "test-subject", msg.Metadata.Get(metadataSubjectKey))
				assert.Equal(t, "http://example.com/schema", msg.Metadata.Get(metadataDataSchemaKey))
				assert.Equal(t, "application/json", msg.Metadata.Get(metadataDataContentTypeKey))

				// Verify time is set
				timeStr := msg.Metadata.Get(metadataTimeKey)
				assert.NotEmpty(t, timeStr)

				// Verify data
				var data map[string]string
				err := json.Unmarshal(msg.Payload, &data)
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
				return &evt
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.Message) {
				assert.Equal(t, "test-id", msg.UUID)
				assert.Equal(t, "com.example.test.event", msg.Metadata.Get(metadataTypeKey))
				assert.Equal(t, "test-id", msg.Metadata.Get(metadataIDKey))
				// Time should not be set if zero
				assert.Empty(t, msg.Metadata.Get(metadataTimeKey))
			},
		},
		{
			name: "event with zero time",
			setupEvent: func() *event.Event {
				evt := event.New()
				evt.SetType("com.example.test.event")
				evt.SetID("test-id")
				// Time is zero by default
				return &evt
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.Message) {
				assert.Equal(t, "com.example.test.event", msg.Metadata.Get(metadataTypeKey))
				// Time metadata should not be set for zero time
				assert.Empty(t, msg.Metadata.Get(metadataTimeKey))
			},
		},
		{
			name: "event with nil data",
			setupEvent: func() *event.Event {
				evt := event.New()
				evt.SetType("com.example.test.event")
				evt.SetID("test-id")
				// Data is nil by default
				return &evt
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.Message) {
				assert.Equal(t, "com.example.test.event", msg.Metadata.Get(metadataTypeKey))
				assert.Nil(t, msg.Payload)
			},
		},
		{
			name: "event with complex data structure",
			setupEvent: func() *event.Event {
				evt := event.New()
				evt.SetType("com.example.test.event")
				evt.SetID("test-id")
				complexData := map[string]interface{}{
					"string":  "value",
					"number":  42,
					"boolean": true,
					"array":   []string{"a", "b", "c"},
					"nested": map[string]interface{}{
						"key": "nested-value",
					},
				}
				evt.SetData(cloudevents.ApplicationJSON, complexData)
				return &evt
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.Message) {
				assert.Equal(t, "com.example.test.event", msg.Metadata.Get(metadataTypeKey))
				var data map[string]interface{}
				err := json.Unmarshal(msg.Payload, &data)
				require.NoError(t, err)
				assert.Equal(t, "value", data["string"])
				assert.Equal(t, float64(42), data["number"]) // JSON numbers are float64
				assert.Equal(t, true, data["boolean"])
			},
		},
		{
			name: "event with empty string data",
			setupEvent: func() *event.Event {
				evt := event.New()
				evt.SetType("com.example.test.event")
				evt.SetID("test-id")
				evt.SetData(cloudevents.ApplicationJSON, "")
				return &evt
			},
			expectError: false,
			validate: func(t *testing.T, msg *message.Message) {
				assert.Equal(t, "com.example.test.event", msg.Metadata.Get(metadataTypeKey))
				assert.Equal(t, `""`, string(msg.Payload)) // Empty string serialized as JSON
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
	originalEvt.SetData(cloudevents.ApplicationJSON, map[string]string{"key": "value"})

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

func TestIsCloudEventsAttribute(t *testing.T) {
	tests := []struct {
		key      string
		expected bool
	}{
		{metadataTypeKey, true},
		{metadataSourceKey, true},
		{metadataIDKey, true},
		{metadataSpecVersionKey, true},
		{metadataTimeKey, true},
		{metadataSubjectKey, true},
		{metadataDataSchemaKey, true},
		{metadataDataContentTypeKey, true},
		{"custom-extension", false},
		{"not-a-cloudevents-attr", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := isCloudEventsAttribute(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}
