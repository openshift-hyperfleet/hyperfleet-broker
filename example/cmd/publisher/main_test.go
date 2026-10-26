package main

import "testing"

func TestCreateEvent(t *testing.T) {
	evt, err := createEvent("event-1", "hello")
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("invalid CloudEvent: %v", err)
	}
	if evt.ID() != "event-1" || evt.Type() != "com.example.event.created" || evt.Source() != "example-publisher" {
		t.Fatalf("unexpected event attributes: %s", evt.String())
	}
	var data struct {
		Message   string `json:"message"`
		Timestamp string `json:"timestamp"`
	}
	if err := evt.DataAs(&data); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
	if data.Message != "hello" || data.Timestamp == "" {
		t.Fatalf("unexpected event data: %+v", data)
	}
}
