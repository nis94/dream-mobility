package api

import (
	"testing"
	"time"
)

func TestParseRequestBody_SingleEvent(t *testing.T) {
	body := `{
		"event_id": "abc-123",
		"entity": {"type": "vehicle", "id": "v1"},
		"timestamp": "2025-01-01T10:00:00Z",
		"position": {"lat": 52.52, "lon": 13.405}
	}`
	events, err := parseEventsFromBytes([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventID != "abc-123" {
		t.Errorf("event_id = %q, want %q", events[0].EventID, "abc-123")
	}
}

func TestParseRequestBody_BatchWrapper(t *testing.T) {
	body := `{"events": [
		{"event_id": "a", "entity": {"type": "v", "id": "1"}, "timestamp": "2025-01-01T10:00:00Z", "position": {"lat": 0, "lon": 0}},
		{"event_id": "b", "entity": {"type": "v", "id": "2"}, "timestamp": "2025-01-01T10:00:00Z", "position": {"lat": 1, "lon": 1}}
	]}`
	events, err := parseEventsFromBytes([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestParseRequestBody_RawArray(t *testing.T) {
	body := `[
		{"event_id": "a", "entity": {"type": "v", "id": "1"}, "timestamp": "2025-01-01T10:00:00Z", "position": {"lat": 0, "lon": 0}}
	]`
	events, err := parseEventsFromBytes([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

func TestParseRequestBody_EmptyBody(t *testing.T) {
	_, err := parseEventsFromBytes([]byte(""))
	if err == nil {
		t.Error("expected error for empty body")
	}
}

func TestMapToAvro(t *testing.T) {
	ev := EventRequest{
		EventID:   "abc-123",
		Entity:    EntityField{Type: "vehicle", ID: "v1"},
		Timestamp: "2025-01-01T10:15:00Z",
		Position:  PositionField{Lat: 52.52, Lon: 13.405},
		SpeedKmh:  ptr(42.3),
		Source:    ptr("gps"),
	}

	m := mapToAvro(&ev)
	if m.EventID != "abc-123" {
		t.Errorf("EventID = %q, want %q", m.EventID, "abc-123")
	}
	if m.EntityType != "vehicle" {
		t.Errorf("EntityType = %q, want %q", m.EntityType, "vehicle")
	}
	if m.EntityID != "v1" {
		t.Errorf("EntityID = %q, want %q", m.EntityID, "v1")
	}
	if m.Lat != 52.52 || m.Lon != 13.405 {
		t.Errorf("position = (%f, %f), want (52.52, 13.405)", m.Lat, m.Lon)
	}
	if m.SpeedKmh == nil || *m.SpeedKmh != 42.3 {
		t.Errorf("SpeedKmh = %v, want 42.3", m.SpeedKmh)
	}
	if m.Source == nil || *m.Source != "gps" {
		t.Errorf("Source = %v, want gps", m.Source)
	}

	wantTs := time.Date(2025, 1, 1, 10, 15, 0, 0, time.UTC)
	if !m.Timestamp.Equal(wantTs) {
		t.Errorf("Timestamp = %v, want %v", m.Timestamp, wantTs)
	}
}

func TestMapToAvro_Attributes(t *testing.T) {
	ev := EventRequest{
		EventID:    "abc-123",
		Entity:     EntityField{Type: "vehicle", ID: "v1"},
		Timestamp:  "2025-01-01T10:15:00Z",
		Position:   PositionField{Lat: 52.52, Lon: 13.405},
		Attributes: []byte(`{"battery_level":0.82}`),
	}

	m := mapToAvro(&ev)
	if m.Attributes == nil {
		t.Fatal("expected non-nil Attributes")
	}
	if *m.Attributes != `{"battery_level":0.82}` {
		t.Errorf("Attributes = %q, want %q", *m.Attributes, `{"battery_level":0.82}`)
	}
}
