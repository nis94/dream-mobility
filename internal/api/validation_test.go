package api

import (
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

func validEvent() EventRequest {
	return EventRequest{
		EventID:   "550e8400-e29b-41d4-a716-446655440000",
		Entity:    EntityField{Type: "vehicle", ID: "vehicle-1"},
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Position:  PositionField{Lat: 52.52, Lon: 13.405},
	}
}

func TestValidateEvent_Valid(t *testing.T) {
	e := validEvent()
	ts, err := ValidateEvent(&e)
	if err != nil {
		t.Errorf("expected valid, got: %v", err)
	}
	if ts.IsZero() {
		t.Error("expected non-zero parsed timestamp on success")
	}
}

func TestValidateEvent_ValidWithOptionals(t *testing.T) {
	e := validEvent()
	e.SpeedKmh = ptr(42.3)
	e.HeadingDeg = ptr(137.5)
	e.AccuracyM = ptr(4.2)
	e.Source = ptr("gps")
	if _, err := ValidateEvent(&e); err != nil {
		t.Errorf("expected valid, got: %v", err)
	}
}

func TestValidateEvent_ReturnedTimestampMatchesInput(t *testing.T) {
	e := validEvent()
	e.Timestamp = "2025-03-15T12:34:56.789Z"
	ts, err := ValidateEvent(&e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Date(2025, 3, 15, 12, 34, 56, 789_000_000, time.UTC)
	if !ts.Equal(want) {
		t.Errorf("ts = %v, want %v", ts, want)
	}
}

func TestValidateEvent_MissingRequired(t *testing.T) {
	tests := []struct {
		name string
		mod  func(*EventRequest)
		want string
	}{
		{"missing event_id", func(e *EventRequest) { e.EventID = "" }, "event_id is required"},
		{"missing entity.type", func(e *EventRequest) { e.Entity.Type = "" }, "entity.type is required"},
		{"missing entity.id", func(e *EventRequest) { e.Entity.ID = "" }, "entity.id is required"},
		{"missing timestamp", func(e *EventRequest) { e.Timestamp = "" }, "timestamp is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEvent()
			tt.mod(&e)
			_, err := ValidateEvent(&e)
			if err == nil || err.Error() != tt.want {
				t.Errorf("got %v, want %q", err, tt.want)
			}
		})
	}
}

func TestValidateEvent_BadTimestamp(t *testing.T) {
	e := validEvent()
	e.Timestamp = "not-a-date"
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for bad timestamp")
	}
}

func TestValidateEvent_FutureTimestamp(t *testing.T) {
	e := validEvent()
	e.Timestamp = time.Now().Add(25 * time.Hour).UTC().Format(time.RFC3339Nano)
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for future timestamp")
	}
}

func TestValidateEvent_LatOutOfRange(t *testing.T) {
	tests := []struct {
		name string
		lat  float64
	}{
		{"too low", -91.0},
		{"too high", 91.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEvent()
			e.Position.Lat = tt.lat
			if _, err := ValidateEvent(&e); err == nil {
				t.Error("expected error for lat out of range")
			}
		})
	}
}

func TestValidateEvent_LonOutOfRange(t *testing.T) {
	e := validEvent()
	e.Position.Lon = 181.0
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for lon out of range")
	}
}

func TestValidateEvent_NegativeSpeed(t *testing.T) {
	e := validEvent()
	e.SpeedKmh = ptr(-1.0)
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for negative speed")
	}
}

func TestValidateEvent_HeadingOutOfRange(t *testing.T) {
	e := validEvent()
	e.HeadingDeg = ptr(360.0)
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for heading >= 360")
	}
}

func TestValidateEvent_NegativeAccuracy(t *testing.T) {
	e := validEvent()
	e.AccuracyM = ptr(-0.5)
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for negative accuracy")
	}
}
