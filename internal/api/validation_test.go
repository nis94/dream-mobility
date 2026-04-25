package api

import (
	"testing"
	"time"
)

// ptr is shared with handler_test.go.

func validEvent() EventRequest {
	return EventRequest{
		EventID:        "550e8400-e29b-41d4-a716-446655440000",
		Icao24:         "abc123",
		OriginCountry:  "GB",
		ObservedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		PositionSource: "ADSB",
		Lat:            52.52,
		Lon:            13.405,
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
	e.Callsign = ptr("BAW123")
	e.BaroAltitudeM = ptr(11000.0)
	e.GeoAltitudeM = ptr(11050.0)
	e.VelocityMs = ptr(245.5)
	e.TrueTrackDeg = ptr(137.5)
	e.VerticalRateMs = ptr(-2.5)
	e.Squawk = ptr("1234")
	e.Category = ptr(5)
	if _, err := ValidateEvent(&e); err != nil {
		t.Errorf("expected valid, got: %v", err)
	}
}

func TestValidateEvent_ReturnedTimestampMatchesInput(t *testing.T) {
	e := validEvent()
	e.ObservedAt = "2025-03-15T12:34:56.789Z"
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
		{"missing icao24", func(e *EventRequest) { e.Icao24 = "" }, "icao24 is required"},
		{"missing origin_country", func(e *EventRequest) { e.OriginCountry = "" }, "origin_country is required"},
		{"missing observed_at", func(e *EventRequest) { e.ObservedAt = "" }, "observed_at is required"},
		{"missing position_source", func(e *EventRequest) { e.PositionSource = "" }, "position_source is required"},
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

func TestValidateEvent_BadIcao24(t *testing.T) {
	e := validEvent()
	e.Icao24 = "ABC123" // uppercase rejected
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for uppercase icao24")
	}
	e.Icao24 = "xyz" // not 6 chars
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for short icao24")
	}
	e.Icao24 = "abc12g" // 'g' is not hex
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for non-hex icao24")
	}
}

func TestValidateEvent_BadPositionSource(t *testing.T) {
	e := validEvent()
	e.PositionSource = "GPS"
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for unknown position_source")
	}
}

func TestValidateEvent_BadTimestamp(t *testing.T) {
	e := validEvent()
	e.ObservedAt = "not-a-date"
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for bad observed_at")
	}
}

func TestValidateEvent_FutureTimestamp(t *testing.T) {
	e := validEvent()
	e.ObservedAt = time.Now().Add(25 * time.Hour).UTC().Format(time.RFC3339Nano)
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for future observed_at")
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
			e.Lat = tt.lat
			if _, err := ValidateEvent(&e); err == nil {
				t.Error("expected error for lat out of range")
			}
		})
	}
}

func TestValidateEvent_LonOutOfRange(t *testing.T) {
	e := validEvent()
	e.Lon = 181.0
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for lon out of range")
	}
}

func TestValidateEvent_NegativeVelocity(t *testing.T) {
	e := validEvent()
	e.VelocityMs = ptr(-1.0)
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for negative velocity")
	}
}

func TestValidateEvent_TrackOutOfRange(t *testing.T) {
	e := validEvent()
	e.TrueTrackDeg = ptr(360.0)
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for true_track_deg >= 360")
	}
}

func TestValidateEvent_BadSquawk(t *testing.T) {
	e := validEvent()
	e.Squawk = ptr("9999") // 9 is not octal
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for non-octal squawk")
	}
	e.Squawk = ptr("123") // 3 chars
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for short squawk")
	}
}

func TestValidateEvent_CategoryOutOfRange(t *testing.T) {
	e := validEvent()
	e.Category = ptr(21)
	if _, err := ValidateEvent(&e); err == nil {
		t.Error("expected error for category > 20")
	}
}
