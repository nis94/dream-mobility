package api

import (
	"errors"
	"fmt"
	"time"
)

const maxFutureWindow = 24 * time.Hour

// ValidateEvent checks required fields and value ranges. On success it returns
// the parsed event timestamp and a nil error. On failure it returns the zero
// time and an error describing the first failure.
//
// Returning the parsed timestamp lets the caller (mapToAvro) skip a redundant
// time.Parse and guarantees the handler and the Avro record agree on the
// exact instant.
func ValidateEvent(e *EventRequest) (time.Time, error) {
	if e.EventID == "" {
		return time.Time{}, errors.New("event_id is required")
	}
	if e.Entity.Type == "" {
		return time.Time{}, errors.New("entity.type is required")
	}
	if e.Entity.ID == "" {
		return time.Time{}, errors.New("entity.id is required")
	}
	if e.Timestamp == "" {
		return time.Time{}, errors.New("timestamp is required")
	}

	ts, err := time.Parse(time.RFC3339Nano, e.Timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("timestamp is not valid RFC3339: %w", err)
	}
	if ts.After(time.Now().Add(maxFutureWindow)) {
		return time.Time{}, errors.New("timestamp is too far in the future (>24h)")
	}

	if e.Position.Lat < -90 || e.Position.Lat > 90 {
		return time.Time{}, fmt.Errorf("position.lat out of range [-90, 90]: %f", e.Position.Lat)
	}
	if e.Position.Lon < -180 || e.Position.Lon > 180 {
		return time.Time{}, fmt.Errorf("position.lon out of range [-180, 180]: %f", e.Position.Lon)
	}

	if e.SpeedKmh != nil && *e.SpeedKmh < 0 {
		return time.Time{}, fmt.Errorf("speed_kmh must be >= 0: %f", *e.SpeedKmh)
	}
	if e.HeadingDeg != nil && (*e.HeadingDeg < 0 || *e.HeadingDeg >= 360) {
		return time.Time{}, fmt.Errorf("heading_deg must be in [0, 360): %f", *e.HeadingDeg)
	}
	if e.AccuracyM != nil && *e.AccuracyM < 0 {
		return time.Time{}, fmt.Errorf("accuracy_m must be >= 0: %f", *e.AccuracyM)
	}

	return ts, nil
}
