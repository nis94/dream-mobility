package api

import (
	"fmt"
	"time"
)

const maxFutureWindow = 24 * time.Hour

// ValidateEvent checks required fields and value ranges.
// Returns a human-readable error message, or "" if valid.
func ValidateEvent(e *EventRequest) string {
	if e.EventID == "" {
		return "event_id is required"
	}
	if e.Entity.Type == "" {
		return "entity.type is required"
	}
	if e.Entity.ID == "" {
		return "entity.id is required"
	}
	if e.Timestamp == "" {
		return "timestamp is required"
	}

	ts, err := time.Parse(time.RFC3339Nano, e.Timestamp)
	if err != nil {
		return fmt.Sprintf("timestamp is not valid RFC3339: %v", err)
	}
	if ts.After(time.Now().Add(maxFutureWindow)) {
		return "timestamp is too far in the future (>24h)"
	}

	if e.Position.Lat < -90 || e.Position.Lat > 90 {
		return fmt.Sprintf("position.lat out of range [-90, 90]: %f", e.Position.Lat)
	}
	if e.Position.Lon < -180 || e.Position.Lon > 180 {
		return fmt.Sprintf("position.lon out of range [-180, 180]: %f", e.Position.Lon)
	}

	if e.SpeedKmh != nil && *e.SpeedKmh < 0 {
		return fmt.Sprintf("speed_kmh must be >= 0: %f", *e.SpeedKmh)
	}
	if e.HeadingDeg != nil && (*e.HeadingDeg < 0 || *e.HeadingDeg >= 360) {
		return fmt.Sprintf("heading_deg must be in [0, 360): %f", *e.HeadingDeg)
	}
	if e.AccuracyM != nil && *e.AccuracyM < 0 {
		return fmt.Sprintf("accuracy_m must be >= 0: %f", *e.AccuracyM)
	}

	return ""
}
