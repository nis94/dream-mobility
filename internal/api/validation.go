package api

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

const maxFutureWindow = 24 * time.Hour

// icao24 is 6-char lowercase hex. OpenSky uppercases occasionally; we normalize
// at validation time so the canonical form on the wire is always lowercase.
var icao24Pattern = regexp.MustCompile(`^[0-9a-f]{6}$`)

// squawk is a 4-digit OCTAL code (digits 0-7 only). 7500/7600/7700 are special
// emergency codes the analytics surface specifically.
var squawkPattern = regexp.MustCompile(`^[0-7]{4}$`)

// validPositionSources is the set of position_source values the schema accepts.
// Mirrors the Avro enum symbols.
var validPositionSources = map[string]struct{}{
	"ADSB":    {},
	"ASTERIX": {},
	"MLAT":    {},
	"FLARM":   {},
	"UNKNOWN": {},
}

// ValidateEvent checks required fields and value ranges. On success it returns
// the parsed observed_at timestamp and a nil error. On failure it returns the
// zero time and an error describing the first failure.
//
// Returning the parsed timestamp lets the caller (mapToAvro) skip a redundant
// time.Parse and guarantees the handler and the Avro record agree on the
// exact instant.
func ValidateEvent(e *EventRequest) (time.Time, error) {
	if e.EventID == "" {
		return time.Time{}, errors.New("event_id is required")
	}
	if e.Icao24 == "" {
		return time.Time{}, errors.New("icao24 is required")
	}
	if !icao24Pattern.MatchString(e.Icao24) {
		return time.Time{}, fmt.Errorf("icao24 must be 6 lowercase hex chars: %q", e.Icao24)
	}
	if e.OriginCountry == "" {
		return time.Time{}, errors.New("origin_country is required")
	}
	if e.ObservedAt == "" {
		return time.Time{}, errors.New("observed_at is required")
	}
	if e.PositionSource == "" {
		return time.Time{}, errors.New("position_source is required")
	}
	if _, ok := validPositionSources[e.PositionSource]; !ok {
		return time.Time{}, fmt.Errorf("position_source %q is not one of ADSB, ASTERIX, MLAT, FLARM, UNKNOWN", e.PositionSource)
	}

	ts, err := time.Parse(time.RFC3339Nano, e.ObservedAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("observed_at is not valid RFC3339: %w", err)
	}
	if ts.After(time.Now().Add(maxFutureWindow)) {
		return time.Time{}, errors.New("observed_at is too far in the future (>24h)")
	}

	if e.Lat < -90 || e.Lat > 90 {
		return time.Time{}, fmt.Errorf("lat out of range [-90, 90]: %f", e.Lat)
	}
	if e.Lon < -180 || e.Lon > 180 {
		return time.Time{}, fmt.Errorf("lon out of range [-180, 180]: %f", e.Lon)
	}

	// velocity must be non-negative; ground speed can't be negative even at
	// the point an aircraft reverses on a runway pushback.
	if e.VelocityMs != nil && *e.VelocityMs < 0 {
		return time.Time{}, fmt.Errorf("velocity_ms must be >= 0: %f", *e.VelocityMs)
	}
	if e.TrueTrackDeg != nil && (*e.TrueTrackDeg < 0 || *e.TrueTrackDeg >= 360) {
		return time.Time{}, fmt.Errorf("true_track_deg must be in [0, 360): %f", *e.TrueTrackDeg)
	}
	if e.Squawk != nil && *e.Squawk != "" && !squawkPattern.MatchString(*e.Squawk) {
		return time.Time{}, fmt.Errorf("squawk must be 4 octal digits (0-7): %q", *e.Squawk)
	}
	if e.Category != nil && (*e.Category < 0 || *e.Category > 20) {
		return time.Time{}, fmt.Errorf("category out of OpenSky range [0, 20]: %d", *e.Category)
	}

	return ts, nil
}
