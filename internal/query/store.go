package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when the requested aircraft has no data.
var ErrNotFound = errors.New("aircraft not found")

// Pool defaults tuned for a read-only query workload. Heavier on MaxConns
// than the writer pool because request concurrency dominates, lighter on
// lifetime because connections rotate naturally under load. Fields are
// applied only when the DSN did not already set them, so pool_* DSN params
// still win.
const (
	poolMaxConns        = 20
	poolMinConns        = 2
	poolMaxConnLifetime = 30 * time.Minute
	poolMaxConnIdleTime = 5 * time.Minute
	poolHealthCheck     = 30 * time.Second
)

// Per-query timeouts. A point read is bounded tight; the paginated listing
// gets more headroom because large limit values can legitimately need a few
// hundred ms on cold index pages.
const (
	pointReadTimeout = 5 * time.Second
	listQueryTimeout = 10 * time.Second
)

// defaultLimit and maxLimit bound the listing page size. The handler validates
// first and returns HTTP 400 on violation; the store clamps as defense in
// depth so a direct caller cannot request an unbounded page.
const (
	defaultLimit = 100
	maxLimit     = 1000
)

// activeWindow bounds the "active aircraft" lookup. Anything older than this
// is considered stale (signal lost / aircraft on the ground at a small
// airport with no reporter coverage). 5 minutes is comfortably wider than
// OpenSky's 300s anonymous polling interval.
const activeWindow = 5 * time.Minute

// Store provides read-only Postgres queries for the Query API.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a query Store from a DSN with bounded pool config.
func NewStore(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = poolMaxConns
	}
	if cfg.MinConns == 0 {
		cfg.MinConns = poolMinConns
	}
	if cfg.MaxConnLifetime == 0 {
		cfg.MaxConnLifetime = poolMaxConnLifetime
	}
	if cfg.MaxConnIdleTime == 0 {
		cfg.MaxConnIdleTime = poolMaxConnIdleTime
	}
	if cfg.HealthCheckPeriod == 0 {
		cfg.HealthCheckPeriod = poolHealthCheck
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

// AircraftState is the last-known state for a single aircraft (one row of
// aircraft_state). Embedded in the aviation query endpoints.
type AircraftState struct {
	Icao24        string    `json:"icao24"`
	LastCallsign  *string   `json:"last_callsign,omitempty"`
	OriginCountry string    `json:"origin_country"`
	ObservedAt    time.Time `json:"observed_at"`
	Lat           float64   `json:"lat"`
	Lon           float64   `json:"lon"`
	BaroAltitudeM *float64  `json:"baro_altitude_m,omitempty"`
	VelocityMs    *float64  `json:"velocity_ms,omitempty"`
	OnGround      bool      `json:"on_ground"`
	LastSquawk    *string   `json:"last_squawk,omitempty"`
	LastEventID   string    `json:"last_event_id"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// GetAircraft returns the last-known state for the given icao24.
func (s *Store) GetAircraft(ctx context.Context, icao24 string) (*AircraftState, error) {
	ctx, cancel := context.WithTimeout(ctx, pointReadTimeout)
	defer cancel()

	var a AircraftState
	err := s.pool.QueryRow(ctx, `
		SELECT icao24, last_callsign, origin_country, observed_at,
		       lat, lon, baro_altitude_m, velocity_ms, on_ground,
		       last_squawk, last_event_id, updated_at
		FROM aircraft_state
		WHERE icao24 = $1`,
		icao24,
	).Scan(
		&a.Icao24, &a.LastCallsign, &a.OriginCountry, &a.ObservedAt,
		&a.Lat, &a.Lon, &a.BaroAltitudeM, &a.VelocityMs, &a.OnGround,
		&a.LastSquawk, &a.LastEventID, &a.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query aircraft_state: %w", err)
	}
	return &a, nil
}

// ActiveAircraftPage is a paginated list of currently active aircraft.
type ActiveAircraftPage struct {
	Aircraft   []AircraftState `json:"aircraft"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

// ListActive returns aircraft whose latest observation is within activeWindow.
// Optional originCountry filter narrows the scan to a 2-letter country code
// (matching the format OpenSky returns — typically a name like "United Kingdom",
// not an ISO code; the producer normalizes whichever it gets).
func (s *Store) ListActive(ctx context.Context, originCountry string, cursor Cursor, limit int) (*ActiveAircraftPage, error) {
	switch {
	case limit <= 0:
		limit = defaultLimit
	case limit > maxLimit:
		limit = maxLimit
	}

	ctx, cancel := context.WithTimeout(ctx, listQueryTimeout)
	defer cancel()

	cutoff := time.Now().UTC().Add(-activeWindow)
	fetchLimit := limit + 1

	var rows pgx.Rows
	var err error
	switch {
	case originCountry != "" && cursor.IsZero():
		rows, err = s.pool.Query(ctx, `
			SELECT icao24, last_callsign, origin_country, observed_at,
			       lat, lon, baro_altitude_m, velocity_ms, on_ground,
			       last_squawk, last_event_id, updated_at
			FROM aircraft_state
			WHERE origin_country = $1 AND observed_at >= $2
			ORDER BY observed_at DESC, icao24 DESC
			LIMIT $3`,
			originCountry, cutoff, fetchLimit,
		)
	case originCountry != "":
		rows, err = s.pool.Query(ctx, `
			SELECT icao24, last_callsign, origin_country, observed_at,
			       lat, lon, baro_altitude_m, velocity_ms, on_ground,
			       last_squawk, last_event_id, updated_at
			FROM aircraft_state
			WHERE origin_country = $1 AND observed_at >= $2
			  AND (observed_at, icao24) < ($3, $4)
			ORDER BY observed_at DESC, icao24 DESC
			LIMIT $5`,
			originCountry, cutoff, cursor.EventTS, cursor.EventID, fetchLimit,
		)
	case cursor.IsZero():
		rows, err = s.pool.Query(ctx, `
			SELECT icao24, last_callsign, origin_country, observed_at,
			       lat, lon, baro_altitude_m, velocity_ms, on_ground,
			       last_squawk, last_event_id, updated_at
			FROM aircraft_state
			WHERE observed_at >= $1
			ORDER BY observed_at DESC, icao24 DESC
			LIMIT $2`,
			cutoff, fetchLimit,
		)
	default:
		rows, err = s.pool.Query(ctx, `
			SELECT icao24, last_callsign, origin_country, observed_at,
			       lat, lon, baro_altitude_m, velocity_ms, on_ground,
			       last_squawk, last_event_id, updated_at
			FROM aircraft_state
			WHERE observed_at >= $1
			  AND (observed_at, icao24) < ($2, $3)
			ORDER BY observed_at DESC, icao24 DESC
			LIMIT $4`,
			cutoff, cursor.EventTS, cursor.EventID, fetchLimit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query aircraft_state list: %w", err)
	}
	defer rows.Close()

	out := make([]AircraftState, 0, fetchLimit)
	for rows.Next() {
		var a AircraftState
		if err := rows.Scan(
			&a.Icao24, &a.LastCallsign, &a.OriginCountry, &a.ObservedAt,
			&a.Lat, &a.Lon, &a.BaroAltitudeM, &a.VelocityMs, &a.OnGround,
			&a.LastSquawk, &a.LastEventID, &a.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan aircraft_state: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	page := &ActiveAircraftPage{Aircraft: out}
	if len(out) > limit {
		page.Aircraft = out[:limit]
		last := out[limit-1]
		page.NextCursor = Cursor{EventTS: last.ObservedAt, EventID: last.Icao24}.Encode()
	}
	return page, nil
}

// Telemetry is a single observation row from flight_telemetry — the full
// per-emission state vector. Used by the per-aircraft trajectory endpoint.
type Telemetry struct {
	EventID        string    `json:"event_id"`
	Icao24         string    `json:"icao24"`
	Callsign       *string   `json:"callsign,omitempty"`
	OriginCountry  string    `json:"origin_country"`
	ObservedAt     time.Time `json:"observed_at"`
	PositionSource string    `json:"position_source"`
	Lat            float64   `json:"lat"`
	Lon            float64   `json:"lon"`
	BaroAltitudeM  *float64  `json:"baro_altitude_m,omitempty"`
	GeoAltitudeM   *float64  `json:"geo_altitude_m,omitempty"`
	VelocityMs     *float64  `json:"velocity_ms,omitempty"`
	TrueTrackDeg   *float64  `json:"true_track_deg,omitempty"`
	VerticalRateMs *float64  `json:"vertical_rate_ms,omitempty"`
	OnGround       bool      `json:"on_ground"`
	Squawk         *string   `json:"squawk,omitempty"`
	Spi            bool      `json:"spi"`
	Category       *int16    `json:"category,omitempty"`
	IngestedAt     time.Time `json:"ingested_at"`
}

// TrackPage is a paginated trajectory for a single aircraft.
type TrackPage struct {
	Telemetry  []Telemetry `json:"telemetry"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

// GetTrack returns the per-observation trajectory for the given icao24.
// Cursor is composite (observed_at, event_id) so duplicate timestamps within
// the same icao24 are served exactly once.
//
// An empty result is "200 OK, empty list" — distinguishing "unknown aircraft"
// from "known aircraft with no observations in this window" would need a
// second query, and the HTTP boundary doesn't make that distinction useful.
func (s *Store) GetTrack(ctx context.Context, icao24 string, cursor Cursor, limit int) (*TrackPage, error) {
	switch {
	case limit <= 0:
		limit = defaultLimit
	case limit > maxLimit:
		limit = maxLimit
	}

	ctx, cancel := context.WithTimeout(ctx, listQueryTimeout)
	defer cancel()

	fetchLimit := limit + 1

	var rows pgx.Rows
	var err error
	if cursor.IsZero() {
		rows, err = s.pool.Query(ctx, `
			SELECT event_id, icao24, callsign, origin_country, observed_at, position_source,
			       lat, lon, baro_altitude_m, geo_altitude_m, velocity_ms, true_track_deg,
			       vertical_rate_ms, on_ground, squawk, spi, category, ingested_at
			FROM flight_telemetry
			WHERE icao24 = $1
			ORDER BY observed_at DESC, event_id DESC
			LIMIT $2`,
			icao24, fetchLimit,
		)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT event_id, icao24, callsign, origin_country, observed_at, position_source,
			       lat, lon, baro_altitude_m, geo_altitude_m, velocity_ms, true_track_deg,
			       vertical_rate_ms, on_ground, squawk, spi, category, ingested_at
			FROM flight_telemetry
			WHERE icao24 = $1
			  AND (observed_at, event_id) < ($2, $3)
			ORDER BY observed_at DESC, event_id DESC
			LIMIT $4`,
			icao24, cursor.EventTS, cursor.EventID, fetchLimit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query flight_telemetry: %w", err)
	}
	defer rows.Close()

	out := make([]Telemetry, 0, fetchLimit)
	for rows.Next() {
		var t Telemetry
		if err := rows.Scan(
			&t.EventID, &t.Icao24, &t.Callsign, &t.OriginCountry, &t.ObservedAt, &t.PositionSource,
			&t.Lat, &t.Lon, &t.BaroAltitudeM, &t.GeoAltitudeM, &t.VelocityMs, &t.TrueTrackDeg,
			&t.VerticalRateMs, &t.OnGround, &t.Squawk, &t.Spi, &t.Category, &t.IngestedAt,
		); err != nil {
			return nil, fmt.Errorf("scan flight_telemetry: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	page := &TrackPage{Telemetry: out}
	if len(out) > limit {
		page.Telemetry = out[:limit]
		last := out[limit-1]
		page.NextCursor = Cursor{EventTS: last.ObservedAt, EventID: last.EventID}.Encode()
	}
	return page, nil
}
