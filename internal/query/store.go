package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when the requested entity has no data.
var ErrNotFound = errors.New("entity not found")

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

// Position is the last-known location for a single entity.
type Position struct {
	EntityType  string    `json:"entity_type"`
	EntityID    string    `json:"entity_id"`
	EventTS     time.Time `json:"event_ts"`
	Lat         float64   `json:"lat"`
	Lon         float64   `json:"lon"`
	LastEventID string    `json:"last_event_id"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// GetPosition returns the last-known position for the given entity. The
// request context is further bounded by pointReadTimeout to protect against
// pathological plans.
func (s *Store) GetPosition(ctx context.Context, entityType, entityID string) (*Position, error) {
	ctx, cancel := context.WithTimeout(ctx, pointReadTimeout)
	defer cancel()

	var p Position
	err := s.pool.QueryRow(ctx, `
		SELECT entity_type, entity_id, event_ts, lat, lon, last_event_id, updated_at
		FROM entity_positions
		WHERE entity_type = $1 AND entity_id = $2`,
		entityType, entityID,
	).Scan(&p.EntityType, &p.EntityID, &p.EventTS, &p.Lat, &p.Lon, &p.LastEventID, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query entity_positions: %w", err)
	}
	return &p, nil
}

// RawEvent is a single row from raw_events. Attributes is opaque JSON bytes —
// typed as json.RawMessage so the HTTP encoder passes it through as a nested
// JSON object rather than double-encoding it as a string.
type RawEvent struct {
	EventID    string          `json:"event_id"`
	EntityType string          `json:"entity_type"`
	EntityID   string          `json:"entity_id"`
	EventTS    time.Time       `json:"event_ts"`
	Lat        float64         `json:"lat"`
	Lon        float64         `json:"lon"`
	SpeedKmh   *float64        `json:"speed_kmh,omitempty"`
	HeadingDeg *float64        `json:"heading_deg,omitempty"`
	AccuracyM  *float64        `json:"accuracy_m,omitempty"`
	Source     *string         `json:"source,omitempty"`
	Attributes json.RawMessage `json:"attributes,omitempty"`
	IngestedAt time.Time       `json:"ingested_at"`
}

// EventsPage is a paginated list of raw events. NextCursor is the opaque
// token to pass as the ?cursor= query param to fetch the next page, or
// empty if this is the last page.
type EventsPage struct {
	Events     []RawEvent `json:"events"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// ListEvents returns raw events for the entity, ordered by (event_ts DESC,
// event_id DESC). The composite ordering — and the composite cursor — is
// load-bearing: event_ts alone is not unique, so a strict `event_ts < cursor`
// predicate would silently drop any sibling rows sharing the boundary
// microsecond. Row-wise comparison `(event_ts, event_id) < (ts, eid)` with
// the matching ORDER BY serves every row exactly once.
//
// An empty result is returned as an empty slice (not an error) — we cannot
// cheaply distinguish "unknown entity" from "known entity with zero events"
// in a single query, so both collapse to "200 OK, empty list" at the HTTP
// boundary.
func (s *Store) ListEvents(ctx context.Context, entityType, entityID string, cursor Cursor, limit int) (*EventsPage, error) {
	switch {
	case limit <= 0:
		limit = defaultLimit
	case limit > maxLimit:
		limit = maxLimit
	}

	ctx, cancel := context.WithTimeout(ctx, listQueryTimeout)
	defer cancel()

	// Fetch one extra row to detect whether a next page exists.
	fetchLimit := limit + 1

	var rows pgx.Rows
	var err error
	if cursor.IsZero() {
		rows, err = s.pool.Query(ctx, `
			SELECT event_id, entity_type, entity_id, event_ts, lat, lon,
			       speed_kmh, heading_deg, accuracy_m, source, attributes, ingested_at
			FROM raw_events
			WHERE entity_type = $1 AND entity_id = $2
			ORDER BY event_ts DESC, event_id DESC
			LIMIT $3`,
			entityType, entityID, fetchLimit,
		)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT event_id, entity_type, entity_id, event_ts, lat, lon,
			       speed_kmh, heading_deg, accuracy_m, source, attributes, ingested_at
			FROM raw_events
			WHERE entity_type = $1 AND entity_id = $2
			  AND (event_ts, event_id) < ($3, $4)
			ORDER BY event_ts DESC, event_id DESC
			LIMIT $5`,
			entityType, entityID, cursor.EventTS, cursor.EventID, fetchLimit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query raw_events: %w", err)
	}
	defer rows.Close()

	events := make([]RawEvent, 0, fetchLimit)
	for rows.Next() {
		var e RawEvent
		if err := rows.Scan(
			&e.EventID, &e.EntityType, &e.EntityID, &e.EventTS,
			&e.Lat, &e.Lon, &e.SpeedKmh, &e.HeadingDeg, &e.AccuracyM,
			&e.Source, &e.Attributes, &e.IngestedAt,
		); err != nil {
			return nil, fmt.Errorf("scan raw_events: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	page := &EventsPage{Events: events}
	if len(events) > limit {
		page.Events = events[:limit]
		last := events[limit-1]
		page.NextCursor = Cursor{EventTS: last.EventTS, EventID: last.EventID}.Encode()
	}
	return page, nil
}
