package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when the requested entity has no data.
var ErrNotFound = errors.New("entity not found")

// Store provides read-only Postgres queries for the Query API.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a query Store from a DSN.
func NewStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
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

// GetPosition returns the last-known position for the given entity.
func (s *Store) GetPosition(ctx context.Context, entityType, entityID string) (*Position, error) {
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

// RawEvent is a single row from raw_events.
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
	Attributes *string         `json:"attributes,omitempty"`
	IngestedAt time.Time       `json:"ingested_at"`
}

// EventsPage is a paginated list of raw events.
type EventsPage struct {
	Events     []RawEvent `json:"events"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// ListEvents returns raw events for the entity, ordered by event_ts DESC,
// with cursor-based pagination.
//
// Cursor is the event_ts of the last event on the previous page (RFC3339Nano).
// An empty cursor starts from the most recent event. Limit caps the page size.
func (s *Store) ListEvents(ctx context.Context, entityType, entityID string, cursor time.Time, limit int) (*EventsPage, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	// Fetch one extra row to determine if there's a next page.
	fetchLimit := limit + 1

	var rows pgx.Rows
	var err error
	if cursor.IsZero() {
		rows, err = s.pool.Query(ctx, `
			SELECT event_id, entity_type, entity_id, event_ts, lat, lon,
			       speed_kmh, heading_deg, accuracy_m, source, attributes::text, ingested_at
			FROM raw_events
			WHERE entity_type = $1 AND entity_id = $2
			ORDER BY event_ts DESC
			LIMIT $3`,
			entityType, entityID, fetchLimit,
		)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT event_id, entity_type, entity_id, event_ts, lat, lon,
			       speed_kmh, heading_deg, accuracy_m, source, attributes::text, ingested_at
			FROM raw_events
			WHERE entity_type = $1 AND entity_id = $2 AND event_ts < $3
			ORDER BY event_ts DESC
			LIMIT $4`,
			entityType, entityID, cursor, fetchLimit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query raw_events: %w", err)
	}
	defer rows.Close()

	var events []RawEvent
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

	// If we got the extra row, there's a next page.
	if len(events) > limit {
		page.Events = events[:limit]
		last := events[limit-1]
		page.NextCursor = last.EventTS.Format(time.RFC3339Nano)
	}

	return page, nil
}
