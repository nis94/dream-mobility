package processor

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	avroschema "github.com/nis94/dream-mobility/internal/avro"
)

//go:embed schema.sql
var migrationSQL string

// Store handles Postgres reads and writes for the stream processor.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a Store and runs migrations.
func NewStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}
	if _, err := pool.Exec(ctx, migrationSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

// InsertResult describes the outcome of inserting a single event.
type InsertResult struct {
	RawInserted     bool // false if duplicate (ON CONFLICT DO NOTHING)
	PositionUpdated bool // false if existing position had a newer timestamp
}

// InsertEvent atomically inserts the raw event (deduplicated) and upserts
// the entity position (timestamp-gated). Both statements run in one
// transaction so the offset is only committed on full success.
func (s *Store) InsertEvent(ctx context.Context, ev *avroschema.MovementEvent) (InsertResult, error) {
	var result InsertResult

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return result, fmt.Errorf("begin tx: %w", err)
	}
	// Use a detached context for rollback: if the request ctx was cancelled
	// mid-commit, we still want the rollback RPC to reach the server.
	// Rollback on an already-committed tx is a documented no-op.
	defer func() { _ = tx.Rollback(context.Background()) }()

	// 1) Raw event insert — dedupe by event_id.
	tag, err := tx.Exec(ctx, `
		INSERT INTO raw_events (
			event_id, entity_type, entity_id, timestamp,
			lat, lon, speed_kmh, heading_deg, accuracy_m,
			source, attributes
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (event_id) DO NOTHING`,
		ev.EventID, ev.EntityType, ev.EntityID, ev.Timestamp,
		ev.Lat, ev.Lon, ev.SpeedKmh, ev.HeadingDeg, ev.AccuracyM,
		ev.Source, attributesToJSONB(ev.Attributes),
	)
	if err != nil {
		return result, fmt.Errorf("insert raw_events: %w", err)
	}
	result.RawInserted = tag.RowsAffected() > 0

	// 2) Entity position upsert — timestamp-gated.
	// The WHERE clause ensures a stale (out-of-order) event does NOT
	// overwrite a position that already has a more recent timestamp.
	tag, err = tx.Exec(ctx, `
		INSERT INTO entity_positions (
			entity_type, entity_id, timestamp, lat, lon, last_event_id
		) VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (entity_type, entity_id)
		DO UPDATE SET
			timestamp     = EXCLUDED.timestamp,
			lat           = EXCLUDED.lat,
			lon           = EXCLUDED.lon,
			last_event_id = EXCLUDED.last_event_id,
			updated_at    = now()
		WHERE EXCLUDED.timestamp > entity_positions.timestamp`,
		ev.EntityType, ev.EntityID, ev.Timestamp,
		ev.Lat, ev.Lon, ev.EventID,
	)
	if err != nil {
		return result, fmt.Errorf("upsert entity_positions: %w", err)
	}
	result.PositionUpdated = tag.RowsAffected() > 0

	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit: %w", err)
	}
	return result, nil
}

// attributesToJSONB converts the optional JSON-string attributes field to
// a value suitable for the JSONB column. Returns nil (SQL NULL) when absent.
func attributesToJSONB(s *string) any {
	if s == nil || *s == "" {
		return nil
	}
	return *s // pgx automatically maps string → JSONB
}
