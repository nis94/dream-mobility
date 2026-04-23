package processor

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	avroschema "github.com/nis94/dream-mobility/internal/avro"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

//go:embed schema.sql
var migrationSQL string

// Pool defaults for single-consumer dev. Override via DSN params
// (pool_max_conns, pool_min_conns, pool_max_conn_lifetime, etc.) — pgxpool
// parses those natively. These bounds exist so the pool cannot grow
// unbounded under bursts, and so idle connections are recycled cleanly
// during rolling database restarts.
const (
	poolMaxConns        = 10
	poolMinConns        = 2
	poolMaxConnLifetime = 30 * time.Minute
	poolMaxConnIdleTime = 5 * time.Minute
	poolHealthCheck     = 30 * time.Second
)

// Store handles Postgres reads and writes for the stream processor.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a Store and runs migrations.
func NewStore(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}
	// Only override fields the DSN did not set. pgxpool.ParseConfig
	// respects pool_* DSN params; falling back to our defaults when the
	// caller didn't specify any keeps behavior predictable.
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
	PositionUpdated bool // false if existing position had a newer event_ts
}

// InsertEvent atomically inserts the raw event (deduplicated) and upserts
// the entity position (timestamp-gated). Both statements run in one
// transaction so the offset is only committed on full success.
//
// Tie semantics: the position upsert uses strict `>`, so on equal event_ts
// the existing row wins. A future ClickHouse ReplacingMergeTree sink MUST
// use `version = event_ts` to keep the two stores consistent.
func (s *Store) InsertEvent(ctx context.Context, ev *avroschema.MovementEvent) (InsertResult, error) {
	ctx, span := tracer.Start(ctx, "postgres.insert_event",
		trace.WithAttributes(
			attribute.String("event.id", ev.EventID),
			attribute.String("entity.type", ev.EntityType),
		),
	)
	defer span.End()

	var result InsertResult

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		span.SetStatus(codes.Error, "begin tx")
		span.RecordError(err)
		return result, fmt.Errorf("begin tx: %w", err)
	}
	// Use a detached context for rollback: if the request ctx was cancelled
	// mid-commit, we still want the rollback RPC to reach the server.
	// Rollback on an already-committed tx is a documented no-op.
	defer func() { _ = tx.Rollback(context.Background()) }()

	// 1) Raw event insert — dedupe by event_id.
	rawCtx, rawSpan := tracer.Start(ctx, "pg.insert raw_events")
	tag, err := tx.Exec(rawCtx, `
		INSERT INTO raw_events (
			event_id, entity_type, entity_id, event_ts,
			lat, lon, speed_kmh, heading_deg, accuracy_m,
			source, attributes
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (event_id) DO NOTHING`,
		ev.EventID, ev.EntityType, ev.EntityID, ev.Timestamp,
		ev.Lat, ev.Lon, ev.SpeedKmh, ev.HeadingDeg, ev.AccuracyM,
		ev.Source, attributesToJSONB(ev.Attributes),
	)
	if err != nil {
		rawSpan.SetStatus(codes.Error, "insert raw_events")
		rawSpan.RecordError(err)
		rawSpan.End()
		span.SetStatus(codes.Error, "insert raw_events")
		return result, fmt.Errorf("insert raw_events: %w", err)
	}
	result.RawInserted = tag.RowsAffected() > 0
	rawSpan.SetAttributes(attribute.Bool("pg.raw_inserted", result.RawInserted))
	rawSpan.End()

	// 2) Entity position upsert — timestamp-gated.
	// The WHERE clause ensures a stale (out-of-order) event does NOT
	// overwrite a position that already has a more recent event_ts.
	posCtx, posSpan := tracer.Start(ctx, "pg.upsert entity_positions")
	tag, err = tx.Exec(posCtx, `
		INSERT INTO entity_positions (
			entity_type, entity_id, event_ts, lat, lon, last_event_id
		) VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (entity_type, entity_id)
		DO UPDATE SET
			event_ts      = EXCLUDED.event_ts,
			lat           = EXCLUDED.lat,
			lon           = EXCLUDED.lon,
			last_event_id = EXCLUDED.last_event_id,
			updated_at    = now()
		WHERE EXCLUDED.event_ts > entity_positions.event_ts`,
		ev.EntityType, ev.EntityID, ev.Timestamp,
		ev.Lat, ev.Lon, ev.EventID,
	)
	if err != nil {
		posSpan.SetStatus(codes.Error, "upsert entity_positions")
		posSpan.RecordError(err)
		posSpan.End()
		span.SetStatus(codes.Error, "upsert entity_positions")
		return result, fmt.Errorf("upsert entity_positions: %w", err)
	}
	result.PositionUpdated = tag.RowsAffected() > 0
	posSpan.SetAttributes(attribute.Bool("pg.position_updated", result.PositionUpdated))
	posSpan.End()

	if err := tx.Commit(ctx); err != nil {
		span.SetStatus(codes.Error, "commit")
		span.RecordError(err)
		return result, fmt.Errorf("commit: %w", err)
	}
	span.SetAttributes(
		attribute.Bool("pg.raw_inserted", result.RawInserted),
		attribute.Bool("pg.position_updated", result.PositionUpdated),
	)
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
