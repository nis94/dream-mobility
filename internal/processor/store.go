package processor

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	avroschema "github.com/nis94/dream-flight/internal/avro"
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
	RawInserted  bool // false if duplicate (ON CONFLICT DO NOTHING)
	StateUpdated bool // false if existing aircraft_state row had a newer observed_at
}

// InsertEvent atomically inserts the raw observation (deduplicated) and
// upserts the aircraft_state (timestamp-gated). Both statements run in one
// transaction so the offset is only committed on full success.
//
// Tie semantics: the state upsert uses strict `>`, so on equal observed_at
// the existing row wins. The ClickHouse ReplacingMergeTree sink MUST use
// `version = observed_at` to keep the two stores consistent.
func (s *Store) InsertEvent(ctx context.Context, ev *avroschema.FlightTelemetry) (InsertResult, error) {
	ctx, span := tracer.Start(ctx, "postgres.insert_event",
		trace.WithAttributes(
			attribute.String("event.id", ev.EventID),
			attribute.String("aircraft.icao24", ev.Icao24),
			attribute.String("aircraft.origin_country", ev.OriginCountry),
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
	// Detached rollback: if the request ctx was cancelled mid-commit, we
	// still want the rollback RPC to reach the server. Rollback on an
	// already-committed tx is a documented no-op.
	defer func() { _ = tx.Rollback(context.Background()) }()

	// 1) Raw observation insert — dedupe by event_id.
	rawCtx, rawSpan := tracer.Start(ctx, "pg.insert flight_telemetry")
	tag, err := tx.Exec(rawCtx, `
		INSERT INTO flight_telemetry (
			event_id, icao24, callsign, origin_country, observed_at, position_source,
			lat, lon, baro_altitude_m, geo_altitude_m, velocity_ms, true_track_deg,
			vertical_rate_ms, on_ground, squawk, spi, category
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		ON CONFLICT (event_id) DO NOTHING`,
		ev.EventID, ev.Icao24, ev.Callsign, ev.OriginCountry, ev.ObservedAt, ev.PositionSource,
		ev.Lat, ev.Lon, ev.BaroAltitudeM, ev.GeoAltitudeM, ev.VelocityMs, ev.TrueTrackDeg,
		ev.VerticalRateMs, ev.OnGround, ev.Squawk, ev.Spi, ev.Category,
	)
	if err != nil {
		rawSpan.SetStatus(codes.Error, "insert flight_telemetry")
		rawSpan.RecordError(err)
		rawSpan.End()
		span.SetStatus(codes.Error, "insert flight_telemetry")
		return result, fmt.Errorf("insert flight_telemetry: %w", err)
	}
	result.RawInserted = tag.RowsAffected() > 0
	rawSpan.SetAttributes(attribute.Bool("pg.raw_inserted", result.RawInserted))
	rawSpan.End()

	// 2) aircraft_state upsert — timestamp-gated.
	// The WHERE clause ensures a stale (out-of-order) observation does NOT
	// overwrite a state row that already has a more recent observed_at.
	stateCtx, stateSpan := tracer.Start(ctx, "pg.upsert aircraft_state")
	tag, err = tx.Exec(stateCtx, `
		INSERT INTO aircraft_state (
			icao24, last_callsign, origin_country, observed_at,
			lat, lon, baro_altitude_m, velocity_ms, on_ground, last_squawk, last_event_id
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (icao24)
		DO UPDATE SET
			last_callsign   = EXCLUDED.last_callsign,
			origin_country  = EXCLUDED.origin_country,
			observed_at     = EXCLUDED.observed_at,
			lat             = EXCLUDED.lat,
			lon             = EXCLUDED.lon,
			baro_altitude_m = EXCLUDED.baro_altitude_m,
			velocity_ms     = EXCLUDED.velocity_ms,
			on_ground       = EXCLUDED.on_ground,
			last_squawk     = EXCLUDED.last_squawk,
			last_event_id   = EXCLUDED.last_event_id,
			updated_at      = now()
		WHERE EXCLUDED.observed_at > aircraft_state.observed_at`,
		ev.Icao24, ev.Callsign, ev.OriginCountry, ev.ObservedAt,
		ev.Lat, ev.Lon, ev.BaroAltitudeM, ev.VelocityMs, ev.OnGround, ev.Squawk, ev.EventID,
	)
	if err != nil {
		stateSpan.SetStatus(codes.Error, "upsert aircraft_state")
		stateSpan.RecordError(err)
		stateSpan.End()
		span.SetStatus(codes.Error, "upsert aircraft_state")
		return result, fmt.Errorf("upsert aircraft_state: %w", err)
	}
	result.StateUpdated = tag.RowsAffected() > 0
	stateSpan.SetAttributes(attribute.Bool("pg.state_updated", result.StateUpdated))
	stateSpan.End()

	if err := tx.Commit(ctx); err != nil {
		span.SetStatus(codes.Error, "commit")
		span.RecordError(err)
		return result, fmt.Errorf("commit: %w", err)
	}
	span.SetAttributes(
		attribute.Bool("pg.raw_inserted", result.RawInserted),
		attribute.Bool("pg.state_updated", result.StateUpdated),
	)
	return result, nil
}
