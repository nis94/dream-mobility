// Package chsink batches FlightTelemetry records and writes them to
// ClickHouse via the native protocol.
//
// Correctness contract (Phase 5 audit fix): the Sink is a pure buffer + batch
// writer. It does NOT own the Kafka reader, does NOT own a background
// goroutine, and does NOT commit offsets. The consumer loop drives every
// flush and commits offsets AFTER the batch write succeeds — so a crash
// between Add() and Flush() loses no data (those events' Kafka offsets
// were never committed and redelivery will replay them).
package chsink

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	avroschema "github.com/nis94/dream-flight/internal/avro"
)

//go:embed schema.sql
var migrationSQL string

// DefaultBatchSize is the default number of events accumulated before a
// forced flush. Exposed so the consumer loop can align its batch-commit
// trigger with the sink's buffer bound.
const DefaultBatchSize = 5000

// DefaultBatchTimeout is the default max duration an incomplete batch
// may sit in the buffer before being flushed.
const DefaultBatchTimeout = 2 * time.Second

// Config bounds the sink's buffer and write behavior. Zero values fall back
// to the Default* constants.
type Config struct {
	Addr         string
	Database     string
	BatchSize    int
	BatchTimeout time.Duration
}

// Sink batches movement events and writes them to ClickHouse via the native
// protocol. Safe for serial Add/Flush/Close from a single goroutine; the
// internal mutex only guards against Close racing a still-in-flight Flush.
type Sink struct {
	conn      driver.Conn
	logger    *slog.Logger
	batchSize int

	mu  sync.Mutex
	buf []*avroschema.FlightTelemetry
}

// New connects to ClickHouse, applies the embedded migration, and returns
// a Sink ready for use.
//
// Bootstraps in two steps: a short-lived connection without Auth.Database
// runs the `CREATE DATABASE IF NOT EXISTS` (so the target DB exists before
// the main connection selects it), then a second connection bound to the
// target DB runs the table + MV DDL. This avoids the chicken-and-egg where
// Ping() on Auth.Database would fail before CREATE DATABASE had a chance
// to run.
func New(ctx context.Context, cfg Config, logger *slog.Logger) (*Sink, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	bootstrap, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{cfg.Addr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse bootstrap open: %w", err)
	}
	if err := bootstrap.Ping(ctx); err != nil {
		_ = bootstrap.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	if err := bootstrap.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+cfg.Database); err != nil {
		_ = bootstrap.Close()
		return nil, fmt.Errorf("clickhouse create database: %w", err)
	}
	_ = bootstrap.Close()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:         []string{cfg.Addr},
		Auth:         clickhouse.Auth{Database: cfg.Database},
		MaxOpenConns: 5,
		MaxIdleConns: 2,
		DialTimeout:  5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}

	if err := execStatements(ctx, conn, migrationSQL); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse migrate: %w", err)
	}
	logger.Info("clickhouse ready", "addr", cfg.Addr, "db", cfg.Database)

	return &Sink{
		conn:      conn,
		logger:    logger,
		batchSize: cfg.BatchSize,
		buf:       make([]*avroschema.FlightTelemetry, 0, cfg.BatchSize),
	}, nil
}

// Add appends an event to the in-memory buffer. Unlike the previous design,
// Add never triggers a flush on its own — the caller decides when to Flush
// (after hitting batchSize, on a timer, or on shutdown). This keeps flushing
// serialized with Kafka offset commits.
func (s *Sink) Add(_ context.Context, event *avroschema.FlightTelemetry) {
	s.mu.Lock()
	s.buf = append(s.buf, event)
	s.mu.Unlock()
}

// Len returns the current buffer length. Used by the consumer loop to decide
// when to trigger a size-based flush.
func (s *Sink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf)
}

// Flush writes every buffered event to ClickHouse in a single batch INSERT.
// On success the buffer is cleared and nil is returned, so the caller may
// commit the corresponding Kafka offsets. On failure the buffer is left
// intact so the next Flush will retry the same events — and because offsets
// were not committed, a crash here replays from Kafka with no data loss.
func (s *Sink) Flush(ctx context.Context) error {
	s.mu.Lock()
	n := len(s.buf)
	if n == 0 {
		s.mu.Unlock()
		return nil
	}
	// Copy into an owned slice before releasing the lock so writeBatch cannot
	// race a concurrent Add() on the same backing array. Today the public API
	// is single-goroutine by contract, but the mutex advertises thread-safety
	// and the detached copy costs only one allocation per flush.
	batch := make([]*avroschema.FlightTelemetry, n)
	copy(batch, s.buf)
	s.mu.Unlock()

	if err := s.writeBatch(ctx, batch); err != nil {
		// On failure, s.buf is untouched so the next Flush retries the same
		// events. Because offsets were not committed, a crash here replays
		// from Kafka with no data loss.
		return err
	}

	s.mu.Lock()
	// Truncate only the portion we successfully wrote; new events that
	// arrived while writeBatch was in flight are retained.
	if len(s.buf) > n {
		remaining := len(s.buf) - n
		tail := make([]*avroschema.FlightTelemetry, remaining, s.batchSize)
		copy(tail, s.buf[n:])
		s.buf = tail
	} else {
		s.buf = make([]*avroschema.FlightTelemetry, 0, s.batchSize)
	}
	s.mu.Unlock()

	s.logger.Debug("clickhouse batch flushed", "count", n)
	return nil
}

// Close flushes any remaining events and closes the underlying connection.
// The caller must have stopped feeding Add() before Close.
func (s *Sink) Close(ctx context.Context) error {
	if err := s.Flush(ctx); err != nil {
		s.logger.Error("close flush failed", "err", err)
	}
	return s.conn.Close()
}

func (s *Sink) writeBatch(ctx context.Context, events []*avroschema.FlightTelemetry) error {
	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO flight.telemetry (
			event_id, icao24, callsign, origin_country, observed_at, position_source,
			lat, lon, baro_altitude_m, geo_altitude_m, velocity_ms, true_track_deg,
			vertical_rate_ms, on_ground, squawk, spi, category
		)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, ev := range events {
		// ClickHouse-go maps Go bool → UInt8 for the on_ground / spi columns
		// in the new schema, so passing the bool directly is correct.
		if err := batch.Append(
			ev.EventID, ev.Icao24, ev.Callsign, ev.OriginCountry, ev.ObservedAt, ev.PositionSource,
			ev.Lat, ev.Lon, ev.BaroAltitudeM, ev.GeoAltitudeM, ev.VelocityMs, ev.TrueTrackDeg,
			ev.VerticalRateMs, ev.OnGround, ev.Squawk, ev.Spi, ev.Category,
		); err != nil {
			return fmt.Errorf("append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

// execStatements splits SQL on top-level semicolons and executes each
// non-empty statement individually (the ClickHouse driver accepts one
// statement per Exec). This is adequate for our CREATE/CREATE MATERIALIZED
// VIEW DDL; if the schema ever grows string literals containing `;` or
// stored-function bodies, switch to a migration tool.
func execStatements(ctx context.Context, conn driver.Conn, sql string) error {
	for _, stmt := range splitStatements(sql) {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q…: %w", truncate(stmt, 60), err)
		}
	}
	return nil
}

// splitStatements strips full-line comments and splits on top-level `;`.
func splitStatements(sql string) []string {
	lines := strings.Split(sql, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}
		cleaned = append(cleaned, l)
	}
	body := strings.Join(cleaned, "\n")
	raw := strings.Split(body, ";")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
