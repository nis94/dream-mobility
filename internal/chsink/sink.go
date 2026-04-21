// Package chsink batches MovementEvent records and writes them to
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
	avroschema "github.com/nis94/dream-mobility/internal/avro"
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
	buf []*avroschema.MovementEvent
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
		buf:       make([]*avroschema.MovementEvent, 0, cfg.BatchSize),
	}, nil
}

// Add appends an event to the in-memory buffer. Unlike the previous design,
// Add never triggers a flush on its own — the caller decides when to Flush
// (after hitting batchSize, on a timer, or on shutdown). This keeps flushing
// serialized with Kafka offset commits.
func (s *Sink) Add(_ context.Context, event *avroschema.MovementEvent) {
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
	batch := s.buf
	s.mu.Unlock()

	if err := s.writeBatch(ctx, batch); err != nil {
		return err
	}

	s.mu.Lock()
	// Truncate only the portion we successfully wrote; new events that
	// arrived while writeBatch was in flight are retained.
	s.buf = s.buf[n:]
	// Re-slice to the original capacity so we don't leak.
	if len(s.buf) == 0 {
		s.buf = make([]*avroschema.MovementEvent, 0, s.batchSize)
	} else {
		tail := make([]*avroschema.MovementEvent, len(s.buf), s.batchSize)
		copy(tail, s.buf)
		s.buf = tail
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

func (s *Sink) writeBatch(ctx context.Context, events []*avroschema.MovementEvent) error {
	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO raw_events (
			event_id, entity_type, entity_id, event_ts,
			lat, lon, speed_kmh, heading_deg, accuracy_m,
			source, attributes
		)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, ev := range events {
		attrs := ""
		if ev.Attributes != nil {
			attrs = *ev.Attributes
		}
		if err := batch.Append(
			ev.EventID, ev.EntityType, ev.EntityID, ev.Timestamp,
			ev.Lat, ev.Lon, ev.SpeedKmh, ev.HeadingDeg, ev.AccuracyM,
			ev.Source, attrs,
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
