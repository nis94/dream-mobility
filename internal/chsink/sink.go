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

const (
	// batchSize is the max events accumulated before a flush.
	batchSize = 5000
	// batchTimeout is the max duration before an incomplete batch is flushed.
	batchTimeout = 2 * time.Second
)

// Sink batches movement events and writes them to ClickHouse via the native
// protocol. Events are accumulated in memory and flushed either when
// batchSize is reached or batchTimeout elapses, whichever comes first.
type Sink struct {
	conn   driver.Conn
	logger *slog.Logger

	mu    sync.Mutex
	buf   []*avroschema.MovementEvent
	timer *time.Timer
}

// New connects to ClickHouse and runs migrations. The addr should be the
// native-protocol endpoint (e.g. "localhost:9000").
func New(ctx context.Context, addr, database string, logger *slog.Logger) (*Sink, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
		},
		MaxOpenConns: 5,
		MaxIdleConns: 2,
		DialTimeout:  5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	// Run migrations (CREATE IF NOT EXISTS is idempotent).
	// ClickHouse driver executes one statement per Exec call, so we split
	// the migration file on semicolons.
	if err := execStatements(ctx, conn, migrationSQL); err != nil {
		return nil, fmt.Errorf("clickhouse migrate: %w", err)
	}
	logger.Info("clickhouse connected, migrations applied", "addr", addr, "db", database)

	s := &Sink{
		conn:   conn,
		logger: logger,
		buf:    make([]*avroschema.MovementEvent, 0, batchSize),
	}
	s.resetTimer()
	return s, nil
}

// Add enqueues an event for batched insertion. If the batch reaches batchSize,
// it is flushed synchronously.
func (s *Sink) Add(ctx context.Context, event *avroschema.MovementEvent) error {
	s.mu.Lock()
	s.buf = append(s.buf, event)
	needFlush := len(s.buf) >= batchSize
	s.mu.Unlock()

	if needFlush {
		return s.Flush(ctx)
	}
	return nil
}

// Flush writes the current buffer to ClickHouse in a single batch INSERT.
func (s *Sink) Flush(ctx context.Context) error {
	s.mu.Lock()
	if len(s.buf) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := s.buf
	s.buf = make([]*avroschema.MovementEvent, 0, batchSize)
	s.resetTimerLocked()
	s.mu.Unlock()

	if err := s.writeBatch(ctx, batch); err != nil {
		// Re-enqueue failed batch for retry on next flush.
		s.mu.Lock()
		s.buf = append(batch, s.buf...)
		s.mu.Unlock()
		return err
	}

	s.logger.Debug("clickhouse batch flushed", "count", len(batch))
	return nil
}

// RunFlusher periodically flushes incomplete batches until ctx is cancelled.
func (s *Sink) RunFlusher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown.
			if err := s.Flush(context.Background()); err != nil {
				s.logger.Error("final clickhouse flush failed", "err", err)
			}
			return
		case <-s.timer.C:
			if err := s.Flush(ctx); err != nil {
				s.logger.Error("clickhouse periodic flush failed", "err", err)
			}
		}
	}
}

// Close flushes remaining events and closes the connection.
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

func (s *Sink) resetTimer() {
	s.timer = time.NewTimer(batchTimeout)
}

func (s *Sink) resetTimerLocked() {
	if !s.timer.Stop() {
		select {
		case <-s.timer.C:
		default:
		}
	}
	s.timer.Reset(batchTimeout)
}

// execStatements splits SQL on semicolons and executes each non-empty
// statement individually (ClickHouse driver only supports one per Exec).
func execStatements(ctx context.Context, conn driver.Conn, sql string) error {
	stmts := strings.Split(sql, ";")
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q…: %w", truncate(stmt, 60), err)
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
