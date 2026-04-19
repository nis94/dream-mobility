package processor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Processor reads from Kafka, decodes Avro events, and writes them to Postgres.
// It uses explicit offset commits (FetchMessage + CommitMessages) so the offset
// is advanced only after the DB transaction succeeds — at-least-once semantics
// with idempotent Postgres writes (ON CONFLICT).
type Processor struct {
	reader *kafka.Reader
	store  *Store
	logger *slog.Logger
}

// New creates a Processor. The Kafka reader is configured for manual commit
// (no auto-commit) so offsets advance only on successful DB writes.
func New(brokers []string, topic, groupID string, store *Store, logger *slog.Logger) *Processor {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 << 20,       // 10 MiB
		CommitInterval: 0,              // manual commit only
		StartOffset:    kafka.FirstOffset,
		Logger:         kafkaLogger{logger: logger, level: slog.LevelDebug},
		ErrorLogger:    kafkaLogger{logger: logger, level: slog.LevelError},
	})

	return &Processor{reader: r, store: store, logger: logger}
}

// Run reads messages in a loop until ctx is cancelled. Each message is decoded,
// inserted into Postgres, and its offset committed. Errors on individual
// messages are logged but do not stop the loop (the message is retried on
// the next consumer restart because the offset was not committed).
func (p *Processor) Run(ctx context.Context) error {
	p.logger.Info("processor starting", "topic", p.reader.Config().Topic, "group", p.reader.Config().GroupID)

	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				p.logger.Info("processor stopped (context cancelled)")
				return nil
			}
			p.logger.Error("fetch message failed", "err", err)
			continue
		}

		if err := p.processMessage(ctx, msg); err != nil {
			p.logger.Error("process message failed, will retry on restart",
				"partition", msg.Partition, "offset", msg.Offset, "err", err)
			// Do NOT commit — the message will be re-delivered after restart.
			continue
		}

		if err := p.reader.CommitMessages(ctx, msg); err != nil {
			p.logger.Error("commit offset failed", "partition", msg.Partition, "offset", msg.Offset, "err", err)
		}
	}
}

func (p *Processor) processMessage(ctx context.Context, msg kafka.Message) error {
	event, err := decodeMovementEvent(msg.Value)
	if err != nil {
		p.logger.Warn("decode failed, skipping message",
			"partition", msg.Partition, "offset", msg.Offset, "err", err)
		// Return nil to commit the offset — a corrupted message will never
		// decode, so retrying is pointless.
		return nil
	}

	start := time.Now()
	result, err := p.store.InsertEvent(ctx, event)
	if err != nil {
		return err
	}

	p.logger.Debug("event processed",
		"event_id", event.EventID,
		"entity", event.EntityType+":"+event.EntityID,
		"raw_inserted", result.RawInserted,
		"position_updated", result.PositionUpdated,
		"duration", time.Since(start),
	)
	return nil
}

// Close shuts down the Kafka reader.
func (p *Processor) Close() error {
	return p.reader.Close()
}

// kafkaLogger adapts slog to kafka-go's Logger interface.
type kafkaLogger struct {
	logger *slog.Logger
	level  slog.Level
}

func (l kafkaLogger) Printf(format string, args ...interface{}) {
	l.logger.Log(context.Background(), l.level, fmt.Sprintf(format, args...))
}
