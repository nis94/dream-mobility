package processor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nis94/dream-mobility/internal/kafkametrics"
	"github.com/nis94/dream-mobility/internal/tracing"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/nis94/dream-mobility/internal/processor")

// commitTimeout bounds the detached CommitMessages call during shutdown.
// The request context may already be cancelled; we still want the offset
// commit RPC to reach the broker so we don't re-deliver the last message.
const commitTimeout = 5 * time.Second

// fetchBackoff is the minimum pause between consecutive FetchMessage errors.
// Without it, a broker outage causes a CPU-pinning retry loop and log flood.
const fetchBackoff = 500 * time.Millisecond

// metricsInterval is how often the sampler goroutine drains Reader.Stats()
// into Prometheus metrics. Shorter than Prometheus's scrape interval so
// every scrape sees fresh data and no sub-interval deltas are dropped.
const metricsInterval = 5 * time.Second

// Processor reads from Kafka, decodes Avro events, and writes them to Postgres.
// It uses explicit offset commits (FetchMessage + CommitMessages) so the offset
// is advanced only after the DB transaction succeeds — at-least-once semantics
// with idempotent Postgres writes (ON CONFLICT).
type Processor struct {
	reader  *kafka.Reader
	store   *Store
	logger  *slog.Logger
	metrics *kafkametrics.Recorder
}

// New creates a Processor. The Kafka reader is configured for manual commit
// (no auto-commit) so offsets advance only on successful DB writes.
func New(brokers []string, topic, groupID string, store *Store, metrics *kafkametrics.Recorder, logger *slog.Logger) *Processor {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 << 20, // 10 MiB
		CommitInterval: 0,        // manual commit only
		StartOffset:    kafka.FirstOffset,
		Logger:         kafkaLogger{logger: logger, level: slog.LevelDebug},
		ErrorLogger:    kafkaLogger{logger: logger, level: slog.LevelError},
	})

	return &Processor{reader: r, store: store, logger: logger, metrics: metrics}
}

// Run reads messages in a loop until ctx is cancelled. Each message is decoded,
// inserted into Postgres, and its offset committed. Errors on individual
// messages are logged but do not stop the loop (the message is retried on
// the next consumer restart because the offset was not committed).
//
// A background goroutine drains Reader.Stats() into the Prometheus Recorder
// every metricsInterval. No "processor stats" log line is emitted — the
// signal belongs in metrics, not logs.
func (p *Processor) Run(ctx context.Context) error {
	p.logger.Info("processor starting", "topic", p.reader.Config().Topic, "group", p.reader.Config().GroupID)

	if p.metrics != nil {
		go p.metrics.Sample(ctx, p.reader, metricsInterval)
	}

	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				p.logger.Info("processor stopped (context cancelled)")
				return nil
			}
			p.logger.Error("fetch message failed", "err", err)
			// Bounded backoff — without this, a broker disconnect becomes a
			// CPU-pinning retry loop that floods the log. On ctx cancel the
			// select returns immediately.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(fetchBackoff):
			}
			continue
		}

		if err := p.processMessage(ctx, msg); err != nil {
			p.logger.Error("process message failed, will retry on restart",
				"partition", msg.Partition, "offset", msg.Offset, "err", err)
			// Do NOT commit — the message will be re-delivered after restart.
			continue
		}

		// Detached context: during graceful shutdown the request ctx is
		// cancelled before this line runs, so passing it to CommitMessages
		// would fail the commit and silently guarantee a re-delivery of the
		// last in-flight message. 5s is well above the normal commit round-trip.
		commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
		err = p.reader.CommitMessages(commitCtx, msg)
		cancel()
		if err != nil {
			p.logger.Error("commit offset failed", "partition", msg.Partition, "offset", msg.Offset, "err", err)
		}
	}
}

func (p *Processor) processMessage(ctx context.Context, msg kafka.Message) error {
	// Continue the producer's trace if a traceparent was carried on the
	// message. Missing headers → root span (useful for tools that write
	// directly to Kafka without going through ingest-api).
	carrier := tracing.KafkaHeaderCarrier(msg.Headers)
	ctx = otel.GetTextMapPropagator().Extract(ctx, &carrier)

	ctx, span := tracer.Start(ctx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			attribute.Int("messaging.kafka.partition", msg.Partition),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		),
	)
	defer span.End()

	event, err := decodeMovementEvent(msg.Value)
	if err != nil {
		if p.metrics != nil {
			p.metrics.IncDecodeFailure()
		}
		span.SetStatus(codes.Error, "decode failed")
		span.RecordError(err)
		p.logger.Warn("decode failed, skipping message",
			"partition", msg.Partition, "offset", msg.Offset,
			"payload_len", len(msg.Value), "err", err)
		// Return nil to commit the offset — a corrupted message will never
		// decode, so retrying is pointless. The counter lets operators see
		// that this is happening.
		return nil
	}
	span.SetAttributes(
		attribute.String("event.id", event.EventID),
		attribute.String("entity.type", event.EntityType),
		attribute.String("entity.id", event.EntityID),
	)

	start := time.Now()
	result, err := p.store.InsertEvent(ctx, event)
	if err != nil {
		span.SetStatus(codes.Error, "store insert failed")
		span.RecordError(err)
		return err
	}
	span.SetAttributes(
		attribute.Bool("pg.raw_inserted", result.RawInserted),
		attribute.Bool("pg.position_updated", result.PositionUpdated),
	)

	p.logger.Debug("event processed",
		"event_id", event.EventID,
		"entity_type", event.EntityType,
		"entity_id", event.EntityID,
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

// NewReader creates a kafka.Reader with the same configuration used by the
// Processor. Exported so other consumers (e.g. ClickHouse sink) can reuse
// the reader setup with a different consumer group.
func NewReader(brokers []string, topic, groupID string, logger *slog.Logger) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
		Logger:         kafkaLogger{logger: logger, level: slog.LevelDebug},
		ErrorLogger:    kafkaLogger{logger: logger, level: slog.LevelError},
	})
}

// kafkaLogger adapts slog to kafka-go's Logger interface.
type kafkaLogger struct {
	logger *slog.Logger
	level  slog.Level
}

func (l kafkaLogger) Printf(format string, args ...interface{}) {
	l.logger.Log(context.Background(), l.level, fmt.Sprintf(format, args...))
}
