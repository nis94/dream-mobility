package producer

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/nis94/dream-flight/internal/avro"
	"github.com/nis94/dream-flight/internal/tracing"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// tracer is the package-level OTel tracer. Using a package-level var keeps
// the API of Producer unchanged — callers don't have to pass a tracer.
var tracer = otel.Tracer("github.com/nis94/dream-flight/internal/producer")

// srHTTPClient bounds each Schema Registry request. The caller can further
// bound total operation time via context.
var srHTTPClient = &http.Client{Timeout: 5 * time.Second}

// Producer serializes FlightTelemetry events to Avro with the Confluent Schema
// Registry wire format (magic byte + 4-byte schema ID + Avro binary) and writes
// them to Kafka.
//
// Durability note: we use RequiredAcks=RequireOne and MaxAttempts=3 without
// the idempotent producer. This is at-least-once and can duplicate on broker
// retry; consumers MUST dedupe on event_id at the flight_telemetry sink.
// For production, switch to RequireAll with replication-factor>=3 and
// min.insync.replicas=2, and consider a client that supports
// enable.idempotence=true (confluent-kafka-go / librdkafka).
type Producer struct {
	writer   *kafka.Writer
	schemaID int
	logger   *slog.Logger
}

// New creates a Producer. It registers the canonical schema (embedded from
// internal/avro/flight_telemetry.avsc) with Schema Registry and caches the
// returned schema ID for the wire-format prefix. Registration retries with
// ctx-aware backoff on transient failures.
func New(ctx context.Context, brokers []string, topic, schemaRegistryURL string, logger *slog.Logger) (*Producer, error) {
	subject := topic + "-value"
	schemaID, err := registerSchema(ctx, schemaRegistryURL, subject, avro.CanonicalSchema, logger)
	if err != nil {
		return nil, fmt.Errorf("register schema: %w", err)
	}
	logger.Info("schema registered", "subject", subject, "schema_id", schemaID)

	w := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		// Murmur2Balancer with Consistent=true matches the librdkafka /
		// Confluent default partitioner. This is a partitioning contract:
		// any future producer on this topic must use the same hash to
		// preserve per-key ordering across writers.
		Balancer:     &kafka.Murmur2Balancer{Consistent: true},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  3,
		Logger:       kafkaLogger{logger: logger, level: slog.LevelDebug},
		ErrorLogger:  kafkaLogger{logger: logger, level: slog.LevelError},
	}

	return &Producer{writer: w, schemaID: schemaID, logger: logger}, nil
}

// Produce serializes the event and writes it to Kafka.
// Key = icao24 (for per-aircraft partition affinity — same icao24 → same
// partition → single consumer preserves trajectory ordering).
//
// A child span "kafka.produce" is started under the caller's context and its
// W3C TraceContext is injected into the outgoing message headers so the
// consumer can continue the same trace.
func (p *Producer) Produce(ctx context.Context, event *avro.FlightTelemetry) error {
	key := event.Icao24

	// Span covers both Avro marshal and Kafka write so marshal failures show
	// up in traces the same way write failures do.
	ctx, span := tracer.Start(ctx, "kafka.produce",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(p.writer.Topic),
			semconv.MessagingKafkaMessageKey(key),
			attribute.String("event.id", event.EventID),
			attribute.String("aircraft.icao24", event.Icao24),
			attribute.String("aircraft.origin_country", event.OriginCountry),
		),
	)
	defer span.End()

	avroBytes, err := event.Marshal()
	if err != nil {
		span.SetStatus(codes.Error, "avro marshal")
		span.RecordError(err)
		return fmt.Errorf("avro marshal: %w", err)
	}
	value := p.encodeWireFormat(avroBytes)

	headers := tracing.KafkaHeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, &headers)

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(key),
		Value:   value,
		Headers: []kafka.Header(headers),
	}); err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return err
	}
	return nil
}

// Close flushes pending writes and closes the Kafka connection.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// encodeWireFormat prepends the Confluent Schema Registry wire format:
// byte 0:     magic byte 0x00
// bytes 1-4:  schema ID (big-endian uint32)
// bytes 5+:   Avro binary data
func (p *Producer) encodeWireFormat(avroBytes []byte) []byte {
	buf := make([]byte, 5+len(avroBytes))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(p.schemaID))
	copy(buf[5:], avroBytes)
	return buf
}

// kafkaLogger adapts slog to kafka-go's Logger interface (Printf-style).
type kafkaLogger struct {
	logger *slog.Logger
	level  slog.Level
}

func (l kafkaLogger) Printf(format string, args ...interface{}) {
	l.logger.Log(context.Background(), l.level, fmt.Sprintf(format, args...))
}

// registerSchema POSTs the canonical Avro schema to Schema Registry and
// returns the global schema ID. Retries on network errors and HTTP 408/429/5xx
// with ctx-aware linear backoff. Other non-200 responses are fatal.
// A 200 on a previously-registered schema returns the existing ID (idempotent).
func registerSchema(ctx context.Context, srURL, subject string, schemaJSON []byte, logger *slog.Logger) (int, error) {
	payload, err := json.Marshal(map[string]string{
		"schemaType": "AVRO",
		"schema":     string(schemaJSON),
	})
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", srURL, subject)

	const maxAttempts = 5
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		id, retriable, err := postSchema(ctx, url, payload)
		if err == nil {
			// SR IDs are 32-bit unsigned on the wire; JSON decodes into a
			// Go int which is 64-bit on all our targets. Bounds-check here
			// so a future SR can't hand us a value that silently truncates
			// in encodeWireFormat's uint32 cast and corrupts the prefix.
			if id < 0 || id > math.MaxUint32 {
				return 0, fmt.Errorf("schema registry returned out-of-range id: %d", id)
			}
			return id, nil
		}
		lastErr = err
		if !retriable {
			return 0, err
		}
		logger.Warn("schema registry request failed, retrying",
			"attempt", attempt, "max_attempts", maxAttempts, "err", err)
		if err := sleepCtx(ctx, time.Duration(attempt)*time.Second); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

// postSchema issues a single POST to SR and classifies the outcome.
// Returns (id, false, nil) on 200.
// Returns (0, true, err) on network error or 408/429/5xx.
// Returns (0, false, err) on all other non-200 responses.
func postSchema(ctx context.Context, url string, payload []byte) (id int, retriable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, false, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := srHTTPClient.Do(req)
	if err != nil {
		return 0, true, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK:
		var r struct {
			ID int `json:"id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return 0, false, fmt.Errorf("decode SR response: %w", err)
		}
		return r.ID, false, nil
	case resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= 500:
		body, _ := io.ReadAll(resp.Body)
		return 0, true, fmt.Errorf("SR %d: %s", resp.StatusCode, truncate(body, 200))
	default:
		body, _ := io.ReadAll(resp.Body)
		return 0, false, fmt.Errorf("SR %d: %s", resp.StatusCode, truncate(body, 200))
	}
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

// sleepCtx blocks for d or until ctx is done, whichever is first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
