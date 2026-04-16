package producer

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/nis94/dream-mobility/internal/avro"
	"github.com/segmentio/kafka-go"
)

// Producer serializes MovementEvents to Avro with the Confluent Schema Registry
// wire format (magic byte + 4-byte schema ID + Avro binary) and writes them to Kafka.
type Producer struct {
	writer   *kafka.Writer
	schemaID int
	logger   *slog.Logger
}

// New creates a Producer. It registers the schema with Schema Registry on startup
// and caches the returned schema ID for the wire-format prefix.
func New(brokers []string, topic, schemaRegistryURL string, logger *slog.Logger) (*Producer, error) {
	schemaID, err := registerSchema(schemaRegistryURL, topic+"-value")
	if err != nil {
		return nil, fmt.Errorf("register schema: %w", err)
	}
	logger.Info("schema registered", "subject", topic+"-value", "schema_id", schemaID)

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  3,
	}

	return &Producer{writer: w, schemaID: schemaID, logger: logger}, nil
}

// Produce serializes the event and writes it to Kafka.
// Key = "entity_type:entity_id" (for partition affinity per entity).
func (p *Producer) Produce(ctx context.Context, event *avro.MovementEvent) error {
	avroBytes, err := event.Marshal()
	if err != nil {
		return fmt.Errorf("avro marshal: %w", err)
	}

	value := p.encodeWireFormat(avroBytes)
	key := event.EntityType + ":" + event.EntityID

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}

// Close flushes pending writes and closes the Kafka connection.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// encodeWireFormat prepends the Confluent Schema Registry wire format:
// byte 0: magic byte 0x00
// bytes 1-4: schema ID (big-endian uint32)
// bytes 5+: Avro binary data
func (p *Producer) encodeWireFormat(avroBytes []byte) []byte {
	buf := make([]byte, 5+len(avroBytes))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(p.schemaID))
	copy(buf[5:], avroBytes)
	return buf
}

// registerSchema POSTs the MovementEvent Avro schema to Schema Registry under
// the given subject and returns the global schema ID.
func registerSchema(srURL, subject string) (int, error) {
	// Get the canonical schema JSON from the generated code.
	schema := (&avro.MovementEvent{}).Schema().String()

	payload, err := json.Marshal(map[string]string{
		"schemaType": "AVRO",
		"schema":     schema,
	})
	if err != nil {
		return 0, err
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", srURL, subject)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	// Retry a few times — SR may still be starting.
	var resp *http.Response
	for attempt := 1; attempt <= 5; attempt++ {
		resp, err = http.DefaultClient.Do(req)
		if err == nil {
			break
		}
		slog.Warn("schema registry not reachable, retrying", "attempt", attempt, "err", err)
		time.Sleep(time.Duration(attempt) * time.Second)
		req.Body = io.NopCloser(bytes.NewReader(payload))
	}
	if err != nil {
		return 0, fmt.Errorf("POST %s after retries: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registry returned %d: %s", resp.StatusCode, body)
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode SR response: %w", err)
	}
	return result.ID, nil
}
