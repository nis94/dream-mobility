package processor

import (
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
	avroschema "github.com/nis94/dream-flight/internal/avro"
)

// DecodeFlightTelemetry is the exported entry point for other consumers
// (e.g. the ClickHouse sink) that share the same Kafka topic.
func DecodeFlightTelemetry(data []byte) (*avroschema.FlightTelemetry, error) {
	return decodeFlightTelemetry(data)
}

// decodeFlightTelemetry strips the Confluent Schema Registry wire format
// (magic byte + 4-byte schema ID) and deserializes the Avro payload.
//
// Wire format:
//
//	byte 0:    0x00 (magic)
//	bytes 1-4: schema ID (big-endian uint32) — ignored here; we trust the
//	           topic-level subject binding.
//	bytes 5+:  Avro binary data
func decodeFlightTelemetry(data []byte) (*avroschema.FlightTelemetry, error) {
	if len(data) < 6 { // 5-byte header + at least 1 byte payload
		return nil, fmt.Errorf("message too short (%d bytes)", len(data))
	}
	if data[0] != 0x00 {
		return nil, fmt.Errorf("expected magic byte 0x00, got 0x%02x", data[0])
	}

	_ = binary.BigEndian.Uint32(data[1:5]) // schema ID (logged if needed)

	var event avroschema.FlightTelemetry
	if err := avro.Unmarshal(event.Schema(), data[5:], &event); err != nil {
		return nil, fmt.Errorf("avro unmarshal: %w", err)
	}
	return &event, nil
}
