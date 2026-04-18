package api

import "encoding/json"

// EventRequest is the JSON shape arriving from external clients, matching the
// task spec. Nested entity/position objects are flattened to the Avro schema
// in the handler.
type EventRequest struct {
	EventID    string          `json:"event_id"`
	Entity     EntityField     `json:"entity"`
	Timestamp  string          `json:"timestamp"`
	Position   PositionField   `json:"position"`
	SpeedKmh   *float64        `json:"speed_kmh,omitempty"`
	HeadingDeg *float64        `json:"heading_deg,omitempty"`
	AccuracyM  *float64        `json:"accuracy_m,omitempty"`
	Source     *string         `json:"source,omitempty"`
	Attributes json.RawMessage `json:"attributes,omitempty"`
}

// EntityField is the nested "entity" object of an EventRequest: the kind and
// identifier of the moving thing. Flattened to entity_type + entity_id in the
// Avro record.
type EntityField struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// PositionField is the nested "position" object of an EventRequest:
// a WGS-84 coordinate. Flattened to lat + lon in the Avro record.
type PositionField struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// BatchRequest wraps multiple events for the batch ingestion path.
type BatchRequest struct {
	// Events is the list of events to ingest. An empty slice is a well-formed
	// no-op and returns 202 with zero accepted/rejected counts.
	Events []EventRequest `json:"events"`
}

// IngestResponse is returned by POST /events.
type IngestResponse struct {
	// Accepted is the number of events successfully produced to Kafka.
	Accepted int `json:"accepted"`
	// Rejected is the number of events dropped due to validation failure or
	// Kafka produce error. The per-event reason is in Errors.
	Rejected int `json:"rejected"`
	// Errors enumerates each rejection by its index in the input.
	Errors []EventError `json:"errors,omitempty"`
}

// EventError describes a single validation or produce failure within a batch.
type EventError struct {
	// Index is the position of the failing event in the input (0-based).
	Index int `json:"index"`
	// EventID is the event_id of the failing event, when present in the input.
	EventID string `json:"event_id,omitempty"`
	// Error is a human-readable description of the failure.
	Error string `json:"error"`
}
