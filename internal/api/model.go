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

type EntityField struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type PositionField struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// BatchRequest wraps multiple events for the batch ingestion path.
type BatchRequest struct {
	Events []EventRequest `json:"events"`
}

// IngestResponse is returned by POST /events.
type IngestResponse struct {
	Accepted int          `json:"accepted"`
	Rejected int          `json:"rejected"`
	Errors   []EventError `json:"errors,omitempty"`
}

// EventError describes a single validation failure within a batch.
type EventError struct {
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitempty"`
	Error   string `json:"error"`
}
