package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/nis94/dream-mobility/internal/avro"
	"github.com/nis94/dream-mobility/internal/producer"
)

// Handler serves the ingestion HTTP endpoints.
type Handler struct {
	producer *producer.Producer
	logger   *slog.Logger
}

// NewHandler creates a Handler wired to the given Kafka producer.
func NewHandler(p *producer.Producer, logger *slog.Logger) *Handler {
	return &Handler{producer: p, logger: logger}
}

// Ingest handles POST /events.
// Accepts three body shapes:
//   - Single event:  { "event_id": "...", ... }
//   - Batch wrapper: { "events": [ {...}, {...} ] }
//   - Raw array:     [ {...}, {...} ]
func (h *Handler) Ingest(w http.ResponseWriter, r *http.Request) {
	events, err := parseRequestBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	resp := IngestResponse{}
	for i, ev := range events {
		if msg := ValidateEvent(&ev); msg != "" {
			resp.Rejected++
			resp.Errors = append(resp.Errors, EventError{
				Index:   i,
				EventID: ev.EventID,
				Error:   msg,
			})
			continue
		}

		avroEvent := mapToAvro(&ev)
		if err := h.producer.Produce(r.Context(), avroEvent); err != nil {
			h.logger.Error("kafka produce failed", "event_id", ev.EventID, "err", err)
			resp.Rejected++
			resp.Errors = append(resp.Errors, EventError{
				Index:   i,
				EventID: ev.EventID,
				Error:   "internal: failed to produce to Kafka",
			})
			continue
		}

		resp.Accepted++
	}

	status := http.StatusAccepted
	if resp.Accepted == 0 && resp.Rejected > 0 {
		status = http.StatusBadRequest
	}
	writeJSON(w, status, resp)
}

// parseRequestBody reads the full body and delegates to parseEventsFromBytes.
func parseRequestBody(r *http.Request) ([]EventRequest, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20)) // 10 MB max
	defer r.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return parseEventsFromBytes(body)
}

// parseEventsFromBytes detects whether the raw JSON is a single event, a batch
// wrapper {"events": [...]}, or a raw array [...].
func parseEventsFromBytes(body []byte) ([]EventRequest, error) {
	trimmed := strings.TrimSpace(string(body))
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty body")
	}

	switch trimmed[0] {
	case '[':
		var events []EventRequest
		if err := json.Unmarshal(body, &events); err != nil {
			return nil, fmt.Errorf("invalid JSON array: %w", err)
		}
		return events, nil

	case '{':
		// Try batch wrapper first: {"events": [...]}
		var batch BatchRequest
		if err := json.Unmarshal(body, &batch); err == nil && len(batch.Events) > 0 {
			return batch.Events, nil
		}
		// Fall back to single event.
		var ev EventRequest
		if err := json.Unmarshal(body, &ev); err != nil {
			return nil, fmt.Errorf("invalid JSON object: %w", err)
		}
		return []EventRequest{ev}, nil

	default:
		return nil, fmt.Errorf("expected JSON object or array")
	}
}

// mapToAvro converts the nested JSON request to the flat Avro struct.
func mapToAvro(ev *EventRequest) *avro.MovementEvent {
	ts, _ := time.Parse(time.RFC3339Nano, ev.Timestamp) // already validated

	m := &avro.MovementEvent{
		EventID:    ev.EventID,
		EntityType: ev.Entity.Type,
		EntityID:   ev.Entity.ID,
		Timestamp:  ts,
		Lat:        ev.Position.Lat,
		Lon:        ev.Position.Lon,
		SpeedKmh:   ev.SpeedKmh,
		HeadingDeg: ev.HeadingDeg,
		AccuracyM:  ev.AccuracyM,
		Source:     ev.Source,
	}

	// Attributes: store as JSON-encoded string if present.
	if len(ev.Attributes) > 0 && string(ev.Attributes) != "null" {
		s := string(ev.Attributes)
		m.Attributes = &s
	}

	return m
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
