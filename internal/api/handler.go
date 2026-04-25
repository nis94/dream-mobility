package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/nis94/dream-flight/internal/avro"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/nis94/dream-flight/internal/api")

// maxRequestBody caps the POST body size. Oversized requests are rejected with
// HTTP 413 via http.MaxBytesReader's typed error.
const maxRequestBody = 10 << 20 // 10 MiB

// eventProducer is the handler's narrow dependency on the Kafka producer.
// Defined here (consumer side) so tests can supply a fake without pulling in
// the real kafka.Writer machinery.
type eventProducer interface {
	Produce(ctx context.Context, event *avro.FlightTelemetry) error
}

// Handler serves the ingestion HTTP endpoints.
type Handler struct {
	producer eventProducer
	logger   *slog.Logger
}

// NewHandler creates a Handler wired to the given event producer.
func NewHandler(p eventProducer, logger *slog.Logger) *Handler {
	return &Handler{producer: p, logger: logger}
}

// Ingest handles POST /events.
// Accepts three body shapes:
//   - Single event:  { "event_id": "...", ... }
//   - Batch wrapper: { "events": [ {...}, {...} ] }
//   - Raw array:     [ {...}, {...} ]
//
// Bodies larger than maxRequestBody are rejected with 413. Non-JSON
// Content-Type (when present and not empty) is rejected with 415.
func (h *Handler) Ingest(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		h.writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}

	events, err := h.parseRequestBody(w, r)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			h.writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
				"error": fmt.Sprintf("request body exceeds %d bytes", maxErr.Limit),
			})
			return
		}
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	resp := IngestResponse{Errors: make([]EventError, 0, len(events))}
	for i := range events {
		ev := &events[i]
		h.processOne(r.Context(), i, ev, &resp)
	}

	status := http.StatusAccepted
	if resp.Accepted == 0 && resp.Rejected > 0 {
		status = http.StatusBadRequest
	}
	h.writeJSON(w, status, resp)
}

// processOne validates + produces a single event inside its own span so each
// event gets a distinct trace root. Mutations to resp are serialized by the
// caller's single-threaded loop.
func (h *Handler) processOne(ctx context.Context, i int, ev *EventRequest, resp *IngestResponse) {
	ctx, span := tracer.Start(ctx, "ingest.event",
		trace.WithAttributes(
			attribute.Int("event.index", i),
			attribute.String("event.id", ev.EventID),
		),
	)
	defer span.End()

	ts, err := ValidateEvent(ev)
	if err != nil {
		span.SetStatus(codes.Error, "validation failed")
		span.SetAttributes(attribute.String("error.reason", err.Error()))
		resp.Rejected++
		resp.Errors = append(resp.Errors, EventError{
			Index:   i,
			EventID: ev.EventID,
			Error:   err.Error(),
		})
		return
	}

	avroEvent := mapToAvro(ev, ts)
	span.SetAttributes(
		attribute.String("aircraft.icao24", avroEvent.Icao24),
		attribute.String("aircraft.origin_country", avroEvent.OriginCountry),
	)
	if err := h.producer.Produce(ctx, avroEvent); err != nil {
		span.SetStatus(codes.Error, "produce failed")
		span.RecordError(err)
		h.logger.Error("kafka produce failed", "event_id", ev.EventID, "err", err)
		resp.Rejected++
		resp.Errors = append(resp.Errors, EventError{
			Index:   i,
			EventID: ev.EventID,
			Error:   "internal: failed to produce to Kafka",
		})
		return
	}
	resp.Accepted++
}

// parseRequestBody reads up to maxRequestBody from the request and delegates
// to parseEventsFromBytes. The returned error may wrap *http.MaxBytesError
// (see errors.As in Ingest) for oversized bodies.
func (h *Handler) parseRequestBody(w http.ResponseWriter, r *http.Request) ([]EventRequest, error) {
	reader := http.MaxBytesReader(w, r.Body, maxRequestBody)
	defer func() { _ = r.Body.Close() }()

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return parseEventsFromBytes(body)
}

// parseEventsFromBytes detects whether the raw JSON is a single event, a batch
// wrapper {"events": [...]}, or a raw array [...], and decodes accordingly.
// An empty batch wrapper ({"events":[]}) is treated as a well-formed no-op
// and returns an empty slice (distinct from a missing "events" key, which
// falls back to single-event decoding).
func parseEventsFromBytes(body []byte) ([]EventRequest, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n\v\f")
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
		// Peek for the "events" key via json.RawMessage. Non-nil means the key
		// was present in the input (even if its value is null or []), in which
		// case this is a batch wrapper — not a single event.
		var peek struct {
			Events json.RawMessage `json:"events"`
		}
		if err := json.Unmarshal(body, &peek); err != nil {
			return nil, fmt.Errorf("invalid JSON object: %w", err)
		}
		if peek.Events != nil {
			var events []EventRequest
			if err := json.Unmarshal(peek.Events, &events); err != nil {
				return nil, fmt.Errorf("invalid JSON in events array: %w", err)
			}
			return events, nil
		}
		// No "events" key → single event.
		var ev EventRequest
		if err := json.Unmarshal(body, &ev); err != nil {
			return nil, fmt.Errorf("invalid JSON object: %w", err)
		}
		return []EventRequest{ev}, nil

	default:
		return nil, fmt.Errorf("expected JSON object or array")
	}
}

// mapToAvro converts the JSON request to the Avro struct. ts is the already-
// parsed timestamp from ValidateEvent; avoids a redundant time.Parse here.
// The shapes are flat-to-flat now — every JSON field has a one-to-one Avro
// counterpart.
func mapToAvro(ev *EventRequest, ts time.Time) *avro.FlightTelemetry {
	return &avro.FlightTelemetry{
		EventID:        ev.EventID,
		Icao24:         ev.Icao24,
		Callsign:       ev.Callsign,
		OriginCountry:  ev.OriginCountry,
		ObservedAt:     ts,
		PositionSource: ev.PositionSource,
		Lat:            ev.Lat,
		Lon:            ev.Lon,
		BaroAltitudeM:  ev.BaroAltitudeM,
		GeoAltitudeM:   ev.GeoAltitudeM,
		VelocityMs:     ev.VelocityMs,
		TrueTrackDeg:   ev.TrueTrackDeg,
		VerticalRateMs: ev.VerticalRateMs,
		OnGround:       ev.OnGround,
		Squawk:         ev.Squawk,
		Spi:            ev.Spi,
		Category:       ev.Category,
	}
}

// writeJSON sets the Content-Type + status and encodes v. Encode errors
// after the status line has been flushed cannot repair the response but
// are logged for operability (truncated body visibility).
func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Warn("response encode failed", "err", err)
	}
}
