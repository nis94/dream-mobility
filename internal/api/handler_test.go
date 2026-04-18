package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nis94/dream-mobility/internal/avro"
)

// fakeProducer implements eventProducer for handler tests.
type fakeProducer struct {
	calls    int
	produced []*avro.MovementEvent
	err      error
}

func (f *fakeProducer) Produce(_ context.Context, e *avro.MovementEvent) error {
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.produced = append(f.produced, e)
	return nil
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestHandler(prod eventProducer) *Handler {
	return NewHandler(prod, silentLogger())
}

// ---- parseEventsFromBytes ---------------------------------------------------

func TestParseRequestBody_SingleEvent(t *testing.T) {
	body := `{
		"event_id": "abc-123",
		"entity": {"type": "vehicle", "id": "v1"},
		"timestamp": "2025-01-01T10:00:00Z",
		"position": {"lat": 52.52, "lon": 13.405}
	}`
	events, err := parseEventsFromBytes([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventID != "abc-123" {
		t.Errorf("event_id = %q, want %q", events[0].EventID, "abc-123")
	}
}

func TestParseRequestBody_BatchWrapper(t *testing.T) {
	body := `{"events": [
		{"event_id": "a", "entity": {"type": "v", "id": "1"}, "timestamp": "2025-01-01T10:00:00Z", "position": {"lat": 0, "lon": 0}},
		{"event_id": "b", "entity": {"type": "v", "id": "2"}, "timestamp": "2025-01-01T10:00:00Z", "position": {"lat": 1, "lon": 1}}
	]}`
	events, err := parseEventsFromBytes([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestParseRequestBody_EmptyBatchWrapper(t *testing.T) {
	events, err := parseEventsFromBytes([]byte(`{"events": []}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if events == nil {
		t.Error("expected non-nil empty slice")
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestParseRequestBody_RawArray(t *testing.T) {
	body := `[
		{"event_id": "a", "entity": {"type": "v", "id": "1"}, "timestamp": "2025-01-01T10:00:00Z", "position": {"lat": 0, "lon": 0}}
	]`
	events, err := parseEventsFromBytes([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

func TestParseRequestBody_EmptyBody(t *testing.T) {
	_, err := parseEventsFromBytes([]byte(""))
	if err == nil {
		t.Error("expected error for empty body")
	}
}

func TestParseRequestBody_LeadingWhitespace(t *testing.T) {
	body := "\n\t  {\"event_id\":\"x\",\"entity\":{\"type\":\"v\",\"id\":\"1\"},\"timestamp\":\"2025-01-01T10:00:00Z\",\"position\":{\"lat\":0,\"lon\":0}}"
	events, err := parseEventsFromBytes([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

// ---- mapToAvro --------------------------------------------------------------

func TestMapToAvro(t *testing.T) {
	ev := EventRequest{
		EventID:   "abc-123",
		Entity:    EntityField{Type: "vehicle", ID: "v1"},
		Timestamp: "2025-01-01T10:15:00Z",
		Position:  PositionField{Lat: 52.52, Lon: 13.405},
		SpeedKmh:  ptr(42.3),
		Source:    ptr("gps"),
	}
	ts := time.Date(2025, 1, 1, 10, 15, 0, 0, time.UTC)

	m := mapToAvro(&ev, ts)
	if m.EventID != "abc-123" {
		t.Errorf("EventID = %q, want %q", m.EventID, "abc-123")
	}
	if m.EntityType != "vehicle" {
		t.Errorf("EntityType = %q, want %q", m.EntityType, "vehicle")
	}
	if m.EntityID != "v1" {
		t.Errorf("EntityID = %q, want %q", m.EntityID, "v1")
	}
	if m.Lat != 52.52 || m.Lon != 13.405 {
		t.Errorf("position = (%f, %f), want (52.52, 13.405)", m.Lat, m.Lon)
	}
	if m.SpeedKmh == nil || *m.SpeedKmh != 42.3 {
		t.Errorf("SpeedKmh = %v, want 42.3", m.SpeedKmh)
	}
	if m.Source == nil || *m.Source != "gps" {
		t.Errorf("Source = %v, want gps", m.Source)
	}
	if !m.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", m.Timestamp, ts)
	}
}

func TestMapToAvro_Attributes(t *testing.T) {
	ev := EventRequest{
		EventID:    "abc-123",
		Entity:     EntityField{Type: "vehicle", ID: "v1"},
		Timestamp:  "2025-01-01T10:15:00Z",
		Position:   PositionField{Lat: 52.52, Lon: 13.405},
		Attributes: []byte(`{"battery_level":0.82}`),
	}
	ts := time.Date(2025, 1, 1, 10, 15, 0, 0, time.UTC)

	m := mapToAvro(&ev, ts)
	if m.Attributes == nil {
		t.Fatal("expected non-nil Attributes")
	}
	if *m.Attributes != `{"battery_level":0.82}` {
		t.Errorf("Attributes = %q, want %q", *m.Attributes, `{"battery_level":0.82}`)
	}
}

// ---- Ingest HTTP tests ------------------------------------------------------

func validEventJSON() string {
	return `{
		"event_id": "abc-123",
		"entity": {"type": "vehicle", "id": "v1"},
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339Nano) + `",
		"position": {"lat": 52.52, "lon": 13.405}
	}`
}

func TestIngest_HappyPath_SingleEvent(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(prod)

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(validEventJSON()))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Ingest(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", rec.Code)
	}
	if prod.calls != 1 {
		t.Errorf("produce called %d times, want 1", prod.calls)
	}
	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Accepted != 1 || resp.Rejected != 0 {
		t.Errorf("accepted/rejected = %d/%d, want 1/0", resp.Accepted, resp.Rejected)
	}
}

func TestIngest_EmptyBatchWrapper(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(prod)

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{"events":[]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Ingest(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202 for empty batch", rec.Code)
	}
	if prod.calls != 0 {
		t.Errorf("produce called %d times, want 0", prod.calls)
	}
	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Accepted != 0 || resp.Rejected != 0 {
		t.Errorf("accepted/rejected = %d/%d, want 0/0", resp.Accepted, resp.Rejected)
	}
}

func TestIngest_OversizedBody(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(prod)

	// Build a body just over 10 MiB.
	big := bytes.Repeat([]byte("x"), maxRequestBody+1)
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(big))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Ingest(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
	if prod.calls != 0 {
		t.Errorf("produce should not be called on oversize body")
	}
}

func TestIngest_BadContentType(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(prod)

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader("nope"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	h.Ingest(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415", rec.Code)
	}
	if prod.calls != 0 {
		t.Errorf("produce should not be called on bad Content-Type")
	}
}

func TestIngest_MissingContentTypeIsAccepted(t *testing.T) {
	// Empty Content-Type should be tolerated (curl default).
	prod := &fakeProducer{}
	h := newTestHandler(prod)

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(validEventJSON()))
	// No Content-Type set.
	rec := httptest.NewRecorder()

	h.Ingest(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202 when Content-Type absent", rec.Code)
	}
}

func TestIngest_ValidationFailure(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(prod)

	bad := `{"event_id":"","entity":{"type":"v","id":"1"},"timestamp":"2025-01-01T10:00:00Z","position":{"lat":0,"lon":0}}`
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(bad))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Ingest(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Rejected != 1 {
		t.Errorf("rejected = %d, want 1", resp.Rejected)
	}
	if len(resp.Errors) != 1 || !strings.Contains(resp.Errors[0].Error, "event_id") {
		t.Errorf("unexpected errors: %+v", resp.Errors)
	}
}

func TestIngest_ProducerError(t *testing.T) {
	prod := &fakeProducer{err: errors.New("kafka down")}
	h := newTestHandler(prod)

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(validEventJSON()))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Ingest(rec, req)

	if rec.Code != http.StatusBadRequest {
		// Accepted=0 + Rejected>0 flips to 400.
		t.Errorf("status = %d, want 400 when all events fail to produce", rec.Code)
	}
	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Accepted != 0 || resp.Rejected != 1 {
		t.Errorf("accepted/rejected = %d/%d, want 0/1", resp.Accepted, resp.Rejected)
	}
	if !strings.Contains(resp.Errors[0].Error, "internal") {
		t.Errorf("expected 'internal' in produce-fail error, got %q", resp.Errors[0].Error)
	}
}

func TestIngest_MixedBatch(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(prod)

	// Three events: valid, invalid (bad lat), valid.
	tsNow := time.Now().UTC().Format(time.RFC3339Nano)
	body := `{"events":[
		{"event_id":"a","entity":{"type":"v","id":"1"},"timestamp":"` + tsNow + `","position":{"lat":0,"lon":0}},
		{"event_id":"b","entity":{"type":"v","id":"2"},"timestamp":"` + tsNow + `","position":{"lat":200,"lon":0}},
		{"event_id":"c","entity":{"type":"v","id":"3"},"timestamp":"` + tsNow + `","position":{"lat":1,"lon":1}}
	]}`

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Ingest(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202 for mixed batch", rec.Code)
	}
	if prod.calls != 2 {
		t.Errorf("produce calls = %d, want 2", prod.calls)
	}
	var resp IngestResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Accepted != 2 || resp.Rejected != 1 {
		t.Errorf("accepted/rejected = %d/%d, want 2/1", resp.Accepted, resp.Rejected)
	}
	if len(resp.Errors) != 1 || resp.Errors[0].Index != 1 {
		t.Errorf("expected single error at index 1, got %+v", resp.Errors)
	}
}
