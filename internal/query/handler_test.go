package query

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

// fakeStore implements dataStore for handler tests.
type fakeStore struct {
	positions map[string]*Position   // key: "type:id"
	events    map[string][]RawEvent  // key: "type:id", sorted by event_ts DESC
}

func (f *fakeStore) GetPosition(_ context.Context, entityType, entityID string) (*Position, error) {
	key := entityType + ":" + entityID
	p, ok := f.positions[key]
	if !ok {
		return nil, ErrNotFound
	}
	return p, nil
}

func (f *fakeStore) ListEvents(_ context.Context, entityType, entityID string, cursor time.Time, limit int) (*EventsPage, error) {
	key := entityType + ":" + entityID
	all := f.events[key]
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	var filtered []RawEvent
	for _, e := range all {
		if !cursor.IsZero() && !e.EventTS.Before(cursor) {
			continue
		}
		filtered = append(filtered, e)
		if len(filtered) > limit {
			break
		}
	}

	page := &EventsPage{Events: filtered}
	if len(filtered) > limit {
		page.Events = filtered[:limit]
		page.NextCursor = filtered[limit-1].EventTS.Format(time.RFC3339Nano)
	}
	return page, nil
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestRouter(h *Handler) *chi.Mux {
	r := chi.NewRouter()
	r.Route("/entities/{type}/{id}", func(r chi.Router) {
		r.Get("/position", h.GetPosition)
		r.Get("/events", h.ListEvents)
	})
	return r
}

func TestGetPosition_Found(t *testing.T) {
	ts := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	store := &fakeStore{
		positions: map[string]*Position{
			"vehicle:v1": {
				EntityType: "vehicle", EntityID: "v1",
				EventTS: ts, Lat: 52.52, Lon: 13.405,
				LastEventID: "00000000-0000-0000-0000-000000000001",
				UpdatedAt:   ts,
			},
		},
	}
	h := NewHandler(store, silentLogger())
	r := newTestRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/entities/vehicle/v1/position", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var pos Position
	if err := json.Unmarshal(rec.Body.Bytes(), &pos); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pos.Lat != 52.52 || pos.Lon != 13.405 {
		t.Errorf("position = (%f, %f), want (52.52, 13.405)", pos.Lat, pos.Lon)
	}
}

func TestGetPosition_NotFound(t *testing.T) {
	store := &fakeStore{positions: map[string]*Position{}}
	h := NewHandler(store, silentLogger())
	r := newTestRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/entities/vehicle/unknown/position", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestListEvents_Paginated(t *testing.T) {
	t1 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 10, 1, 0, 0, time.UTC)
	t3 := time.Date(2025, 1, 1, 10, 2, 0, 0, time.UTC)

	// Events pre-sorted DESC (as the real store would return).
	store := &fakeStore{
		events: map[string][]RawEvent{
			"vehicle:v1": {
				{EventID: "c", EventTS: t3, EntityType: "vehicle", EntityID: "v1", Lat: 3, Lon: 3},
				{EventID: "b", EventTS: t2, EntityType: "vehicle", EntityID: "v1", Lat: 2, Lon: 2},
				{EventID: "a", EventTS: t1, EntityType: "vehicle", EntityID: "v1", Lat: 1, Lon: 1},
			},
		},
	}
	h := NewHandler(store, silentLogger())
	r := newTestRouter(h)

	// Page 1: limit=2
	req := httptest.NewRequest(http.MethodGet, "/entities/vehicle/v1/events?limit=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var page EventsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Events) != 2 {
		t.Fatalf("page 1 events = %d, want 2", len(page.Events))
	}
	if page.NextCursor == "" {
		t.Fatal("expected non-empty next_cursor for first page")
	}

	// Page 2: use cursor from page 1
	req2 := httptest.NewRequest(http.MethodGet, "/entities/vehicle/v1/events?limit=2&cursor="+page.NextCursor, nil)
	rec2 := httptest.NewRecorder()
	r.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("page 2 status = %d; body = %s", rec2.Code, rec2.Body.String())
	}
	var page2 EventsPage
	json.Unmarshal(rec2.Body.Bytes(), &page2)
	if len(page2.Events) != 1 {
		t.Errorf("page 2 events = %d, want 1", len(page2.Events))
	}
	if page2.NextCursor != "" {
		t.Errorf("expected empty cursor on last page, got %q", page2.NextCursor)
	}
}

func TestListEvents_EmptyResult(t *testing.T) {
	store := &fakeStore{events: map[string][]RawEvent{}}
	h := NewHandler(store, silentLogger())
	r := newTestRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/entities/vehicle/unknown/events", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (empty list, not 404)", rec.Code)
	}
}

func TestListEvents_BadCursor(t *testing.T) {
	store := &fakeStore{events: map[string][]RawEvent{}}
	h := NewHandler(store, silentLogger())
	r := newTestRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/entities/vehicle/v1/events?cursor=not-a-date", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}
