package query

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

// fakeStore implements dataStore for handler tests. It mirrors the real
// store's algorithm faithfully: order events by (event_ts DESC, event_id
// DESC), filter strictly below the cursor using row-wise compare, fetch
// limit+1, then slice to limit and encode the cursor from the last row.
type fakeStore struct {
	positions map[string]*Position  // key: "type:id"
	events    map[string][]RawEvent // key: "type:id"; order does not matter
}

func (f *fakeStore) GetPosition(_ context.Context, entityType, entityID string) (*Position, error) {
	p, ok := f.positions[entityType+":"+entityID]
	if !ok {
		return nil, ErrNotFound
	}
	return p, nil
}

func (f *fakeStore) ListEvents(_ context.Context, entityType, entityID string, cursor Cursor, limit int) (*EventsPage, error) {
	switch {
	case limit <= 0:
		limit = defaultLimit
	case limit > maxLimit:
		limit = maxLimit
	}

	all := append([]RawEvent(nil), f.events[entityType+":"+entityID]...)
	sort.Slice(all, func(i, j int) bool {
		if !all[i].EventTS.Equal(all[j].EventTS) {
			return all[i].EventTS.After(all[j].EventTS)
		}
		return all[i].EventID > all[j].EventID
	})

	fetchLimit := limit + 1
	filtered := make([]RawEvent, 0, fetchLimit)
	for _, e := range all {
		if !cursor.IsZero() {
			// Row-wise strict-less: keep rows where (e.EventTS, e.EventID)
			// is strictly less than (cursor.EventTS, cursor.EventID) under
			// the same DESC ordering.
			if e.EventTS.After(cursor.EventTS) {
				continue
			}
			if e.EventTS.Equal(cursor.EventTS) && e.EventID >= cursor.EventID {
				continue
			}
		}
		filtered = append(filtered, e)
		if len(filtered) == fetchLimit {
			break
		}
	}

	page := &EventsPage{Events: filtered}
	if len(filtered) > limit {
		page.Events = filtered[:limit]
		last := filtered[limit-1]
		page.NextCursor = Cursor{EventTS: last.EventTS, EventID: last.EventID}.Encode()
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

	// Page 1: limit=2 → expect [c, b] with a non-empty cursor.
	req := httptest.NewRequest(http.MethodGet, "/entities/vehicle/v1/events?limit=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var page EventsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page 1: %v", err)
	}
	if len(page.Events) != 2 || page.Events[0].EventID != "c" || page.Events[1].EventID != "b" {
		t.Fatalf("page 1 = %+v, want [c, b]", page.Events)
	}
	if page.NextCursor == "" {
		t.Fatal("expected non-empty next_cursor for first page")
	}

	// Page 2: cursor from page 1 → expect [a], empty cursor.
	req2 := httptest.NewRequest(http.MethodGet, "/entities/vehicle/v1/events?limit=2&cursor="+page.NextCursor, nil)
	rec2 := httptest.NewRecorder()
	r.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("page 2 status = %d; body = %s", rec2.Code, rec2.Body.String())
	}
	var page2 EventsPage
	if err := json.Unmarshal(rec2.Body.Bytes(), &page2); err != nil {
		t.Fatalf("decode page 2: %v", err)
	}
	if len(page2.Events) != 1 || page2.Events[0].EventID != "a" {
		t.Errorf("page 2 = %+v, want [a]", page2.Events)
	}
	if page2.NextCursor != "" {
		t.Errorf("expected empty cursor on last page, got %q", page2.NextCursor)
	}
}

// TestListEvents_TieBreakAcrossPages is the regression test for the Phase 4
// audit's Critical finding: two events sharing the same event_ts must not
// be dropped at a page boundary. With a cursor that carries only (event_ts),
// the second event at t2 would vanish between pages. With the composite
// (event_ts, event_id) cursor plus row-wise comparison, both are served.
func TestListEvents_TieBreakAcrossPages(t *testing.T) {
	t1 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 10, 1, 0, 0, time.UTC)

	// Four events for v1: one at t1, three at the same t2.
	store := &fakeStore{
		events: map[string][]RawEvent{
			"vehicle:v1": {
				{EventID: "z", EventTS: t2, EntityType: "vehicle", EntityID: "v1"},
				{EventID: "y", EventTS: t2, EntityType: "vehicle", EntityID: "v1"},
				{EventID: "x", EventTS: t2, EntityType: "vehicle", EntityID: "v1"},
				{EventID: "a", EventTS: t1, EntityType: "vehicle", EntityID: "v1"},
			},
		},
	}
	h := NewHandler(store, silentLogger())
	r := newTestRouter(h)

	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		path := "/entities/vehicle/v1/events?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("page %d status = %d; body = %s", pages, rec.Code, rec.Body.String())
		}
		var page EventsPage
		if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode page %d: %v", pages, err)
		}
		for _, e := range page.Events {
			if seen[e.EventID] {
				t.Fatalf("event %q served twice", e.EventID)
			}
			seen[e.EventID] = true
		}
		pages++
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	for _, id := range []string{"z", "y", "x", "a"} {
		if !seen[id] {
			t.Errorf("event %q was skipped across pages", id)
		}
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

	req := httptest.NewRequest(http.MethodGet, "/entities/vehicle/v1/events?cursor=not-a-valid-token", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestListEvents_BadLimit(t *testing.T) {
	store := &fakeStore{events: map[string][]RawEvent{}}
	h := NewHandler(store, silentLogger())
	r := newTestRouter(h)

	cases := []string{"abc", "0", "-5", "5000"}
	for _, v := range cases {
		t.Run(v, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/entities/vehicle/v1/events?limit="+v, nil)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("limit=%s → status %d, want 400; body = %s", v, rec.Code, rec.Body.String())
			}
		})
	}
}
