package query

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

// fakeStore implements dataStore for handler tests. Minimal — exercises only
// the dispatch + 200/404/400 paths the handler owns. Database-shaped behavior
// (cursor compare, ordering) is tested in store_test.go via a real Postgres.
type fakeStore struct {
	state map[string]*AircraftState // key: icao24
	track map[string][]Telemetry    // key: icao24
	err   error                     // injected error for failure paths
}

func (f *fakeStore) GetAircraft(_ context.Context, icao24 string) (*AircraftState, error) {
	if f.err != nil {
		return nil, f.err
	}
	a, ok := f.state[icao24]
	if !ok {
		return nil, ErrNotFound
	}
	return a, nil
}

func (f *fakeStore) GetTrack(_ context.Context, icao24 string, _ Cursor, limit int) (*TrackPage, error) {
	if f.err != nil {
		return nil, f.err
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	out := f.track[icao24]
	if len(out) > limit {
		out = out[:limit]
	}
	return &TrackPage{Telemetry: out}, nil
}

func (f *fakeStore) ListActive(_ context.Context, originCountry string, _ Cursor, _ int) (*ActiveAircraftPage, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]AircraftState, 0, len(f.state))
	for _, a := range f.state {
		if originCountry != "" && a.OriginCountry != originCountry {
			continue
		}
		out = append(out, *a)
	}
	return &ActiveAircraftPage{Aircraft: out}, nil
}

func newTestHandler(s dataStore) *Handler {
	return NewHandler(s, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newRouter(h *Handler) http.Handler {
	r := chi.NewRouter()
	r.Get("/flights/active", h.ListActive)
	r.Route("/aircraft/{icao24}", func(r chi.Router) {
		r.Get("/", h.GetAircraft)
		r.Get("/track", h.GetTrack)
	})
	return r
}

func TestGetAircraft_OK(t *testing.T) {
	s := &fakeStore{state: map[string]*AircraftState{
		"abc123": {Icao24: "abc123", OriginCountry: "GB", ObservedAt: time.Now(), Lat: 51.5, Lon: -0.1, LastEventID: "ev-1"},
	}}
	h := newTestHandler(s)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/aircraft/abc123", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got AircraftState
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Icao24 != "abc123" {
		t.Errorf("icao24 = %q", got.Icao24)
	}
}

func TestGetAircraft_NotFound(t *testing.T) {
	s := &fakeStore{state: map[string]*AircraftState{}}
	h := newTestHandler(s)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/aircraft/missing", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestGetAircraft_InternalError(t *testing.T) {
	s := &fakeStore{err: errors.New("pg down")}
	h := newTestHandler(s)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/aircraft/abc123", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestListActive_OK(t *testing.T) {
	s := &fakeStore{state: map[string]*AircraftState{
		"a": {Icao24: "a", OriginCountry: "GB", ObservedAt: time.Now()},
		"b": {Icao24: "b", OriginCountry: "DE", ObservedAt: time.Now()},
	}}
	h := newTestHandler(s)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/flights/active", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var page ActiveAircraftPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Aircraft) != 2 {
		t.Errorf("got %d aircraft, want 2", len(page.Aircraft))
	}
}

func TestListActive_FilterByCountry(t *testing.T) {
	s := &fakeStore{state: map[string]*AircraftState{
		"a": {Icao24: "a", OriginCountry: "GB", ObservedAt: time.Now()},
		"b": {Icao24: "b", OriginCountry: "DE", ObservedAt: time.Now()},
	}}
	h := newTestHandler(s)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/flights/active?origin_country=GB", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	var page ActiveAircraftPage
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if len(page.Aircraft) != 1 || page.Aircraft[0].OriginCountry != "GB" {
		t.Errorf("filter failed: got %+v", page.Aircraft)
	}
}

func TestGetTrack_BadLimit(t *testing.T) {
	h := newTestHandler(&fakeStore{})
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/aircraft/abc123/track?limit=9999", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for over-max limit", rec.Code)
	}
}

func TestGetTrack_BadCursor(t *testing.T) {
	h := newTestHandler(&fakeStore{})
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/aircraft/abc123/track?cursor=not-a-valid-cursor", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for bad cursor", rec.Code)
	}
}

func TestGetTrack_OK(t *testing.T) {
	s := &fakeStore{track: map[string][]Telemetry{
		"abc123": {
			{EventID: "1", Icao24: "abc123", OriginCountry: "GB", ObservedAt: time.Now()},
			{EventID: "2", Icao24: "abc123", OriginCountry: "GB", ObservedAt: time.Now().Add(-1 * time.Minute)},
		},
	}}
	h := newTestHandler(s)
	r := newRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/aircraft/abc123/track", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var page TrackPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Telemetry) != 2 {
		t.Errorf("got %d telemetry rows, want 2", len(page.Telemetry))
	}
}
