package query

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// dataStore is the handler's narrow dependency on the query store.
// Defined here (consumer side) so tests can supply a fake without a pgx pool.
type dataStore interface {
	GetAircraft(ctx context.Context, icao24 string) (*AircraftState, error)
	GetTrack(ctx context.Context, icao24 string, cursor Cursor, limit int) (*TrackPage, error)
	ListActive(ctx context.Context, originCountry string, cursor Cursor, limit int) (*ActiveAircraftPage, error)
}

// Handler serves the query HTTP endpoints.
type Handler struct {
	store  dataStore
	logger *slog.Logger
}

// NewHandler creates a Handler backed by the given store.
func NewHandler(s dataStore, logger *slog.Logger) *Handler {
	return &Handler{store: s, logger: logger}
}

// GetAircraft handles GET /aircraft/{icao24}.
// Returns the last-known state from aircraft_state.
func (h *Handler) GetAircraft(w http.ResponseWriter, r *http.Request) {
	icao24 := chi.URLParam(r, "icao24")

	a, err := h.store.GetAircraft(r.Context(), icao24)
	if errors.Is(err, ErrNotFound) {
		h.writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "no state for icao24=" + icao24,
		})
		return
	}
	if err != nil {
		h.logger.Error("get aircraft failed", "icao24", icao24, "err", err)
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	h.writeJSON(w, http.StatusOK, a)
}

// GetTrack handles GET /aircraft/{icao24}/track?cursor=&limit=.
// Returns paginated per-observation trajectory from flight_telemetry.
func (h *Handler) GetTrack(w http.ResponseWriter, r *http.Request) {
	icao24 := chi.URLParam(r, "icao24")

	limit, ok := h.parseLimit(w, r)
	if !ok {
		return
	}
	cursor, ok := h.parseCursor(w, r)
	if !ok {
		return
	}

	page, err := h.store.GetTrack(r.Context(), icao24, cursor, limit)
	if err != nil {
		h.logger.Error("get track failed", "icao24", icao24, "err", err)
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	h.writeJSON(w, http.StatusOK, page)
}

// ListActive handles GET /flights/active?origin_country=&cursor=&limit=.
// Returns aircraft whose latest observed_at is within the active window
// (5 minutes). Optional origin_country filter scopes the response.
func (h *Handler) ListActive(w http.ResponseWriter, r *http.Request) {
	origin := r.URL.Query().Get("origin_country")

	limit, ok := h.parseLimit(w, r)
	if !ok {
		return
	}
	cursor, ok := h.parseCursor(w, r)
	if !ok {
		return
	}

	page, err := h.store.ListActive(r.Context(), origin, cursor, limit)
	if err != nil {
		h.logger.Error("list active failed", "origin_country", origin, "err", err)
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	h.writeJSON(w, http.StatusOK, page)
}

func (h *Handler) parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return defaultLimit, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 || n > maxLimit {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "limit must be an integer in [1, 1000]",
		})
		return 0, false
	}
	return n, true
}

func (h *Handler) parseCursor(w http.ResponseWriter, r *http.Request) (Cursor, bool) {
	cursor, err := DecodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "cursor: " + err.Error(),
		})
		return Cursor{}, false
	}
	return cursor, true
}

// writeJSON sets the Content-Type and status, then encodes v. Encode errors
// after the status has been flushed cannot repair the response but are
// logged for operability (caller-disconnect mid-write, marshal failure).
func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Warn("response encode failed", "err", err)
	}
}
