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
	GetPosition(ctx context.Context, entityType, entityID string) (*Position, error)
	ListEvents(ctx context.Context, entityType, entityID string, cursor Cursor, limit int) (*EventsPage, error)
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

// GetPosition handles GET /entities/{type}/{id}/position.
func (h *Handler) GetPosition(w http.ResponseWriter, r *http.Request) {
	entityType := chi.URLParam(r, "type")
	entityID := chi.URLParam(r, "id")
	entityKey := entityType + ":" + entityID

	pos, err := h.store.GetPosition(r.Context(), entityType, entityID)
	if errors.Is(err, ErrNotFound) {
		h.writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "no position data for " + entityKey,
		})
		return
	}
	if err != nil {
		h.logger.Error("get position failed", "entity", entityKey, "err", err)
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	h.writeJSON(w, http.StatusOK, pos)
}

// ListEvents handles GET /entities/{type}/{id}/events?cursor=&limit=.
//
// `limit` must parse as an integer in [1, 1000]; malformed or out-of-range
// values return 400 with a descriptive error. `cursor` must be an opaque
// token previously returned by the API (base64url composite of event_ts +
// event_id); malformed cursors return 400.
func (h *Handler) ListEvents(w http.ResponseWriter, r *http.Request) {
	entityType := chi.URLParam(r, "type")
	entityID := chi.URLParam(r, "id")
	entityKey := entityType + ":" + entityID

	limit := defaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > maxLimit {
			h.writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "limit must be an integer in [1, 1000]",
			})
			return
		}
		limit = n
	}

	cursor, err := DecodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "cursor: " + err.Error(),
		})
		return
	}

	page, err := h.store.ListEvents(r.Context(), entityType, entityID, cursor, limit)
	if err != nil {
		h.logger.Error("list events failed", "entity", entityKey, "err", err)
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	h.writeJSON(w, http.StatusOK, page)
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
