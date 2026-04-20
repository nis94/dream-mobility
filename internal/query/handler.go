package query

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

// dataStore is the handler's narrow dependency on the query store.
// Defined here (consumer side) so tests can supply a fake without a pgx pool.
type dataStore interface {
	GetPosition(ctx context.Context, entityType, entityID string) (*Position, error)
	ListEvents(ctx context.Context, entityType, entityID string, cursor time.Time, limit int) (*EventsPage, error)
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

	pos, err := h.store.GetPosition(r.Context(), entityType, entityID)
	if errors.Is(err, ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "no position data for " + entityType + ":" + entityID,
		})
		return
	}
	if err != nil {
		h.logger.Error("get position failed", "entity", entityType+":"+entityID, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, pos)
}

// ListEvents handles GET /entities/{type}/{id}/events?cursor=&limit=.
func (h *Handler) ListEvents(w http.ResponseWriter, r *http.Request) {
	entityType := chi.URLParam(r, "type")
	entityID := chi.URLParam(r, "id")

	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}

	var cursor time.Time
	if v := r.URL.Query().Get("cursor"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "cursor must be RFC3339Nano timestamp",
			})
			return
		}
		cursor = t
	}

	page, err := h.store.ListEvents(r.Context(), entityType, entityID, cursor, limit)
	if err != nil {
		h.logger.Error("list events failed", "entity", entityType+":"+entityID, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, page)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
