package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

type BridgeDB interface {
	GetByPacketHash(ctx context.Context, packetHash string) (*db.BridgeRecord, error)
	List(ctx context.Context, f db.ListFilter) ([]*db.BridgeRecord, error)
	Count(ctx context.Context) (int64, error)
}

type BridgeHandler struct {
	repo BridgeDB
}

func NewBridgeHandler(repo BridgeDB) *BridgeHandler {
	return &BridgeHandler{repo: repo}
}

// GET /status/{packet_hash}
func (h *BridgeHandler) GetByPacketHash(w http.ResponseWriter, r *http.Request) {
	t, err := h.repo.GetByPacketHash(r.Context(), chi.URLParam(r, "packet_hash"))
	if err != nil {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}
	jsonOK(w, t)
}

// GET /wallet/{sender_address}?limit=&orderby=
func (h *BridgeHandler) ListByWallet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, offset := parsePagination(q)

	bridges, err := h.repo.List(r.Context(), db.ListFilter{
		Address: chi.URLParam(r, "sender_address"),
		Order:   q.Get("orderby"),
		Limit:   limit,
		Offset:  offset,
	})
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"data": bridges, "limit": limit, "offset": offset})
}

// GET /history?limit=&orderby=
func (h *BridgeHandler) History(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, offset := parsePagination(q)

	bridges, err := h.repo.List(r.Context(), db.ListFilter{
		Order:  q.Get("orderby"),
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"data": bridges, "limit": limit, "offset": offset})
}

type StatsHandler struct {
	repo BridgeDB
}

func NewStatsHandler(repo BridgeDB) *StatsHandler {
	return &StatsHandler{repo: repo}
}

// GET /summary
func (h *StatsHandler) Summary(w http.ResponseWriter, r *http.Request) {
	count, err := h.repo.Count(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"total": count})
}

func parsePagination(q interface{ Get(string) string }) (limit, offset int) {
	limit, _ = strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	offset, _ = strconv.Atoi(q.Get("offset"))
	return
}

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
