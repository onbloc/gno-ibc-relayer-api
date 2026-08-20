package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

type BridgeDB interface {
	GetByPacketHash(ctx context.Context, packetHash string) (*db.BridgeRecord, error)
	List(ctx context.Context, f db.ListFilter) ([]*db.BridgeRecord, error)
	Count(ctx context.Context) (int64, error)
	CountRecentByStatus(ctx context.Context, limit int) (*db.StatusSummary, error)
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

const (
	defaultRecentLimit = 1000
	maxRecentLimit     = 5000
	recentCacheTTL     = 5 * time.Second
)

type StatsHandler struct {
	repo  BridgeDB
	cache *ttlCache[int, *db.StatusSummary]
}

func NewStatsHandler(repo BridgeDB) *StatsHandler {
	return &StatsHandler{repo: repo, cache: newTTLCache[int, *db.StatusSummary](recentCacheTTL)}
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

// GET /summary/recent?limit=1000
// Result is cached for recentCacheTTL to avoid re-running the aggregate on every poll.
func (h *StatsHandler) RecentSummary(w http.ResponseWriter, r *http.Request) {
	limit := parseRecentLimit(r.URL.Query())

	s, err := h.cache.getOrLoad(limit, func() (*db.StatusSummary, error) {
		return h.repo.CountRecentByStatus(r.Context(), limit)
	})
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, s)
}

func parseRecentLimit(q interface{ Get(string) string }) int {
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > maxRecentLimit {
		limit = defaultRecentLimit
	}
	return limit
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
