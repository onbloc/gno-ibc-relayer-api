package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/onbloc/gno-ibc-relayer-api/internal/repository"
)

type TransferHandler struct {
	repo *repository.TransferRepo
}

func NewTransferHandler(repo *repository.TransferRepo) *TransferHandler {
	return &TransferHandler{repo: repo}
}

// GET /status/{packet_hash}
func (h *TransferHandler) GetByPacketHash(w http.ResponseWriter, r *http.Request) {
	t, err := h.repo.GetByPacketHash(r.Context(), chi.URLParam(r, "packet_hash"))
	if err != nil {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}
	jsonOK(w, t)
}

// GET /wallet/{sender_address}?limit=&orderby=
func (h *TransferHandler) ListByWallet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, offset := parsePagination(q)

	transfers, err := h.repo.List(r.Context(), repository.ListFilter{
		Address: chi.URLParam(r, "sender_address"),
		Order:   q.Get("orderby"),
		Limit:   limit,
		Offset:  offset,
	})
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"data": transfers, "limit": limit, "offset": offset})
}

// GET /history?limit=&orderby=
func (h *TransferHandler) History(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, offset := parsePagination(q)

	transfers, err := h.repo.List(r.Context(), repository.ListFilter{
		Order:  q.Get("orderby"),
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, map[string]any{"data": transfers, "limit": limit, "offset": offset})
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
