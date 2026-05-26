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

// GET /transfers/{id}
func (h *TransferHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "invalid id")
		return
	}

	t, err := h.repo.GetByID(r.Context(), id)
	if err != nil {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}

	jsonOK(w, t)
}

// GET /transfers?address=<addr>&status=<0-3>&order=asc|desc&limit=&offset=
//
// address is required — returns [] when omitted.
// status is optional; omit to get all statuses.
// order defaults to desc (newest first).
func (h *TransferHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	offset, _ := strconv.Atoi(q.Get("offset"))

	f := repository.ListFilter{
		Address: q.Get("address"),
		Order:   q.Get("order"),
		Limit:   limit,
		Offset:  offset,
	}
	if s := q.Get("status"); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			f.Status = &v
		}
	}

	transfers, err := h.repo.List(r.Context(), f)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	jsonOK(w, map[string]any{
		"data":   transfers,
		"limit":  limit,
		"offset": offset,
	})
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
