package handler

import (
	"net/http"

	"github.com/onbloc/gno-ibc-relayer-api/internal/repository"
)

type StatsHandler struct {
	repo *repository.TransferRepo
}

func NewStatsHandler(repo *repository.TransferRepo) *StatsHandler {
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
