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

// GET /stats
func (h *StatsHandler) Get(w http.ResponseWriter, r *http.Request) {
	stats, err := h.repo.GetStats(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonOK(w, stats)
}
