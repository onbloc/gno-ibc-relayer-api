package handler

import (
	"net/http"
)

type StatsHandler struct {
	repo transferRepository
}

func NewStatsHandler(repo transferRepository) *StatsHandler {
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
