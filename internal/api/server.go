package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/onbloc/gno-ibc-relayer-api/internal/api/handler"
	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/repository"
)

type Server struct {
	cfg config.ServerConfig
	mux *chi.Mux
}

func New(cfg config.ServerConfig, repo *repository.TransferRepo) *Server {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	th := handler.NewTransferHandler(repo)
	sh := handler.NewStatsHandler(repo)

	r.Get("/transfers", th.List)
	r.Get("/transfers/{id}", th.GetByID)
	r.Get("/stats", sh.Get)

	return &Server{cfg: cfg, mux: r}
}

func (s *Server) Run() error {
	addr := fmt.Sprintf(":%d", s.cfg.Port)
	return http.ListenAndServe(addr, s.mux)
}
