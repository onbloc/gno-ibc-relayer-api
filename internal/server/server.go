package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

type Server struct {
	cfg config.ServerConfig
	mux *chi.Mux
}

func New(cfg config.ServerConfig, repo *db.BridgeDB) *Server {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	th := NewBridgeHandler(repo)
	sh := NewStatsHandler(repo)

	r.Get("/status/{packet_hash}", th.GetByPacketHash)
	r.Get("/wallet/{sender_address}", th.ListByWallet)
	r.Get("/history", th.History)
	r.Get("/summary", sh.Summary)
	r.Get("/summary/recent", sh.RecentSummary)

	return &Server{cfg: cfg, mux: r}
}

func (s *Server) Run() error {
	addr := fmt.Sprintf(":%d", s.cfg.Port)
	return http.ListenAndServe(addr, s.mux)
}
