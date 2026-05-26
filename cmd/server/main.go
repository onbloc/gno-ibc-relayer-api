package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/onbloc/gno-ibc-relayer-api/internal/api"
	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
	"github.com/onbloc/gno-ibc-relayer-api/internal/indexer"
	"github.com/onbloc/gno-ibc-relayer-api/internal/repository"
)

func main() {
	cfgPath := flag.String("config", "config.toml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	relayerDB, err := db.NewPool(ctx, cfg.RelayerDB)
	if err != nil {
		log.Fatalf("relayer db: %v", err)
	}
	defer relayerDB.Close()

	appDB, err := db.NewPool(ctx, cfg.AppDB)
	if err != nil {
		log.Fatalf("app db: %v", err)
	}
	defer appDB.Close()

	repo := repository.NewTransferRepo(appDB)

	idx := indexer.New(relayerDB, repo, cfg.Indexer, cfg.ChannelChains)
	go idx.Run(ctx)

	srv := api.New(cfg.Server, repo)
	log.Printf("server: listening on :%d", cfg.Server.Port)
	if err := srv.Run(); err != nil {
		log.Fatalf("server: %v", err)
	}
}
