package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
	"github.com/onbloc/gno-ibc-relayer-api/internal/indexer"
	"github.com/onbloc/gno-ibc-relayer-api/internal/server"
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

	bridgeDBPool, err := db.NewPool(ctx, cfg.BridgeDB)
	if err != nil {
		log.Fatalf("bridge db: %v", err)
	}
	defer bridgeDBPool.Close()

	bridgeDB := db.New(bridgeDBPool)

	idx := indexer.New(relayerDB, bridgeDB, cfg.Indexer, cfg.ChannelChains)
	go idx.Run(ctx)

	srv := server.New(cfg.Server, bridgeDB)
	log.Printf("server: listening on :%d", cfg.Server.Port)
	if err := srv.Run(); err != nil {
		log.Fatalf("server: %v", err)
	}
}
