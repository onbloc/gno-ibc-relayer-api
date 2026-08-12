package indexer

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

// RelayerDB is the subset of *pgxpool.Pool the indexer needs to read the relayer's own
// queue/done/failed tables and listen for their NOTIFY events. Narrowed here so tests can
// inject a fake instead of a real database.
type RelayerDB interface {
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// BridgeDB is the subset of *db.BridgeDB the indexer depends on to track bridges.
// Narrowed here so tests can inject a fake instead of a real database.
type BridgeDB interface {
	DedupeByPacketHash(ctx context.Context) (int64, error)
	Insert(ctx context.Context, t *db.BridgeRecord) error
	MarkProcessing(ctx context.Context, ids []int64) error
	MarkDone(ctx context.Context, id int64, doneAt time.Time, txIn string) error
	SetTxIn(ctx context.Context, packetHash, txIn string) error
	MarkFailed(ctx context.Context, id int64, errMsg string, txIn string) error
	GetCursor(ctx context.Context, name string) (int64, error)
	SetCursor(ctx context.Context, name string, id int64) error
	FindByTimeoutAndChannel(ctx context.Context, timeoutTimestamp int64, srcChannelID int) (int64, error)
	FindByPacketHash(ctx context.Context, packetHash string) (int64, error)
	FindAncestor(ctx context.Context, ids []int64) (int64, error)
	GetDetectedIDs(ctx context.Context) ([]int64, error)
	GetInFlightCreatedAt(ctx context.Context) ([]time.Time, error)
}

type Indexer struct {
	relayerDB RelayerDB
	bridgeDB  BridgeDB
	cfg       config.IndexerConfig
	chains    []config.ChannelChain
}

func New(relayerDB RelayerDB, bridgeDB BridgeDB, cfg config.IndexerConfig, chains []config.ChannelChain) *Indexer {
	return &Indexer{relayerDB: relayerDB, bridgeDB: bridgeDB, cfg: cfg, chains: chains}
}

func (idx *Indexer) Run(ctx context.Context) {
	log.Println("indexer: started")

	if n, err := idx.bridgeDB.DedupeByPacketHash(ctx); err != nil {
		log.Printf("indexer: startup dedupe: %v", err)
	} else if n > 0 {
		log.Printf("indexer: startup dedupe removed %d duplicate bridge(s)", n)
	}

	if err := idx.syncQueue(ctx); err != nil {
		log.Printf("indexer: startup sync queue: %v", err)
	}
	if err := idx.syncDone(ctx); err != nil {
		log.Printf("indexer: startup sync done: %v", err)
	}
	if err := idx.syncFailed(ctx); err != nil {
		log.Printf("indexer: startup sync failed: %v", err)
	}

	go idx.listenQueue(ctx)
	go idx.listenDone(ctx)
	go idx.listenFailed(ctx)

	ticker := time.NewTicker(time.Duration(idx.cfg.PollIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("indexer: stopped")
			return
		case <-ticker.C:
			if err := idx.poll(ctx); err != nil {
				log.Printf("indexer: poll error: %v", err)
			}
		}
	}
}
