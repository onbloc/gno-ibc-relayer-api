package indexer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/parser"
	"github.com/onbloc/gno-ibc-relayer-api/internal/repository"
)

type Indexer struct {
	relayerDB *pgxpool.Pool
	repo      *repository.TransferRepo
	cfg       config.IndexerConfig
	chains    []config.ChannelChain
}

func New(relayerDB *pgxpool.Pool, repo *repository.TransferRepo, cfg config.IndexerConfig, chains []config.ChannelChain) *Indexer {
	return &Indexer{relayerDB: relayerDB, repo: repo, cfg: cfg, chains: chains}
}

func (idx *Indexer) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(idx.cfg.PollIntervalSec) * time.Second)
	defer ticker.Stop()
	log.Println("indexer: started")

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

func (idx *Indexer) poll(ctx context.Context) error {
	if err := idx.syncQueue(ctx); err != nil {
		return fmt.Errorf("sync queue: %w", err)
	}
	if err := idx.syncProcessing(ctx); err != nil {
		return fmt.Errorf("sync processing: %w", err)
	}
	if err := idx.syncDone(ctx); err != nil {
		return fmt.Errorf("sync done: %w", err)
	}
	if err := idx.syncFailed(ctx); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	return nil
}

// syncQueue reads new packet_send events from the relayer queue and inserts
// them as status=detected (0).
func (idx *Indexer) syncQueue(ctx context.Context) error {
	cursor, err := idx.repo.GetCursor(ctx, "queue")
	if err != nil {
		return err
	}

	rows, err := idx.relayerDB.Query(ctx,
		`SELECT id, item, created_at FROM queue WHERE id > $1 ORDER BY id LIMIT $2`,
		cursor, idx.cfg.BatchSize,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var lastID int64
	for rows.Next() {
		var id int64
		var item []byte
		var createdAt time.Time
		if err := rows.Scan(&id, &item, &createdAt); err != nil {
			return err
		}

		t, err := parser.Parse(id, item, createdAt, idx.chains)
		if err != nil {
			log.Printf("indexer: parse queue id=%d: %v", id, err)
		} else if t != nil {
			if err := idx.repo.Insert(ctx, t); err != nil {
				return fmt.Errorf("insert id=%d: %w", id, err)
			}
		}
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if lastID > 0 {
		return idx.repo.SetCursor(ctx, "queue", lastID)
	}
	return nil
}

// syncProcessing detects transfers that are no longer in the relayer queue
// (picked up for processing) and marks them as status=processing (1).
func (idx *Indexer) syncProcessing(ctx context.Context) error {
	detectedIDs, err := idx.repo.GetDetectedIDs(ctx)
	if err != nil || len(detectedIDs) == 0 {
		return err
	}

	// check which IDs are still in the relayer queue
	rows, err := idx.relayerDB.Query(ctx,
		`SELECT id FROM queue WHERE id = ANY($1)`, detectedIDs,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	inQueue := make(map[int64]bool)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		inQueue[id] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// IDs no longer in queue → mark processing
	var gone []int64
	for _, id := range detectedIDs {
		if !inQueue[id] {
			gone = append(gone, id)
		}
	}
	if len(gone) > 0 {
		return idx.repo.MarkProcessing(ctx, gone)
	}
	return nil
}

// syncDone reads new rows from the relayer done table and marks transfers done.
func (idx *Indexer) syncDone(ctx context.Context) error {
	cursor, err := idx.repo.GetCursor(ctx, "done")
	if err != nil {
		return err
	}

	rows, err := idx.relayerDB.Query(ctx,
		`SELECT id, created_at FROM done WHERE id > $1 ORDER BY id LIMIT $2`,
		cursor, idx.cfg.BatchSize,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var lastID int64
	for rows.Next() {
		var id int64
		var createdAt time.Time
		if err := rows.Scan(&id, &createdAt); err != nil {
			return err
		}
		if err := idx.repo.MarkDone(ctx, id, createdAt); err != nil {
			log.Printf("indexer: mark done id=%d: %v", id, err)
		}
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if lastID > 0 {
		return idx.repo.SetCursor(ctx, "done", lastID)
	}
	return nil
}

// syncFailed reads new rows from the relayer failed table and marks transfers failed.
func (idx *Indexer) syncFailed(ctx context.Context) error {
	cursor, err := idx.repo.GetCursor(ctx, "failed")
	if err != nil {
		return err
	}

	rows, err := idx.relayerDB.Query(ctx,
		`SELECT id FROM failed WHERE id > $1 ORDER BY id LIMIT $2`,
		cursor, idx.cfg.BatchSize,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var lastID int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if err := idx.repo.MarkFailed(ctx, id); err != nil {
			log.Printf("indexer: mark failed id=%d: %v", id, err)
		}
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if lastID > 0 {
		return idx.repo.SetCursor(ctx, "failed", lastID)
	}
	return nil
}
