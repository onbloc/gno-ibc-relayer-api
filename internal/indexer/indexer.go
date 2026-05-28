package indexer

import (
	"context"
	"fmt"
	"log"
	"strconv"
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
	log.Println("indexer: started")

	// Catch up on items inserted while the app was down.
	if err := idx.syncQueue(ctx); err != nil {
		log.Printf("indexer: startup sync queue: %v", err)
	}

	go idx.listenQueue(ctx)

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

// poll handles state transitions — queue insert is covered by LISTEN/NOTIFY.
func (idx *Indexer) poll(ctx context.Context) error {
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

// listenQueue maintains a LISTEN connection and reconnects on error.
func (idx *Indexer) listenQueue(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := idx.runListener(ctx); err != nil {
			log.Printf("indexer: listener error (reconnecting in 5s): %v", err)
			// Catch up on missed items before reconnecting.
			if syncErr := idx.syncQueue(ctx); syncErr != nil {
				log.Printf("indexer: reconnect sync queue: %v", syncErr)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// runListener blocks until the context is cancelled or the connection breaks.
func (idx *Indexer) runListener(ctx context.Context) error {
	conn, err := idx.relayerDB.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN queue_insert"); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	log.Println("indexer: listening on queue_insert channel")

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("wait: %w", err)
		}

		id, err := strconv.ParseInt(notification.Payload, 10, 64)
		if err != nil {
			log.Printf("indexer: listen bad payload %q: %v", notification.Payload, err)
			continue
		}

		var item []byte
		var createdAt time.Time
		if err := idx.relayerDB.QueryRow(ctx,
			`SELECT item, created_at FROM queue WHERE id = $1`, id,
		).Scan(&item, &createdAt); err != nil {
			// Item already picked up by Voyager before we could read it.
			// syncDone/syncFailed will handle it in the next poll cycle.
			log.Printf("indexer: queue id=%d gone before read (syncDone will catch it)", id)
			continue
		}

		t, err := parser.Parse(id, item, createdAt, idx.chains)
		if err != nil {
			log.Printf("indexer: listen parse id=%d: %v", id, err)
			continue
		}
		if t != nil {
			if err := idx.repo.Insert(ctx, t); err != nil {
				log.Printf("indexer: listen insert id=%d: %v", id, err)
			} else {
				log.Printf("indexer: detected id=%d via notify", id)
			}
		}
	}
}

// syncQueue reads new items from the relayer queue using a cursor and inserts
// them as status=detected. Used on startup and after listener reconnects.
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
		if err := idx.repo.SetCursor(ctx, "done", lastID); err != nil {
			return err
		}
		if _, err := idx.relayerDB.Exec(ctx,
			`DELETE FROM done WHERE id <= $1`, lastID,
		); err != nil {
			log.Printf("indexer: cleanup done id<=%d: %v", lastID, err)
		}
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
