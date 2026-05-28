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

// poll handles only processing state — done/failed are covered by LISTEN/NOTIFY.
func (idx *Indexer) poll(ctx context.Context) error {
	return idx.syncProcessing(ctx)
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

// listenDone maintains a LISTEN connection for done table inserts.
// Marks a transfer done when packet_recv appears in done with matching timeout+channel.
func (idx *Indexer) listenDone(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := idx.runDoneListener(ctx); err != nil {
			log.Printf("indexer: done listener error (reconnecting in 5s): %v", err)
			if syncErr := idx.syncDone(ctx); syncErr != nil {
				log.Printf("indexer: reconnect sync done: %v", syncErr)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (idx *Indexer) runDoneListener(ctx context.Context) error {
	conn, err := idx.relayerDB.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN done_insert"); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	log.Println("indexer: listening on done_insert channel")

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
			log.Printf("indexer: done bad payload %q: %v", notification.Payload, err)
			continue
		}

		var item []byte
		var createdAt time.Time
		if err := idx.relayerDB.QueryRow(ctx,
			`SELECT item, created_at FROM done WHERE id = $1`, id,
		).Scan(&item, &createdAt); err != nil {
			continue
		}

		fields := parser.ParseItemFields(item)
		if fields == nil || fields.EventType != "packet_recv" {
			continue
		}

		transferID, err := idx.repo.FindByTimeoutAndChannel(ctx, fields.TimeoutTimestamp, fields.SrcChannelID)
		if err != nil {
			log.Printf("indexer: done find transfer timeout=%d ch=%d: %v", fields.TimeoutTimestamp, fields.SrcChannelID, err)
			continue
		}
		if transferID == 0 {
			continue
		}
		if err := idx.repo.MarkDone(ctx, transferID, createdAt); err != nil {
			log.Printf("indexer: done mark id=%d: %v", transferID, err)
		} else {
			log.Printf("indexer: done transfer id=%d via packet_recv notify", transferID)
		}
	}
}

// listenFailed maintains a LISTEN connection for failed table inserts.
// Marks a transfer failed by matching timeout+channel from the embedded packet info.
func (idx *Indexer) listenFailed(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := idx.runFailedListener(ctx); err != nil {
			log.Printf("indexer: failed listener error (reconnecting in 5s): %v", err)
			if syncErr := idx.syncFailed(ctx); syncErr != nil {
				log.Printf("indexer: reconnect sync failed: %v", syncErr)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (idx *Indexer) runFailedListener(ctx context.Context) error {
	conn, err := idx.relayerDB.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN failed_insert"); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	log.Println("indexer: listening on failed_insert channel")

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
			log.Printf("indexer: failed bad payload %q: %v", notification.Payload, err)
			continue
		}

		var item []byte
		var errMsg string
		if err := idx.relayerDB.QueryRow(ctx,
			`SELECT item, message FROM failed WHERE id = $1`, id,
		).Scan(&item, &errMsg); err != nil {
			continue
		}

		// Try direct id match first (e.g. the original packet item itself failed).
		if err := idx.repo.MarkFailed(ctx, id, errMsg); err == nil {
			log.Printf("indexer: failed transfer id=%d (direct)", id)
			continue
		}

		// Fall back to timeout+channel match from embedded packet info.
		fields := parser.ParseItemFields(item)
		if fields == nil {
			continue
		}
		transferID, err := idx.repo.FindByTimeoutAndChannel(ctx, fields.TimeoutTimestamp, fields.SrcChannelID)
		if err != nil {
			log.Printf("indexer: failed find transfer timeout=%d ch=%d: %v", fields.TimeoutTimestamp, fields.SrcChannelID, err)
			continue
		}
		if transferID == 0 {
			continue
		}
		if err := idx.repo.MarkFailed(ctx, transferID, errMsg); err != nil {
			log.Printf("indexer: failed mark id=%d: %v", transferID, err)
		} else {
			log.Printf("indexer: failed transfer id=%d via promise notify", transferID)
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

// syncDone is a startup/reconnect catch-up that scans done for packet_recv items
// matching our in-flight transfers by timeout_timestamp.
func (idx *Indexer) syncDone(ctx context.Context) error {
	inFlight, err := idx.repo.GetInFlight(ctx)
	if err != nil || len(inFlight) == 0 {
		return err
	}

	// Build timeout → transfer lookup.
	timeoutMap := make(map[int64]repository.InFlightTransfer, len(inFlight))
	var oldest time.Time
	for _, t := range inFlight {
		timeoutMap[t.TimeoutTimestamp] = t
		if oldest.IsZero() || t.CreatedAt.Before(oldest) {
			oldest = t.CreatedAt
		}
	}

	// Scan done for packet_recv items since the oldest in-flight transfer.
	rows, err := idx.relayerDB.Query(ctx,
		`SELECT item, created_at FROM done
		 WHERE item::text LIKE '%packet_recv%'
		 AND created_at >= $1`,
		oldest.Add(-time.Minute),
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var item []byte
		var createdAt time.Time
		if err := rows.Scan(&item, &createdAt); err != nil {
			return err
		}
		fields := parser.ParseItemFields(item)
		if fields == nil || fields.EventType != "packet_recv" {
			continue
		}
		t, ok := timeoutMap[fields.TimeoutTimestamp]
		if !ok {
			continue
		}
		if err := idx.repo.MarkDone(ctx, t.ID, createdAt); err != nil {
			log.Printf("indexer: startup mark done id=%d: %v", t.ID, err)
		} else {
			log.Printf("indexer: startup caught done id=%d", t.ID)
		}
	}
	return rows.Err()
}

// syncFailed reads new rows from the relayer failed table and marks transfers failed.
// If the failed item is not directly in our transfers (it may be a descendant op),
// it traverses the parents chain through the done table to find the origin transfer.
func (idx *Indexer) syncFailed(ctx context.Context) error {
	cursor, err := idx.repo.GetCursor(ctx, "failed")
	if err != nil {
		return err
	}

	rows, err := idx.relayerDB.Query(ctx,
		`SELECT id, parents, message FROM failed WHERE id > $1 ORDER BY id LIMIT $2`,
		cursor, idx.cfg.BatchSize,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var lastID int64
	for rows.Next() {
		var id int64
		var parents []int64
		var errMsg string
		if err := rows.Scan(&id, &parents, &errMsg); err != nil {
			return err
		}

		if err := idx.repo.MarkFailed(ctx, id, errMsg); err != nil {
			log.Printf("indexer: mark failed id=%d: %v", id, err)
		} else {
			// Direct match — also check ancestors in case a parent transfer
			// should be marked failed (e.g. the packet_send that spawned this op).
			if ancestorID, err := idx.traceFailedAncestor(ctx, parents); err != nil {
				log.Printf("indexer: trace ancestor id=%d: %v", id, err)
			} else if ancestorID > 0 && ancestorID != id {
				if err := idx.repo.MarkFailed(ctx, ancestorID, errMsg); err != nil {
					log.Printf("indexer: mark failed ancestor id=%d: %v", ancestorID, err)
				} else {
					log.Printf("indexer: marked origin transfer id=%d failed via descendant id=%d", ancestorID, id)
				}
			}
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

// traceFailedAncestor walks the parents chain through the done table (up to 8 hops)
// and returns the first ancestor id that exists in our transfers table.
func (idx *Indexer) traceFailedAncestor(ctx context.Context, startParents []int64) (int64, error) {
	if len(startParents) == 0 {
		return 0, nil
	}

	visited := make(map[int64]bool)
	current := startParents

	for depth := 0; depth < 8 && len(current) > 0; depth++ {
		// Check if any current id is in our transfers.
		if id, err := idx.repo.FindAncestor(ctx, current); err != nil {
			return 0, err
		} else if id > 0 {
			return id, nil
		}

		// Follow parents one level up via done table.
		var next []int64
		for _, pid := range current {
			if visited[pid] {
				continue
			}
			visited[pid] = true

			var grandparents []int64
			if err := idx.relayerDB.QueryRow(ctx,
				`SELECT parents FROM done WHERE id = $1`, pid,
			).Scan(&grandparents); err == nil {
				next = append(next, grandparents...)
			}
		}
		current = next
	}
	return 0, nil
}
