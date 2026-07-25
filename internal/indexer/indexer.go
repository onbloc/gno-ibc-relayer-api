package indexer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

type Indexer struct {
	relayerDB *pgxpool.Pool
	repo      *db.Store
	cfg       config.IndexerConfig
	chains    []config.ChannelChain
}

func New(relayerDB *pgxpool.Pool, repo *db.Store, cfg config.IndexerConfig, chains []config.ChannelChain) *Indexer {
	return &Indexer{relayerDB: relayerDB, repo: repo, cfg: cfg, chains: chains}
}

func (idx *Indexer) Run(ctx context.Context) {
	log.Println("indexer: started")

	if n, err := idx.repo.DedupeByPacketHash(ctx); err != nil {
		log.Printf("indexer: startup dedupe: %v", err)
	} else if n > 0 {
		log.Printf("indexer: startup dedupe removed %d duplicate transfer(s)", n)
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

func (idx *Indexer) poll(ctx context.Context) error {
	return idx.syncProcessing(ctx)
}

func (idx *Indexer) listenQueue(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := idx.runListener(ctx); err != nil {
			log.Printf("indexer: listener error (reconnecting in 5s): %v", err)
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
			log.Printf("indexer: queue id=%d gone before read (syncDone will catch it)", id)
			continue
		}

		t, err := Parse(id, item, createdAt, idx.chains)
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

		fields := ParseItemFields(item)
		if fields != nil && (fields.EventType == "packet_recv" || fields.EventType == "write_ack") {
			transferID, err := idx.repo.FindByPacketHash(ctx, fields.PacketHash)
			if err != nil {
				log.Printf("indexer: done find transfer (%s): %v", fields.EventType, err)
			} else if transferID != 0 {
				switch fields.EventType {
				case "packet_recv":
					if err := idx.repo.SetTxIn(ctx, fields.PacketHash, fields.TxHash); err != nil {
						log.Printf("indexer: set tx_in id=%d: %v", transferID, err)
					}
				case "write_ack":
					if fields.AckSuccess {
						if err := idx.repo.MarkDone(ctx, transferID, createdAt, fields.TxHash); err != nil {
							log.Printf("indexer: done mark id=%d: %v", transferID, err)
						} else {
							log.Printf("indexer: done transfer id=%d via write_ack notify", transferID)
						}
					} else {
						if err := idx.repo.MarkFailed(ctx, transferID, ackErrMessage(fields.AckError)); err != nil {
							log.Printf("indexer: ack error mark failed id=%d: %v", transferID, err)
						} else {
							log.Printf("indexer: transfer id=%d failed via write_ack ack error notify", transferID)
						}
					}
				}
			}
		}

		for _, pt := range ParsePacketTimeouts(item) {
			idx.markPacketTimeoutFailed(ctx, pt, "notify")
		}
	}
}

// markPacketTimeoutFailed marks the transfer matching a packet_timeout datagram
// as failed — the destination chain rejected the packet, so it was refunded on
// the source chain instead of completing.
func (idx *Indexer) markPacketTimeoutFailed(ctx context.Context, pt PacketTimeoutFields, via string) {
	transferID, err := idx.repo.FindByTimeoutAndChannel(ctx, pt.TimeoutTimestamp, pt.SrcChannelID)
	if err != nil {
		log.Printf("indexer: packet_timeout find transfer timeout=%d ch=%d: %v", pt.TimeoutTimestamp, pt.SrcChannelID, err)
		return
	}
	if transferID == 0 {
		return
	}
	if err := idx.repo.MarkFailed(ctx, transferID, packetTimeoutErrMsg); err != nil {
		log.Printf("indexer: packet_timeout mark failed id=%d: %v", transferID, err)
	} else {
		log.Printf("indexer: transfer id=%d failed via packet_timeout %s", transferID, via)
	}
}

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

		if err := idx.repo.MarkFailed(ctx, id, errMsg); err == nil {
			log.Printf("indexer: failed transfer id=%d (direct)", id)
			continue
		}

		fields := ParseItemFields(item)
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

// packetTimeoutErrMsg is stored in err_msg when a packet_timeout datagram
// refunds the transfer on the source chain after the destination chain rejected it.
const packetTimeoutErrMsg = "packet timed out; refunded on source chain"

// ackErrMessage formats a write_ack TAG_ACK_FAILURE's decoded inner_ack for storage in err_msg.
func ackErrMessage(ackErr string) string {
	if ackErr == "" {
		return "ack error"
	}
	return "ack error: " + ackErr
}

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

		t, err := Parse(id, item, createdAt, idx.chains)
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

func (idx *Indexer) syncDone(ctx context.Context) error {
	createdAts, err := idx.repo.GetInFlightCreatedAt(ctx)
	if err != nil || len(createdAts) == 0 {
		return err
	}

	var oldest time.Time
	for _, t := range createdAts {
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}

	rows, err := idx.relayerDB.Query(ctx,
		`SELECT item, created_at FROM done
		 WHERE (item::text LIKE '%packet_recv%' OR item::text LIKE '%write_ack%' OR item::text LIKE '%packet_timeout%')
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

		for _, pt := range ParsePacketTimeouts(item) {
			idx.markPacketTimeoutFailed(ctx, pt, "startup catch-up")
		}

		fields := ParseItemFields(item)
		if fields == nil || (fields.EventType != "packet_recv" && fields.EventType != "write_ack") {
			continue
		}

		transferID, err := idx.repo.FindByPacketHash(ctx, fields.PacketHash)
		if err != nil || transferID == 0 {
			continue
		}

		switch fields.EventType {
		case "packet_recv":
			if err := idx.repo.SetTxIn(ctx, fields.PacketHash, fields.TxHash); err != nil {
				log.Printf("indexer: startup set tx_in id=%d: %v", transferID, err)
			}
		case "write_ack":
			if fields.AckSuccess {
				if err := idx.repo.MarkDone(ctx, transferID, createdAt, fields.TxHash); err != nil {
					log.Printf("indexer: startup mark done id=%d: %v", transferID, err)
				} else {
					log.Printf("indexer: startup caught done id=%d via write_ack", transferID)
				}
			} else {
				if err := idx.repo.MarkFailed(ctx, transferID, ackErrMessage(fields.AckError)); err != nil {
					log.Printf("indexer: startup mark ack error failed id=%d: %v", transferID, err)
				} else {
					log.Printf("indexer: startup caught ack error id=%d via write_ack", transferID)
				}
			}
		}
	}
	return rows.Err()
}

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

func (idx *Indexer) traceFailedAncestor(ctx context.Context, startParents []int64) (int64, error) {
	if len(startParents) == 0 {
		return 0, nil
	}

	visited := make(map[int64]bool)
	current := startParents

	for depth := 0; depth < 8 && len(current) > 0; depth++ {
		if id, err := idx.repo.FindAncestor(ctx, current); err != nil {
			return 0, err
		} else if id > 0 {
			return id, nil
		}

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
