package indexer

import (
	"context"
	"fmt"
	"log"
	"time"
)

func (idx *Indexer) poll(ctx context.Context) error {
	return idx.syncProcessing(ctx)
}

func (idx *Indexer) syncQueue(ctx context.Context) error {
	cursor, err := idx.bridgeDB.GetCursor(ctx, "queue")
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
			if err := idx.bridgeDB.Insert(ctx, t); err != nil {
				return fmt.Errorf("insert id=%d: %w", id, err)
			}
		}
		lastID = id
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if lastID > 0 {
		return idx.bridgeDB.SetCursor(ctx, "queue", lastID)
	}
	return nil
}

func (idx *Indexer) syncProcessing(ctx context.Context) error {
	detectedIDs, err := idx.bridgeDB.GetDetectedIDs(ctx)
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
		return idx.bridgeDB.MarkProcessing(ctx, gone)
	}
	return nil
}

func (idx *Indexer) syncDone(ctx context.Context) error {
	createdAts, err := idx.bridgeDB.GetInFlightCreatedAt(ctx)
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

		bridgeID, err := idx.bridgeDB.FindByPacketHash(ctx, fields.PacketHash)
		if err != nil || bridgeID == 0 {
			continue
		}

		switch fields.EventType {
		case "packet_recv":
			if err := idx.bridgeDB.SetTxIn(ctx, fields.PacketHash, fields.TxHash); err != nil {
				log.Printf("indexer: startup set tx_in id=%d: %v", bridgeID, err)
			}
		case "write_ack":
			idx.markWriteAckResult(ctx, bridgeID, createdAt, fields, "startup catch-up")
		}
	}
	return rows.Err()
}

func (idx *Indexer) syncFailed(ctx context.Context) error {
	cursor, err := idx.bridgeDB.GetCursor(ctx, "failed")
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

		if _, err := idx.bridgeDB.MarkFailed(ctx, id, errMsg, ""); err != nil {
			// A transient failure here must not advance the cursor past this row —
			// otherwise it would never be retried. Stop the batch; the next poll
			// picks back up from the last successfully-marked id.
			log.Printf("indexer: mark failed id=%d: %v", id, err)
			break
		}
		lastID = id

		ancestorID, err := idx.traceFailedAncestor(ctx, parents)
		if err != nil {
			log.Printf("indexer: trace ancestor id=%d: %v", id, err)
			continue
		}
		if ancestorID == 0 || ancestorID == id {
			continue
		}
		if _, err := idx.bridgeDB.MarkFailed(ctx, ancestorID, errMsg, ""); err != nil {
			log.Printf("indexer: mark failed ancestor id=%d: %v", ancestorID, err)
			continue
		}
		log.Printf("indexer: marked origin bridge id=%d failed via descendant id=%d", ancestorID, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if lastID > 0 {
		return idx.bridgeDB.SetCursor(ctx, "failed", lastID)
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
		if id, err := idx.bridgeDB.FindAncestor(ctx, current); err != nil {
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
