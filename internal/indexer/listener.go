package indexer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

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
			if err := idx.bridgeDB.Insert(ctx, t); err != nil {
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
			bridgeID, err := idx.bridgeDB.FindByPacketHash(ctx, fields.PacketHash)
			if err != nil {
				log.Printf("indexer: done find bridge (%s): %v", fields.EventType, err)
			} else if bridgeID != 0 {
				switch fields.EventType {
				case "packet_recv":
					if err := idx.bridgeDB.SetTxIn(ctx, fields.PacketHash, fields.TxHash); err != nil {
						log.Printf("indexer: set tx_in id=%d: %v", bridgeID, err)
					}
				case "write_ack":
					idx.markWriteAckResult(ctx, bridgeID, createdAt, fields, "notify")
				}
			}
		}

		for _, pt := range ParsePacketTimeouts(item) {
			idx.markPacketTimeoutFailed(ctx, pt, "notify")
		}
	}
}

// markPacketTimeoutFailed marks the bridge failed — the destination chain rejected the
// packet, refunding it on the source chain.
func (idx *Indexer) markPacketTimeoutFailed(ctx context.Context, pt PacketTimeoutFields, via string) {
	bridgeID, err := idx.bridgeDB.FindByTimeoutAndChannel(ctx, pt.TimeoutTimestamp, pt.SrcChannelID)
	if err != nil {
		log.Printf("indexer: packet_timeout find bridge timeout=%d ch=%d: %v", pt.TimeoutTimestamp, pt.SrcChannelID, err)
		return
	}
	if bridgeID == 0 {
		return
	}
	matched, err := idx.bridgeDB.MarkFailed(ctx, bridgeID, "packet timed out; refunded on source chain", "")
	if err != nil {
		log.Printf("indexer: packet_timeout mark failed id=%d: %v", bridgeID, err)
		return
	}
	if !matched {
		return // already done/failed; must not be overwritten
	}
	log.Printf("indexer: bridge id=%d failed via packet_timeout %s", bridgeID, via)
}

// markWriteAckResult marks the bridge done or failed based on the decoded write_ack outcome.
func (idx *Indexer) markWriteAckResult(ctx context.Context, bridgeID int64, createdAt time.Time, fields *ItemFields, via string) {
	if fields.AckSuccess {
		matched, err := idx.bridgeDB.MarkDone(ctx, bridgeID, createdAt, fields.TxHash)
		if err != nil {
			log.Printf("indexer: write_ack mark done id=%d (%s): %v", bridgeID, via, err)
			return
		}
		if !matched {
			return // already done/failed; nothing changed
		}
		log.Printf("indexer: bridge id=%d done via write_ack %s", bridgeID, via)
		return
	}

	matched, err := idx.bridgeDB.MarkFailed(ctx, bridgeID, ackErrMessage(fields.AckError), fields.TxHash)
	if err != nil {
		log.Printf("indexer: write_ack mark ack error failed id=%d (%s): %v", bridgeID, via, err)
		return
	}
	if !matched {
		return // already done/failed; must not be overwritten
	}
	log.Printf("indexer: bridge id=%d failed via write_ack ack error %s", bridgeID, via)
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

		idx.handleFailedNotification(ctx, id, item, errMsg)
	}
}

// handleFailedNotification marks the bridge matching a failed-table row failed. It first
// tries id directly (the common case for non-batched items); if that doesn't match any
// tracked bridge, it falls back to matching the promise-batch item by
// timeout_timestamp + src_channel_id.
func (idx *Indexer) handleFailedNotification(ctx context.Context, id int64, item []byte, errMsg string) {
	matched, err := idx.bridgeDB.MarkFailed(ctx, id, errMsg, "")
	if err != nil {
		log.Printf("indexer: failed mark id=%d: %v", id, err)
		return
	}
	if matched {
		log.Printf("indexer: failed bridge id=%d (direct)", id)
		return
	}

	fields := ParseItemFields(item)
	if fields == nil {
		return
	}
	bridgeID, err := idx.bridgeDB.FindByTimeoutAndChannel(ctx, fields.TimeoutTimestamp, fields.SrcChannelID)
	if err != nil {
		log.Printf("indexer: failed find bridge timeout=%d ch=%d: %v", fields.TimeoutTimestamp, fields.SrcChannelID, err)
		return
	}
	if bridgeID == 0 {
		return
	}
	matched, err = idx.bridgeDB.MarkFailed(ctx, bridgeID, errMsg, fields.TxHash)
	if err != nil {
		log.Printf("indexer: failed mark id=%d: %v", bridgeID, err)
		return
	}
	if !matched {
		return // already done/failed; must not be overwritten
	}
	log.Printf("indexer: failed bridge id=%d via promise notify", bridgeID)
}

// ackErrMessage formats a write_ack TAG_ACK_FAILURE's decoded inner_ack for storage in err_msg.
func ackErrMessage(ackErr string) string {
	if ackErr == "" {
		return "ack error"
	}
	return "ack error: " + ackErr
}
