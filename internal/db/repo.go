package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// ── write ─────────────────────────────────────────────────────────────────────

const sqlInsert = `
INSERT INTO transfers (
    id, packet_hash,
    src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
    from_address, to_address, base_token, base_amount, quote_token, quote_amount,
    height, tx_out, timeout_timestamp,
    status, created_at
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
) ON CONFLICT (id) DO NOTHING`

func (r *Store) Insert(ctx context.Context, t *Transfer) error {
	_, err := r.db.Exec(ctx, sqlInsert,
		t.ID, t.PacketHash,
		t.SrcChainID, t.DstChainID, t.SrcChannelID, t.DstChannelID,
		t.FromAddress, t.ToAddress, t.BaseToken, t.BaseAmount, t.QuoteToken, t.QuoteAmount,
		t.Height, t.TxOut, t.TimeoutTimestamp,
		int(t.Status), t.CreatedAt,
	)
	return err
}

func (r *Store) MarkProcessing(ctx context.Context, ids []int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1 WHERE id = ANY($2) AND status=$3`,
		int(StatusProcessing), ids, int(StatusDetected),
	)
	return err
}

// MarkDone marks the transfer complete. txIn is a fallback tx_in value used only
// if SetTxIn (from the packet_recv event) hasn't already populated it.
func (r *Store) MarkDone(ctx context.Context, id int64, doneAt time.Time, txIn string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1, done_at=$2, tx_in=COALESCE(tx_in, NULLIF($3, '')) WHERE id=$4 AND status < $1`,
		int(StatusDone), doneAt, txIn, id,
	)
	return err
}

// SetTxIn records the destination-chain receive transaction hash, matched by packet_hash.
// Does not overwrite an existing value.
func (r *Store) SetTxIn(ctx context.Context, packetHash, txIn string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET tx_in=$1 WHERE packet_hash=$2 AND tx_in IS NULL`,
		txIn, packetHash,
	)
	return err
}

func (r *Store) MarkFailed(ctx context.Context, id int64, errMsg string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1, err_msg=$2 WHERE id=$3 AND status < $1`,
		int(StatusFailed), errMsg, id,
	)
	return err
}

// ── cursor ────────────────────────────────────────────────────────────────────

func (r *Store) GetCursor(ctx context.Context, name string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT last_id FROM indexer_cursors WHERE name=$1`, name,
	).Scan(&id)
	if err != nil {
		return 0, nil
	}
	return id, nil
}

func (r *Store) SetCursor(ctx context.Context, name string, id int64) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO indexer_cursors (name, last_id) VALUES ($1,$2)
         ON CONFLICT (name) DO UPDATE SET last_id = EXCLUDED.last_id`,
		name, id,
	)
	return err
}

func (r *Store) FindByTimeoutAndChannel(ctx context.Context, timeoutTimestamp int64, srcChannelID int) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT id FROM transfers WHERE timeout_timestamp=$1 AND src_channel_id=$2 AND status < $3 LIMIT 1`,
		timeoutTimestamp, srcChannelID, int(StatusFailed),
	).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (r *Store) FindByPacketHash(ctx context.Context, packetHash string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT id FROM transfers WHERE packet_hash=$1 AND status < $2 LIMIT 1`,
		packetHash, int(StatusFailed),
	).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (r *Store) FindAncestor(ctx context.Context, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT id FROM transfers WHERE id = ANY($1) AND status < $2 LIMIT 1`,
		ids, int(StatusFailed),
	).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (r *Store) GetDetectedIDs(ctx context.Context) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id FROM transfers WHERE status=$1`, int(StatusDetected),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// GetInFlightCreatedAt returns the created_at of every transfer not yet done,
// used to bound how far back a done-table catch-up scan needs to look.
func (r *Store) GetInFlightCreatedAt(ctx context.Context) ([]time.Time, error) {
	rows, err := r.db.Query(ctx,
		`SELECT created_at FROM transfers WHERE status < $1`,
		int(StatusDone),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []time.Time
	for rows.Next() {
		var t time.Time
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// ── read ──────────────────────────────────────────────────────────────────────

type ListFilter struct {
	Address string
	Order   string
	Limit   int
	Offset  int
}

func (r *Store) List(ctx context.Context, f ListFilter) ([]*Transfer, error) {
	base := `SELECT id, packet_hash,
                     src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
                     from_address, to_address, base_token, base_amount, quote_token, quote_amount,
                     height, timeout_timestamp,
                     status, created_at, done_at, err_msg, tx_out, tx_in
              FROM transfers`

	order := "DESC"
	if f.Order == "asc" {
		order = "ASC"
	}

	var query string
	var args []any
	if f.Address != "" {
		query = fmt.Sprintf("%s WHERE (from_address=$1 OR to_address=$1) ORDER BY created_at %s LIMIT $2 OFFSET $3", base, order)
		args = []any{f.Address, f.Limit, f.Offset}
	} else {
		query = fmt.Sprintf("%s ORDER BY created_at %s LIMIT $1 OFFSET $2", base, order)
		args = []any{f.Limit, f.Offset}
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transfers []*Transfer
	for rows.Next() {
		t, err := scanTransfer(rows)
		if err != nil {
			return nil, err
		}
		transfers = append(transfers, t)
	}
	return transfers, rows.Err()
}

func (r *Store) GetByPacketHash(ctx context.Context, packetHash string) (*Transfer, error) {
	row := r.db.QueryRow(ctx,
		`SELECT id, packet_hash,
                src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
                from_address, to_address, base_token, base_amount, quote_token, quote_amount,
                height, timeout_timestamp,
                status, created_at, done_at, err_msg, tx_out, tx_in
         FROM transfers WHERE packet_hash=$1`, packetHash,
	)
	return scanTransfer(row)
}

func (r *Store) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM transfers`).Scan(&count)
	return count, err
}

// ── scan ─────────────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...any) error
}

func scanTransfer(row scanner) (*Transfer, error) {
	t := &Transfer{}
	var status int
	err := row.Scan(
		&t.ID, &t.PacketHash,
		&t.SrcChainID, &t.DstChainID, &t.SrcChannelID, &t.DstChannelID,
		&t.FromAddress, &t.ToAddress, &t.BaseToken, &t.BaseAmount, &t.QuoteToken, &t.QuoteAmount,
		&t.Height, &t.TimeoutTimestamp,
		&status, &t.CreatedAt, &t.DoneAt, &t.ErrMsg, &t.TxOut, &t.TxIn,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("transfer not found")
		}
		return nil, err
	}
	t.Status = TransferStatus(status)
	return t, nil
}
