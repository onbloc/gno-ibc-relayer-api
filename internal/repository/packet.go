package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/onbloc/gno-ibc-relayer-api/internal/model"
)

type TransferRepo struct {
	db *pgxpool.Pool
}

func NewTransferRepo(db *pgxpool.Pool) *TransferRepo {
	return &TransferRepo{db: db}
}

// ── write ─────────────────────────────────────────────────────────────────────

const sqlInsert = `
INSERT INTO transfers (
    id, packet_hash,
    src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
    from_address, to_address, base_token, base_amount, quote_token, quote_amount,
    height, tx_hash, timeout_timestamp,
    status, created_at, raw_item
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
) ON CONFLICT (id) DO NOTHING`

func (r *TransferRepo) Insert(ctx context.Context, t *model.Transfer) error {
	_, err := r.db.Exec(ctx, sqlInsert,
		t.ID, t.PacketHash,
		t.SrcChainID, t.DstChainID, t.SrcChannelID, t.DstChannelID,
		t.FromAddress, t.ToAddress, t.BaseToken, t.BaseAmount, t.QuoteToken, t.QuoteAmount,
		t.Height, t.TxHash, t.TimeoutTimestamp,
		int(t.Status), t.CreatedAt, t.RawItem,
	)
	return err
}

func (r *TransferRepo) MarkProcessing(ctx context.Context, ids []int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1 WHERE id = ANY($2) AND status=$3`,
		int(model.StatusProcessing), ids, int(model.StatusDetected),
	)
	return err
}

func (r *TransferRepo) MarkDone(ctx context.Context, id int64, doneAt time.Time) error {
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1, done_at=$2 WHERE id=$3 AND status < $1`,
		int(model.StatusDone), doneAt, id,
	)
	return err
}

func (r *TransferRepo) MarkFailed(ctx context.Context, id int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1 WHERE id=$2 AND status < $1`,
		int(model.StatusFailed), id,
	)
	return err
}

// ── cursor ────────────────────────────────────────────────────────────────────

func (r *TransferRepo) GetCursor(ctx context.Context, name string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT last_id FROM indexer_cursors WHERE name=$1`, name,
	).Scan(&id)
	if err != nil {
		return 0, nil
	}
	return id, nil
}

func (r *TransferRepo) SetCursor(ctx context.Context, name string, id int64) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO indexer_cursors (name, last_id) VALUES ($1,$2)
         ON CONFLICT (name) DO UPDATE SET last_id = EXCLUDED.last_id`,
		name, id,
	)
	return err
}

// GetDetectedIDs returns IDs of transfers currently in status=detected.
func (r *TransferRepo) GetDetectedIDs(ctx context.Context) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id FROM transfers WHERE status=$1`, int(model.StatusDetected),
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

// ── read ──────────────────────────────────────────────────────────────────────

type ListFilter struct {
	Address string // required: matches from_address OR to_address
	Status  *int   // optional
	Order   string // "asc" or "desc" by created_at (default "desc")
	Limit   int
	Offset  int
}

func (r *TransferRepo) List(ctx context.Context, f ListFilter) ([]*model.Transfer, error) {
	// address is required — return empty without hitting the DB
	if f.Address == "" {
		return []*model.Transfer{}, nil
	}

	args := []any{f.Address}
	query := `SELECT id, packet_hash,
                     src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
                     from_address, to_address, base_token, base_amount, quote_token, quote_amount,
                     height, tx_hash, timeout_timestamp,
                     status, created_at, done_at
              FROM transfers
              WHERE (from_address=$1 OR to_address=$1)`
	n := 2

	if f.Status != nil {
		query += fmt.Sprintf(" AND status=$%d", n)
		args = append(args, *f.Status)
		n++
	}

	order := "DESC"
	if f.Order == "asc" {
		order = "ASC"
	}
	query += fmt.Sprintf(" ORDER BY created_at %s LIMIT $%d OFFSET $%d", order, n, n+1)
	args = append(args, f.Limit, f.Offset)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transfers []*model.Transfer
	for rows.Next() {
		t, err := scanTransfer(rows)
		if err != nil {
			return nil, err
		}
		transfers = append(transfers, t)
	}
	return transfers, rows.Err()
}

func (r *TransferRepo) GetByID(ctx context.Context, id int64) (*model.Transfer, error) {
	row := r.db.QueryRow(ctx,
		`SELECT id, packet_hash,
                src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
                from_address, to_address, base_token, base_amount, quote_token, quote_amount,
                height, tx_hash, timeout_timestamp,
                status, created_at, done_at
         FROM transfers WHERE id=$1`, id,
	)
	return scanTransfer(row)
}

type Stats struct {
	Total      int64 `json:"total"`
	Detected   int64 `json:"detected"`
	Processing int64 `json:"processing"`
	Done       int64 `json:"done"`
	Failed     int64 `json:"failed"`
}

func (r *TransferRepo) GetStats(ctx context.Context) (*Stats, error) {
	var s Stats
	err := r.db.QueryRow(ctx, `
		SELECT
			COUNT(*)                                  AS total,
			COUNT(*) FILTER (WHERE status=0)          AS detected,
			COUNT(*) FILTER (WHERE status=1)          AS processing,
			COUNT(*) FILTER (WHERE status=2)          AS done,
			COUNT(*) FILTER (WHERE status=3)          AS failed
		FROM transfers`,
	).Scan(&s.Total, &s.Detected, &s.Processing, &s.Done, &s.Failed)
	return &s, err
}

// ── scan ─────────────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...any) error
}

func scanTransfer(row scanner) (*model.Transfer, error) {
	t := &model.Transfer{}
	var status int
	err := row.Scan(
		&t.ID, &t.PacketHash,
		&t.SrcChainID, &t.DstChainID, &t.SrcChannelID, &t.DstChannelID,
		&t.FromAddress, &t.ToAddress, &t.BaseToken, &t.BaseAmount, &t.QuoteToken, &t.QuoteAmount,
		&t.Height, &t.TxHash, &t.TimeoutTimestamp,
		&status, &t.CreatedAt, &t.DoneAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("repository: transfer not found")
		}
		return nil, err
	}
	t.Status = model.TransferStatus(status)
	return t, nil
}
