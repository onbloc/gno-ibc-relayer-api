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
    status, created_at
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
) ON CONFLICT (id) DO NOTHING`

func (r *TransferRepo) Insert(ctx context.Context, t *model.Transfer) error {
	_, err := r.db.Exec(ctx, sqlInsert,
		t.ID, t.PacketHash,
		t.SrcChainID, t.DstChainID, t.SrcChannelID, t.DstChannelID,
		t.FromAddress, t.ToAddress, t.BaseToken, t.BaseAmount, t.QuoteToken, t.QuoteAmount,
		t.Height, t.TxHash, t.TimeoutTimestamp,
		int(t.Status), t.CreatedAt,
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

// FindByTimeoutAndChannel returns the transfer id matching the given timeout_timestamp
// and src_channel_id with status < failed. Used to match done/failed items back to
// the original transfer when the Voyager item id differs.
func (r *TransferRepo) FindByTimeoutAndChannel(ctx context.Context, timeoutTimestamp int64, srcChannelID int) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT id FROM transfers WHERE timeout_timestamp=$1 AND src_channel_id=$2 AND status < $3 LIMIT 1`,
		timeoutTimestamp, srcChannelID, int(model.StatusFailed),
	).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// FindAncestor returns the first id from the given list that exists in transfers
// with status < failed. Used to trace a failed Voyager op back to its origin transfer.
func (r *TransferRepo) FindAncestor(ctx context.Context, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT id FROM transfers WHERE id = ANY($1) AND status < $2 LIMIT 1`,
		ids, int(model.StatusFailed),
	).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return id, err
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

// InFlightTransfer holds fields needed to match in-flight transfers against done/failed.
type InFlightTransfer struct {
	ID               int64
	TimeoutTimestamp int64
	SrcChannelID     int
	CreatedAt        time.Time
}

// GetInFlight returns all transfers with status < done.
func (r *TransferRepo) GetInFlight(ctx context.Context) ([]InFlightTransfer, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, timeout_timestamp, src_channel_id, created_at FROM transfers WHERE status < $1`,
		int(model.StatusDone),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []InFlightTransfer
	for rows.Next() {
		var t InFlightTransfer
		if err := rows.Scan(&t.ID, &t.TimeoutTimestamp, &t.SrcChannelID, &t.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// ── read ──────────────────────────────────────────────────────────────────────

type ListFilter struct {
	Address string // optional: matches from_address OR to_address
	Order   string // "asc" or "desc" by created_at (default "desc")
	Limit   int
	Offset  int
}

func (r *TransferRepo) List(ctx context.Context, f ListFilter) ([]*model.Transfer, error) {
	base := `SELECT id, packet_hash,
                     src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
                     from_address, to_address, base_token, base_amount, quote_token, quote_amount,
                     height, tx_hash, timeout_timestamp,
                     status, created_at, done_at
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

func (r *TransferRepo) GetByPacketHash(ctx context.Context, packetHash string) (*model.Transfer, error) {
	row := r.db.QueryRow(ctx,
		`SELECT id, packet_hash,
                src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
                from_address, to_address, base_token, base_amount, quote_token, quote_amount,
                height, tx_hash, timeout_timestamp,
                status, created_at, done_at
         FROM transfers WHERE packet_hash=$1`, packetHash,
	)
	return scanTransfer(row)
}

func (r *TransferRepo) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM transfers`).Scan(&count)
	return count, err
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
