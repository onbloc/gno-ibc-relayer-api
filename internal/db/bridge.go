package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxQuerier is the subset of *pgxpool.Pool that BridgeDB needs. Narrowed here
// so tests can inject a mock (e.g. pgxmock) instead of a real database.
type pgxQuerier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type BridgeStatus int

const (
	StatusDetected   BridgeStatus = 0
	StatusProcessing BridgeStatus = 1
	StatusDone       BridgeStatus = 2
	StatusFailed     BridgeStatus = 3
)

type BridgeRecord struct {
	ID         int64  `json:"id"`
	PacketHash string `json:"packet_hash"`

	SrcChainID   string `json:"src_chain_id"`
	DstChainID   string `json:"dst_chain_id"`
	SrcChannelID int    `json:"src_channel_id"`
	DstChannelID int    `json:"dst_channel_id"`

	FromAddress string `json:"from_address"`
	ToAddress   string `json:"to_address"`
	BaseToken   string `json:"base_token"`
	BaseAmount  string `json:"base_amount"`
	QuoteToken  string `json:"quote_token"`
	QuoteAmount string `json:"quote_amount"`

	Height           int64 `json:"height"`
	TimeoutTimestamp int64 `json:"timeout_timestamp"`

	Status    BridgeStatus `json:"status"`
	CreatedAt time.Time    `json:"created_at"`
	DoneAt    *time.Time   `json:"done_at,omitempty"`
	ErrMsg    *string      `json:"err_msg,omitempty"`

	// TxOut is the source-chain send transaction hash.
	// TxIn is the destination-chain receive transaction hash, set once a packet_recv/write_ack is matched.
	TxOut string  `json:"tx_out"`
	TxIn  *string `json:"tx_in,omitempty"`
}

type BridgeDB struct {
	db pgxQuerier
}

func New(db *pgxpool.Pool) *BridgeDB {
	return &BridgeDB{db: db}
}

// ── write ─────────────────────────────────────────────────────────────────────

// statusPriority ranks a bridge status for duplicate resolution — lowest wins (done > failed > in-flight).
// Keep in sync with DedupeByPacketHash's CASE below.
func statusPriority(status int) int {
	switch BridgeStatus(status) {
	case StatusDone:
		return 0
	case StatusFailed:
		return 1
	case StatusProcessing:
		return 2
	default: // StatusDetected
		return 3
	}
}

// Insert adds a newly detected bridge, but if a row for this packet_hash already exists
// (e.g. the relayer re-emitted it under a new id), it only advances that row's status when
// the new one outranks it (see statusPriority).
func (r *BridgeDB) Insert(ctx context.Context, t *BridgeRecord) error {
	var existingID int64
	var existingStatus int
	err := r.db.QueryRow(ctx,
		`SELECT id, status FROM transfers WHERE packet_hash=$1`, t.PacketHash,
	).Scan(&existingID, &existingStatus)

	switch {
	case err == pgx.ErrNoRows:
		_, err := r.db.Exec(ctx, `
			INSERT INTO transfers (
			    id, packet_hash,
			    src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
			    from_address, to_address, base_token, base_amount, quote_token, quote_amount,
			    height, tx_out, timeout_timestamp,
			    status, created_at
			) VALUES (
			    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
			) ON CONFLICT (id) DO NOTHING`,
			t.ID, t.PacketHash,
			t.SrcChainID, t.DstChainID, t.SrcChannelID, t.DstChannelID,
			t.FromAddress, t.ToAddress, t.BaseToken, t.BaseAmount, t.QuoteToken, t.QuoteAmount,
			t.Height, t.TxOut, t.TimeoutTimestamp,
			int(t.Status), t.CreatedAt,
		)
		return err
	case err != nil:
		return err
	case statusPriority(int(t.Status)) < statusPriority(existingStatus):
		_, err := r.db.Exec(ctx, `UPDATE transfers SET status=$1 WHERE id=$2`, int(t.Status), existingID)
		return err
	default:
		return nil
	}
}

// DedupeByPacketHash keeps one row per packet_hash — highest-priority status wins, ties keep
// the lowest id. Run at startup to clean up pre-dedup rows or relayer re-scan races.
func (r *BridgeDB) DedupeByPacketHash(ctx context.Context) (int64, error) {
	tag, err := r.db.Exec(ctx, `
		WITH ranked AS (
			SELECT id,
			       ROW_NUMBER() OVER (
			           PARTITION BY packet_hash
			           ORDER BY
			               CASE status
			                   WHEN 2 THEN 0  -- done: highest priority
			                   WHEN 3 THEN 1  -- failed
			                   WHEN 1 THEN 2  -- processing
			                   ELSE 3         -- detected: lowest priority
			               END ASC,
			               id ASC
			       ) AS rn
			FROM transfers
		)
		DELETE FROM transfers WHERE id IN (SELECT id FROM ranked WHERE rn > 1)`,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (r *BridgeDB) MarkProcessing(ctx context.Context, ids []int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1 WHERE id = ANY($2) AND status=$3`,
		int(StatusProcessing), ids, int(StatusDetected),
	)
	return err
}

// MarkDone marks the bridge complete; txIn is a fallback used only if SetTxIn hasn't already
// set tx_in. The returned bool reports whether a row was actually matched — false (with a nil
// error) means id doesn't exist, or the bridge already reached a terminal state (done or
// failed), which must never be overwritten.
func (r *BridgeDB) MarkDone(ctx context.Context, id int64, doneAt time.Time, txIn string) (bool, error) {
	tag, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1, done_at=$2, tx_in=COALESCE(tx_in, NULLIF($3, '')) WHERE id=$4 AND status < $1`,
		int(StatusDone), doneAt, txIn, id,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// SetTxIn records the destination-chain receive tx hash, matched by packet_hash; never
// overwrites an existing value. A no-op if txIn is empty — writing "" would set tx_in to a
// non-NULL empty string, permanently blocking this row from ever being corrected by a real
// hash (the WHERE clause only matches tx_in IS NULL).
func (r *BridgeDB) SetTxIn(ctx context.Context, packetHash, txIn string) error {
	if txIn == "" {
		return nil
	}
	_, err := r.db.Exec(ctx,
		`UPDATE transfers SET tx_in=$1 WHERE packet_hash=$2 AND tx_in IS NULL`,
		txIn, packetHash,
	)
	return err
}

// MarkFailed marks the bridge failed; txIn is a fallback used only if SetTxIn hasn't already
// set tx_in (pass "" if no hash is available). The returned bool reports whether a row was
// actually matched — false (with a nil error) means id doesn't exist, or the bridge already
// reached a terminal state (done or failed), which must never be overwritten.
func (r *BridgeDB) MarkFailed(ctx context.Context, id int64, errMsg string, txIn string) (bool, error) {
	tag, err := r.db.Exec(ctx,
		`UPDATE transfers SET status=$1, err_msg=$2, tx_in=COALESCE(tx_in, NULLIF($3, '')) WHERE id=$4 AND status < $5`,
		int(StatusFailed), errMsg, txIn, id, int(StatusDone),
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ── cursor ────────────────────────────────────────────────────────────────────

// GetCursor returns 0 if no cursor has been set yet for name. A genuine query error is
// propagated rather than treated as "no cursor", so callers don't silently restart from 0.
func (r *BridgeDB) GetCursor(ctx context.Context, name string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT last_id FROM indexer_cursors WHERE name=$1`, name,
	).Scan(&id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return id, nil
}

func (r *BridgeDB) SetCursor(ctx context.Context, name string, id int64) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO indexer_cursors (name, last_id) VALUES ($1,$2)
         ON CONFLICT (name) DO UPDATE SET last_id = EXCLUDED.last_id`,
		name, id,
	)
	return err
}

// FindByTimeoutAndChannel finds an in-flight (not yet done or failed) bridge for a
// packet_timeout to mark failed. Terminal bridges are excluded — there is nothing useful
// to do with one here, since the only action a caller takes with the result is MarkFailed.
func (r *BridgeDB) FindByTimeoutAndChannel(ctx context.Context, timeoutTimestamp int64, srcChannelID int) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT id FROM transfers WHERE timeout_timestamp=$1 AND src_channel_id=$2 AND status < $3 LIMIT 1`,
		timeoutTimestamp, srcChannelID, int(StatusDone),
	).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// FindByPacketHash finds a bridge by packet_hash, including ones already marked done.
// Unlike the other finders, this one is intentionally not narrowed to in-flight rows: a
// packet_recv arriving after the bridge is already done (e.g. out-of-order with write_ack)
// must still be able to correct tx_in via SetTxIn. Callers that instead intend to mutate
// status (e.g. MarkDone/MarkFailed) rely on those methods' own terminal-state guards.
func (r *BridgeDB) FindByPacketHash(ctx context.Context, packetHash string) (int64, error) {
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

// FindAncestor finds an in-flight (not yet done or failed) ancestor to mark failed. Terminal
// ancestors are excluded — the only action a caller takes with the result is MarkFailed.
func (r *BridgeDB) FindAncestor(ctx context.Context, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var id int64
	err := r.db.QueryRow(ctx,
		`SELECT id FROM transfers WHERE id = ANY($1) AND status < $2 LIMIT 1`,
		ids, int(StatusDone),
	).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (r *BridgeDB) GetDetectedIDs(ctx context.Context) ([]int64, error) {
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

// GetInFlightCreatedAt returns the created_at of every bridge not yet done,
// used to bound how far back a done-table catch-up scan needs to look.
func (r *BridgeDB) GetInFlightCreatedAt(ctx context.Context) ([]time.Time, error) {
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

func (r *BridgeDB) List(ctx context.Context, f ListFilter) ([]*BridgeRecord, error) {
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

	var bridges []*BridgeRecord
	for rows.Next() {
		t, err := scanBridgeRecord(rows)
		if err != nil {
			return nil, err
		}
		bridges = append(bridges, t)
	}
	return bridges, rows.Err()
}

func (r *BridgeDB) GetByPacketHash(ctx context.Context, packetHash string) (*BridgeRecord, error) {
	row := r.db.QueryRow(ctx,
		`SELECT id, packet_hash,
                src_chain_id, dst_chain_id, src_channel_id, dst_channel_id,
                from_address, to_address, base_token, base_amount, quote_token, quote_amount,
                height, timeout_timestamp,
                status, created_at, done_at, err_msg, tx_out, tx_in
         FROM transfers WHERE packet_hash=$1`, packetHash,
	)
	return scanBridgeRecord(row)
}

func (r *BridgeDB) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM transfers`).Scan(&count)
	return count, err
}

// ── scan ─────────────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...any) error
}

func scanBridgeRecord(row scanner) (*BridgeRecord, error) {
	t := &BridgeRecord{}
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
			return nil, fmt.Errorf("bridge not found")
		}
		return nil, err
	}
	t.Status = BridgeStatus(status)
	return t, nil
}
