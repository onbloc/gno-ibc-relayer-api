package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
)

func newMockBridgeDB(t *testing.T) (*BridgeDB, pgxmock.PgxPoolIface) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool: %v", err)
	}
	t.Cleanup(mock.Close)
	return &BridgeDB{db: mock}, mock
}

func expectMet(t *testing.T, mock pgxmock.PgxPoolIface) {
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ── statusPriority ──────────────────────────────────────────────────────────

func TestStatusPriority(t *testing.T) {
	cases := []struct {
		status int
		want   int
	}{
		{int(StatusDone), 0},
		{int(StatusFailed), 1},
		{int(StatusProcessing), 2},
		{int(StatusDetected), 3},
		{99, 3}, // unknown status defaults like StatusDetected
	}
	for _, tc := range cases {
		if got := statusPriority(tc.status); got != tc.want {
			t.Errorf("statusPriority(%d) = %d, want %d", tc.status, got, tc.want)
		}
	}
}

// ── Insert ───────────────────────────────────────────────────────────────────

func TestBridgeDB_Insert_NewRow(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT id, status FROM transfers").
		WithArgs("hash1").
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectExec("INSERT INTO transfers").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	t_ := &BridgeRecord{ID: 1, PacketHash: "hash1", Status: StatusDetected, CreatedAt: time.Now()}
	if err := bdb.Insert(ctx, t_); err != nil {
		t.Fatalf("Insert() error: %v", err)
	}
	expectMet(t, mock)
}

func TestBridgeDB_Insert_ExistingRow_Outranked_Updates(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	// existing row is Detected (worst priority); new one is Done (best) -> update
	mock.ExpectQuery("SELECT id, status FROM transfers").
		WithArgs("hash1").
		WillReturnRows(pgxmock.NewRows([]string{"id", "status"}).AddRow(int64(42), int(StatusDetected)))
	mock.ExpectExec("UPDATE transfers SET status").
		WithArgs(int(StatusDone), int64(42)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	t_ := &BridgeRecord{ID: 1, PacketHash: "hash1", Status: StatusDone, CreatedAt: time.Now()}
	if err := bdb.Insert(ctx, t_); err != nil {
		t.Fatalf("Insert() error: %v", err)
	}
	expectMet(t, mock)
}

func TestBridgeDB_Insert_ExistingRow_NotOutranked_Skipped(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	// existing row is already Done (best priority); new one is Detected (worst) -> skip
	mock.ExpectQuery("SELECT id, status FROM transfers").
		WithArgs("hash1").
		WillReturnRows(pgxmock.NewRows([]string{"id", "status"}).AddRow(int64(42), int(StatusDone)))

	t_ := &BridgeRecord{ID: 1, PacketHash: "hash1", Status: StatusDetected, CreatedAt: time.Now()}
	if err := bdb.Insert(ctx, t_); err != nil {
		t.Fatalf("Insert() error: %v", err)
	}
	expectMet(t, mock) // no Exec expected/called
}

func TestBridgeDB_Insert_QueryError(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	wantErr := errors.New("connection reset")
	mock.ExpectQuery("SELECT id, status FROM transfers").
		WithArgs("hash1").
		WillReturnError(wantErr)

	t_ := &BridgeRecord{ID: 1, PacketHash: "hash1", Status: StatusDetected, CreatedAt: time.Now()}
	if err := bdb.Insert(ctx, t_); !errors.Is(err, wantErr) {
		t.Errorf("Insert() error = %v, want %v", err, wantErr)
	}
	expectMet(t, mock)
}

// ── DedupeByPacketHash ───────────────────────────────────────────────────────

func TestBridgeDB_DedupeByPacketHash(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectExec("DELETE FROM transfers").
		WillReturnResult(pgxmock.NewResult("DELETE", 3))

	n, err := bdb.DedupeByPacketHash(ctx)
	if err != nil {
		t.Fatalf("DedupeByPacketHash() error: %v", err)
	}
	if n != 3 {
		t.Errorf("DedupeByPacketHash() = %d, want 3", n)
	}
	expectMet(t, mock)
}

func TestBridgeDB_DedupeByPacketHash_Error(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	wantErr := errors.New("db down")
	mock.ExpectExec("DELETE FROM transfers").WillReturnError(wantErr)

	if _, err := bdb.DedupeByPacketHash(ctx); !errors.Is(err, wantErr) {
		t.Errorf("DedupeByPacketHash() error = %v, want %v", err, wantErr)
	}
	expectMet(t, mock)
}

// ── MarkProcessing / MarkDone / SetTxIn / MarkFailed ────────────────────────

func TestBridgeDB_MarkProcessing(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectExec("UPDATE transfers SET status").
		WithArgs(int(StatusProcessing), []int64{1, 2}, int(StatusDetected)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))

	if err := bdb.MarkProcessing(ctx, []int64{1, 2}); err != nil {
		t.Fatalf("MarkProcessing() error: %v", err)
	}
	expectMet(t, mock)
}

func TestBridgeDB_MarkDone_Matched(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()
	doneAt := time.Now()

	mock.ExpectExec("done_at").
		WithArgs(int(StatusDone), doneAt, "0xabc", int64(7)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	matched, err := bdb.MarkDone(ctx, 7, doneAt, "0xabc")
	if err != nil {
		t.Fatalf("MarkDone() error: %v", err)
	}
	if !matched {
		t.Error("MarkDone() matched = false, want true")
	}
	expectMet(t, mock)
}

// TestBridgeDB_MarkDone_AlreadyTerminal verifies MarkDone's own guard (status <
// StatusDone) blocks re-marking a row that's already done or failed, and that
// callers can detect the no-op via matched=false.
func TestBridgeDB_MarkDone_AlreadyTerminal(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()
	doneAt := time.Now()

	mock.ExpectExec("done_at").
		WithArgs(int(StatusDone), doneAt, "0xabc", int64(7)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0)) // already done/failed

	matched, err := bdb.MarkDone(ctx, 7, doneAt, "0xabc")
	if err != nil {
		t.Fatalf("MarkDone() error: %v", err)
	}
	if matched {
		t.Error("MarkDone() matched = true, want false (already terminal)")
	}
	expectMet(t, mock)
}

func TestBridgeDB_SetTxIn(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectExec("tx_in IS NULL").
		WithArgs("0xin", "hash1").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	if err := bdb.SetTxIn(ctx, "hash1", "0xin"); err != nil {
		t.Fatalf("SetTxIn() error: %v", err)
	}
	expectMet(t, mock)
}

// TestBridgeDB_SetTxIn_EmptyIsNoOp verifies an empty txIn never reaches the
// database: writing "" would set tx_in to a non-NULL empty string, and the
// WHERE clause's "tx_in IS NULL" guard would then permanently block any later
// call from correcting it with a real hash.
func TestBridgeDB_SetTxIn_EmptyIsNoOp(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	if err := bdb.SetTxIn(ctx, "hash1", ""); err != nil {
		t.Fatalf("SetTxIn() error: %v", err)
	}
	expectMet(t, mock) // no Exec expected/called
}

// TestBridgeDB_MarkFailed_Matched also pins the guard threshold to
// int(StatusDone): if the WHERE clause's guard argument regresses back to
// StatusFailed (which would let a done bridge be overwritten to failed), the
// hardcoded arg list below stops matching and this test fails.
func TestBridgeDB_MarkFailed_Matched(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectExec("err_msg").
		WithArgs(int(StatusFailed), "ack error", "", int64(7), int(StatusDone)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	matched, err := bdb.MarkFailed(ctx, 7, "ack error", "")
	if err != nil {
		t.Fatalf("MarkFailed() error: %v", err)
	}
	if !matched {
		t.Error("MarkFailed() matched = false, want true")
	}
	expectMet(t, mock)
}

// TestBridgeDB_MarkFailed_DoneBridgeNotOverwritten simulates a done bridge
// (status=2) receiving a late/duplicate failure signal: the guard must block
// the update (0 rows affected), never downgrading a completed transfer.
func TestBridgeDB_MarkFailed_DoneBridgeNotOverwritten(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectExec("err_msg").
		WithArgs(int(StatusFailed), "late ack error", "", int64(7), int(StatusDone)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0)) // guard blocked: row is already done

	matched, err := bdb.MarkFailed(ctx, 7, "late ack error", "")
	if err != nil {
		t.Fatalf("MarkFailed() error: %v", err)
	}
	if matched {
		t.Error("MarkFailed() matched = true, want false (a done bridge must never be overwritten to failed)")
	}
	expectMet(t, mock)
}

// TestBridgeDB_MarkFailed_NoMatchingRow verifies the caller can tell "no row
// matched" (id doesn't exist, or the "status < $1" guard blocked it) apart
// from a genuine error — this is what runFailedListener's direct-match branch
// relies on to fall back to promise-based matching instead of misreporting a
// no-op as a successful direct match.
func TestBridgeDB_MarkFailed_NoMatchingRow(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectExec("err_msg").
		WithArgs(int(StatusFailed), "ack error", "", int64(999), int(StatusDone)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0)) // no row matched id=999

	matched, err := bdb.MarkFailed(ctx, 999, "ack error", "")
	if err != nil {
		t.Fatalf("MarkFailed() error: %v", err)
	}
	if matched {
		t.Error("MarkFailed() matched = true, want false (0 rows affected)")
	}
	expectMet(t, mock)
}

// ── cursor ────────────────────────────────────────────────────────────────────

func TestBridgeDB_GetCursor_Found(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT last_id FROM indexer_cursors").
		WithArgs("queue").
		WillReturnRows(pgxmock.NewRows([]string{"last_id"}).AddRow(int64(100)))

	id, err := bdb.GetCursor(ctx, "queue")
	if err != nil {
		t.Fatalf("GetCursor() error: %v", err)
	}
	if id != 100 {
		t.Errorf("GetCursor() = %d, want 100", id)
	}
	expectMet(t, mock)
}

func TestBridgeDB_GetCursor_NotFound(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT last_id FROM indexer_cursors").
		WithArgs("queue").
		WillReturnError(pgx.ErrNoRows)

	id, err := bdb.GetCursor(ctx, "queue")
	if err != nil {
		t.Fatalf("GetCursor() error: %v, want nil (not-found treated as zero cursor)", err)
	}
	if id != 0 {
		t.Errorf("GetCursor() = %d, want 0", id)
	}
	expectMet(t, mock)
}

// TestBridgeDB_GetCursor_GenericErrorPropagates verifies a genuine query error
// (e.g. a DB outage) is returned to the caller rather than masqueraded as "no
// cursor yet" — otherwise syncQueue/syncFailed would silently restart from
// id=0 (a full rescan) instead of surfacing the outage.
func TestBridgeDB_GetCursor_GenericErrorPropagates(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	wantErr := errors.New("connection reset")
	mock.ExpectQuery("SELECT last_id FROM indexer_cursors").
		WithArgs("queue").
		WillReturnError(wantErr)

	_, err := bdb.GetCursor(ctx, "queue")
	if !errors.Is(err, wantErr) {
		t.Errorf("GetCursor() error = %v, want %v", err, wantErr)
	}
	expectMet(t, mock)
}

func TestBridgeDB_SetCursor(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectExec("INSERT INTO indexer_cursors").
		WithArgs("queue", int64(100)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	if err := bdb.SetCursor(ctx, "queue", 100); err != nil {
		t.Fatalf("SetCursor() error: %v", err)
	}
	expectMet(t, mock)
}

// ── find ──────────────────────────────────────────────────────────────────────

func TestBridgeDB_FindByTimeoutAndChannel_Found(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT id FROM transfers WHERE timeout_timestamp").
		WithArgs(int64(123), 2, int(StatusDone)).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(9)))

	id, err := bdb.FindByTimeoutAndChannel(ctx, 123, 2)
	if err != nil {
		t.Fatalf("FindByTimeoutAndChannel() error: %v", err)
	}
	if id != 9 {
		t.Errorf("FindByTimeoutAndChannel() = %d, want 9", id)
	}
	expectMet(t, mock)
}

func TestBridgeDB_FindByTimeoutAndChannel_NotFound(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT id FROM transfers WHERE timeout_timestamp").
		WithArgs(int64(123), 2, int(StatusDone)).
		WillReturnError(pgx.ErrNoRows)

	id, err := bdb.FindByTimeoutAndChannel(ctx, 123, 2)
	if err != nil {
		t.Fatalf("FindByTimeoutAndChannel() error: %v", err)
	}
	if id != 0 {
		t.Errorf("FindByTimeoutAndChannel() = %d, want 0", id)
	}
	expectMet(t, mock)
}

func TestBridgeDB_FindByPacketHash_Found(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT id FROM transfers WHERE packet_hash").
		WithArgs("hash1", int(StatusFailed)).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(5)))

	id, err := bdb.FindByPacketHash(ctx, "hash1")
	if err != nil {
		t.Fatalf("FindByPacketHash() error: %v", err)
	}
	if id != 5 {
		t.Errorf("FindByPacketHash() = %d, want 5", id)
	}
	expectMet(t, mock)
}

func TestBridgeDB_FindByPacketHash_NotFound(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT id FROM transfers WHERE packet_hash").
		WithArgs("missing", int(StatusFailed)).
		WillReturnError(pgx.ErrNoRows)

	id, err := bdb.FindByPacketHash(ctx, "missing")
	if err != nil {
		t.Fatalf("FindByPacketHash() error: %v", err)
	}
	if id != 0 {
		t.Errorf("FindByPacketHash() = %d, want 0", id)
	}
	expectMet(t, mock)
}

func TestBridgeDB_FindAncestor_EmptyIDs(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	id, err := bdb.FindAncestor(ctx, nil)
	if err != nil {
		t.Fatalf("FindAncestor() error: %v", err)
	}
	if id != 0 {
		t.Errorf("FindAncestor() = %d, want 0", id)
	}
	expectMet(t, mock) // no query issued
}

func TestBridgeDB_FindAncestor_Found(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT id FROM transfers WHERE id = ANY").
		WithArgs([]int64{1, 2}, int(StatusDone)).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(2)))

	id, err := bdb.FindAncestor(ctx, []int64{1, 2})
	if err != nil {
		t.Fatalf("FindAncestor() error: %v", err)
	}
	if id != 2 {
		t.Errorf("FindAncestor() = %d, want 2", id)
	}
	expectMet(t, mock)
}

func TestBridgeDB_FindAncestor_NotFound(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT id FROM transfers WHERE id = ANY").
		WithArgs([]int64{1, 2}, int(StatusDone)).
		WillReturnError(pgx.ErrNoRows)

	id, err := bdb.FindAncestor(ctx, []int64{1, 2})
	if err != nil {
		t.Fatalf("FindAncestor() error: %v", err)
	}
	if id != 0 {
		t.Errorf("FindAncestor() = %d, want 0", id)
	}
	expectMet(t, mock)
}

// ── GetDetectedIDs / GetInFlightCreatedAt ───────────────────────────────────

func TestBridgeDB_GetDetectedIDs(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT id FROM transfers WHERE status").
		WithArgs(int(StatusDetected)).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)))

	ids, err := bdb.GetDetectedIDs(ctx)
	if err != nil {
		t.Fatalf("GetDetectedIDs() error: %v", err)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("GetDetectedIDs() = %v, want [1 2]", ids)
	}
	expectMet(t, mock)
}

func TestBridgeDB_GetDetectedIDs_QueryError(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	wantErr := errors.New("db down")
	mock.ExpectQuery("SELECT id FROM transfers WHERE status").
		WithArgs(int(StatusDetected)).
		WillReturnError(wantErr)

	if _, err := bdb.GetDetectedIDs(ctx); !errors.Is(err, wantErr) {
		t.Errorf("GetDetectedIDs() error = %v, want %v", err, wantErr)
	}
	expectMet(t, mock)
}

func TestBridgeDB_GetDetectedIDs_ScanError(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	scanErr := errors.New("scan failed")
	mock.ExpectQuery("SELECT id FROM transfers WHERE status").
		WithArgs(int(StatusDetected)).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)).RowError(0, scanErr))

	if _, err := bdb.GetDetectedIDs(ctx); !errors.Is(err, scanErr) {
		t.Errorf("GetDetectedIDs() error = %v, want %v", err, scanErr)
	}
	expectMet(t, mock)
}

func TestBridgeDB_GetInFlightCreatedAt(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	now := time.Now()
	mock.ExpectQuery("SELECT created_at FROM transfers").
		WithArgs(int(StatusDone)).
		WillReturnRows(pgxmock.NewRows([]string{"created_at"}).AddRow(now))

	result, err := bdb.GetInFlightCreatedAt(ctx)
	if err != nil {
		t.Fatalf("GetInFlightCreatedAt() error: %v", err)
	}
	if len(result) != 1 || !result[0].Equal(now) {
		t.Errorf("GetInFlightCreatedAt() = %v, want [%v]", result, now)
	}
	expectMet(t, mock)
}

func TestBridgeDB_GetInFlightCreatedAt_QueryError(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	wantErr := errors.New("db down")
	mock.ExpectQuery("SELECT created_at FROM transfers").
		WithArgs(int(StatusDone)).
		WillReturnError(wantErr)

	if _, err := bdb.GetInFlightCreatedAt(ctx); !errors.Is(err, wantErr) {
		t.Errorf("GetInFlightCreatedAt() error = %v, want %v", err, wantErr)
	}
	expectMet(t, mock)
}

func TestBridgeDB_GetInFlightCreatedAt_ScanError(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	scanErr := errors.New("scan failed")
	mock.ExpectQuery("SELECT created_at FROM transfers").
		WithArgs(int(StatusDone)).
		WillReturnRows(pgxmock.NewRows([]string{"created_at"}).AddRow(time.Now()).RowError(0, scanErr))

	if _, err := bdb.GetInFlightCreatedAt(ctx); !errors.Is(err, scanErr) {
		t.Errorf("GetInFlightCreatedAt() error = %v, want %v", err, scanErr)
	}
	expectMet(t, mock)
}

// ── List / GetByPacketHash / Count ──────────────────────────────────────────

var bridgeCols = []string{
	"id", "packet_hash",
	"src_chain_id", "dst_chain_id", "src_channel_id", "dst_channel_id",
	"from_address", "to_address", "base_token", "base_amount", "quote_token", "quote_amount",
	"height", "timeout_timestamp",
	"status", "created_at", "done_at", "err_msg", "tx_out", "tx_in",
}

func sampleRow(now time.Time) []any {
	return []any{
		int64(1), "hash1",
		"dev", "11155111", 2, 28,
		"g1abc", "0xdef", "ugnot", "1000", "0x7f5c", "1000",
		int64(100), int64(999),
		int(StatusDone), now, &now, (*string)(nil), "0xout", (*string)(nil),
	}
}

func TestBridgeDB_List_NoAddressFilter(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()
	now := time.Now()

	mock.ExpectQuery("FROM transfers").
		WithArgs(20, 0).
		WillReturnRows(pgxmock.NewRows(bridgeCols).AddRow(sampleRow(now)...))

	bridges, err := bdb.List(ctx, ListFilter{Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(bridges) != 1 || bridges[0].PacketHash != "hash1" {
		t.Errorf("List() = %+v, want one bridge with packet_hash=hash1", bridges)
	}
	expectMet(t, mock)
}

func TestBridgeDB_List_WithAddressFilter(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()
	now := time.Now()

	mock.ExpectQuery("FROM transfers").
		WithArgs("g1abc", 10, 5).
		WillReturnRows(pgxmock.NewRows(bridgeCols).AddRow(sampleRow(now)...))

	bridges, err := bdb.List(ctx, ListFilter{Address: "g1abc", Limit: 10, Offset: 5})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(bridges) != 1 {
		t.Errorf("List() = %+v, want one bridge", bridges)
	}
	expectMet(t, mock)
}

func TestBridgeDB_List_QueryError(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	wantErr := errors.New("db down")
	mock.ExpectQuery("FROM transfers").WithArgs(20, 0).WillReturnError(wantErr)

	if _, err := bdb.List(ctx, ListFilter{Limit: 20}); !errors.Is(err, wantErr) {
		t.Errorf("List() error = %v, want %v", err, wantErr)
	}
	expectMet(t, mock)
}

func TestBridgeDB_List_ScanError(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	scanErr := errors.New("column type mismatch")
	mock.ExpectQuery("FROM transfers").
		WithArgs(20, 0).
		WillReturnRows(pgxmock.NewRows(bridgeCols).AddRow(sampleRow(time.Now())...).RowError(0, scanErr))

	if _, err := bdb.List(ctx, ListFilter{Limit: 20}); !errors.Is(err, scanErr) {
		t.Errorf("List() error = %v, want %v", err, scanErr)
	}
	expectMet(t, mock)
}

func TestBridgeDB_GetByPacketHash_Found(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()
	now := time.Now()

	mock.ExpectQuery("FROM transfers WHERE packet_hash").
		WithArgs("hash1").
		WillReturnRows(pgxmock.NewRows(bridgeCols).AddRow(sampleRow(now)...))

	got, err := bdb.GetByPacketHash(ctx, "hash1")
	if err != nil {
		t.Fatalf("GetByPacketHash() error: %v", err)
	}
	if got.PacketHash != "hash1" || got.Status != StatusDone {
		t.Errorf("GetByPacketHash() = %+v", got)
	}
	expectMet(t, mock)
}

func TestBridgeDB_GetByPacketHash_NotFound(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("FROM transfers WHERE packet_hash").
		WithArgs("missing").
		WillReturnError(pgx.ErrNoRows)

	_, err := bdb.GetByPacketHash(ctx, "missing")
	if err == nil {
		t.Fatal("GetByPacketHash() want error, got nil")
	}
	if err.Error() != "bridge not found" {
		t.Errorf("GetByPacketHash() error = %q, want %q", err.Error(), "bridge not found")
	}
	expectMet(t, mock)
}

func TestBridgeDB_Count(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(42)))

	count, err := bdb.Count(ctx)
	if err != nil {
		t.Fatalf("Count() error: %v", err)
	}
	if count != 42 {
		t.Errorf("Count() = %d, want 42", count)
	}
	expectMet(t, mock)
}

func TestBridgeDB_CountRecentByStatus(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	mock.ExpectQuery("FROM \\(SELECT status FROM transfers ORDER BY created_at DESC LIMIT").
		WithArgs(int(StatusDetected), int(StatusProcessing), int(StatusDone), int(StatusFailed), 1000).
		WillReturnRows(pgxmock.NewRows([]string{"total", "detected", "processing", "succeeded", "failed"}).
			AddRow(int64(1000), int64(10), int64(20), int64(900), int64(70)))

	got, err := bdb.CountRecentByStatus(ctx, 1000)
	if err != nil {
		t.Fatalf("CountRecentByStatus() error: %v", err)
	}
	want := &StatusSummary{Total: 1000, Detected: 10, Processing: 20, Succeeded: 900, Failed: 70}
	if *got != *want {
		t.Errorf("CountRecentByStatus() = %+v, want %+v", got, want)
	}
	expectMet(t, mock)
}

func TestBridgeDB_CountRecentByStatus_QueryError(t *testing.T) {
	bdb, mock := newMockBridgeDB(t)
	ctx := context.Background()

	wantErr := errors.New("db down")
	mock.ExpectQuery("FROM \\(SELECT status FROM transfers ORDER BY created_at DESC LIMIT").
		WithArgs(int(StatusDetected), int(StatusProcessing), int(StatusDone), int(StatusFailed), 1000).
		WillReturnError(wantErr)

	if _, err := bdb.CountRecentByStatus(ctx, 1000); !errors.Is(err, wantErr) {
		t.Errorf("CountRecentByStatus() error = %v, want %v", err, wantErr)
	}
	expectMet(t, mock)
}
