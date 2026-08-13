package indexer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/pashagolub/pgxmock/v4"
)

// testBatchSize is a realistic non-zero BatchSize so tests exercise the actual
// LIMIT the indexer sends, rather than accidentally testing the LIMIT 0 that a
// zero-value/missing config would silently (and permanently) stall on.
const testBatchSize = 100

func newTestIndexer(t *testing.T) (*Indexer, pgxmock.PgxPoolIface, *fakeBridgeDB) {
	relayerDB, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool: %v", err)
	}
	t.Cleanup(relayerDB.Close)

	bridgeDB := newFakeBridgeDB()
	idx := &Indexer{
		relayerDB: relayerDB,
		bridgeDB:  bridgeDB,
		cfg:       config.IndexerConfig{BatchSize: testBatchSize},
		chains:    testChains,
	}
	return idx, relayerDB, bridgeDB
}

// ── syncQueue ────────────────────────────────────────────────────────────────

func TestSyncQueue_InsertsNewBridge(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.cursors["queue"] = 5

	item := buildQueueItem("voyager-event-source-plugin-gno/dev", "packet_send", "0xabc", 2, 28)
	relayerDB.ExpectQuery("SELECT id, item, created_at FROM queue").
		WithArgs(int64(5), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "item", "created_at"}).AddRow(int64(10), item, time.Now()))

	if err := idx.syncQueue(context.Background()); err != nil {
		t.Fatalf("syncQueue() error: %v", err)
	}
	if len(bridgeDB.inserted) != 1 {
		t.Fatalf("inserted = %d, want 1", len(bridgeDB.inserted))
	}
	if bridgeDB.cursors["queue"] != 10 {
		t.Errorf("cursor = %d, want 10", bridgeDB.cursors["queue"])
	}
}

func TestSyncQueue_CursorError(t *testing.T) {
	idx, _, bridgeDB := newTestIndexer(t)
	bridgeDB.getCursorErr = errors.New("db down")

	if err := idx.syncQueue(context.Background()); err == nil {
		t.Error("syncQueue() want error, got nil")
	}
}

func TestSyncQueue_QueryError(t *testing.T) {
	idx, relayerDB, _ := newTestIndexer(t)

	relayerDB.ExpectQuery("SELECT id, item, created_at FROM queue").
		WithArgs(int64(0), testBatchSize).
		WillReturnError(errors.New("db down"))

	if err := idx.syncQueue(context.Background()); err == nil {
		t.Error("syncQueue() want error, got nil")
	}
}

func TestSyncQueue_ParseErrorSkipsButAdvancesCursor(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)

	relayerDB.ExpectQuery("SELECT id, item, created_at FROM queue").
		WithArgs(int64(0), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "item", "created_at"}).AddRow(int64(10), []byte("not json"), time.Now()))

	if err := idx.syncQueue(context.Background()); err != nil {
		t.Fatalf("syncQueue() error: %v", err)
	}
	if len(bridgeDB.inserted) != 0 {
		t.Errorf("inserted = %d, want 0 (parse failed)", len(bridgeDB.inserted))
	}
	if bridgeDB.cursors["queue"] != 10 {
		t.Errorf("cursor = %d, want 10 (should still advance past unparseable row)", bridgeDB.cursors["queue"])
	}
}

func TestSyncQueue_InsertError(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.insertErr = errors.New("db down")

	item := buildQueueItem("voyager-event-source-plugin-gno/dev", "packet_send", "0xabc", 2, 28)
	relayerDB.ExpectQuery("SELECT id, item, created_at FROM queue").
		WithArgs(int64(0), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "item", "created_at"}).AddRow(int64(10), item, time.Now()))

	if err := idx.syncQueue(context.Background()); err == nil {
		t.Error("syncQueue() want error, got nil")
	}
	if _, ok := bridgeDB.cursors["queue"]; ok {
		t.Error("cursor should not be set when insert fails")
	}
}

func TestSyncQueue_NoRows(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)

	relayerDB.ExpectQuery("SELECT id, item, created_at FROM queue").
		WithArgs(int64(0), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "item", "created_at"}))

	if err := idx.syncQueue(context.Background()); err != nil {
		t.Fatalf("syncQueue() error: %v", err)
	}
	if _, ok := bridgeDB.cursors["queue"]; ok {
		t.Error("cursor should not be set when no rows are processed")
	}
}

// ── syncProcessing ───────────────────────────────────────────────────────────

func TestSyncProcessing_NoDetectedIDs(t *testing.T) {
	idx, _, _ := newTestIndexer(t)

	if err := idx.syncProcessing(context.Background()); err != nil {
		t.Fatalf("syncProcessing() error: %v", err)
	}
}

func TestSyncProcessing_AllStillInQueue(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.detectedIDs = []int64{1, 2}

	relayerDB.ExpectQuery("SELECT id FROM queue WHERE id = ANY").
		WithArgs([]int64{1, 2}).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)))

	if err := idx.syncProcessing(context.Background()); err != nil {
		t.Fatalf("syncProcessing() error: %v", err)
	}
	if len(bridgeDB.markProcessingIDs) != 0 {
		t.Errorf("MarkProcessing calls = %v, want none", bridgeDB.markProcessingIDs)
	}
}

func TestSyncProcessing_SomeGoneFromQueue(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.detectedIDs = []int64{1, 2}

	relayerDB.ExpectQuery("SELECT id FROM queue WHERE id = ANY").
		WithArgs([]int64{1, 2}).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	if err := idx.syncProcessing(context.Background()); err != nil {
		t.Fatalf("syncProcessing() error: %v", err)
	}
	if len(bridgeDB.markProcessingIDs) != 1 || bridgeDB.markProcessingIDs[0][0] != 2 {
		t.Errorf("MarkProcessing calls = %v, want [[2]]", bridgeDB.markProcessingIDs)
	}
}

func TestSyncProcessing_GetDetectedIDsError(t *testing.T) {
	idx, _, bridgeDB := newTestIndexer(t)
	bridgeDB.getDetectedIDsErr = errors.New("db down")

	if err := idx.syncProcessing(context.Background()); err == nil {
		t.Error("syncProcessing() want error, got nil")
	}
}

func TestSyncProcessing_QueryError(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.detectedIDs = []int64{1}

	relayerDB.ExpectQuery("SELECT id FROM queue WHERE id = ANY").
		WithArgs([]int64{1}).
		WillReturnError(errors.New("db down"))

	if err := idx.syncProcessing(context.Background()); err == nil {
		t.Error("syncProcessing() want error, got nil")
	}
}

// ── syncDone ─────────────────────────────────────────────────────────────────

func TestSyncDone_NoInFlight(t *testing.T) {
	idx, _, _ := newTestIndexer(t)

	if err := idx.syncDone(context.Background()); err != nil {
		t.Fatalf("syncDone() error: %v", err)
	}
}

func TestSyncDone_WriteAckSuccess_MarksDone(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.inFlightCreatedAt = []time.Time{time.Now()}
	bridgeDB.findByPacketHashID = 7

	item := buildWriteAckItem("voyager-event-source-plugin-evm/11155111", "packethash1", "", 2)
	relayerDB.ExpectQuery("FROM done").
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"item", "created_at"}).AddRow(item, time.Now()))

	if err := idx.syncDone(context.Background()); err != nil {
		t.Fatalf("syncDone() error: %v", err)
	}
	if len(bridgeDB.markDoneCalls) != 1 || bridgeDB.markDoneCalls[0].id != 7 {
		t.Errorf("MarkDone calls = %+v, want one call with id=7", bridgeDB.markDoneCalls)
	}
}

func TestSyncDone_NoMatchingBridge_Skipped(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.inFlightCreatedAt = []time.Time{time.Now()}
	bridgeDB.findByPacketHashID = 0 // no match

	item := buildWriteAckItem("voyager-event-source-plugin-evm/11155111", "packethash1", "", 2)
	relayerDB.ExpectQuery("FROM done").
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"item", "created_at"}).AddRow(item, time.Now()))

	if err := idx.syncDone(context.Background()); err != nil {
		t.Fatalf("syncDone() error: %v", err)
	}
	if len(bridgeDB.markDoneCalls) != 0 {
		t.Errorf("MarkDone calls = %d, want 0", len(bridgeDB.markDoneCalls))
	}
}

func TestSyncDone_GetInFlightError(t *testing.T) {
	idx, _, bridgeDB := newTestIndexer(t)
	bridgeDB.getInFlightErr = errors.New("db down")

	if err := idx.syncDone(context.Background()); err == nil {
		t.Error("syncDone() want error, got nil")
	}
}

func TestSyncDone_QueryError(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.inFlightCreatedAt = []time.Time{time.Now()}

	relayerDB.ExpectQuery("FROM done").WithArgs(pgxmock.AnyArg()).WillReturnError(errors.New("db down"))

	if err := idx.syncDone(context.Background()); err == nil {
		t.Error("syncDone() want error, got nil")
	}
}

// ── syncFailed ───────────────────────────────────────────────────────────────

func TestSyncFailed_DirectMark_NoParents(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)

	relayerDB.ExpectQuery("SELECT id, parents, message FROM failed").
		WithArgs(int64(0), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "parents", "message"}).AddRow(int64(1), []int64(nil), "timeout"))

	if err := idx.syncFailed(context.Background()); err != nil {
		t.Fatalf("syncFailed() error: %v", err)
	}
	if len(bridgeDB.markFailedCalls) != 1 || bridgeDB.markFailedCalls[0].id != 1 {
		t.Errorf("MarkFailed calls = %+v, want one call with id=1", bridgeDB.markFailedCalls)
	}
	if bridgeDB.cursors["failed"] != 1 {
		t.Errorf("cursor = %d, want 1", bridgeDB.cursors["failed"])
	}
}

// TestSyncFailed_MarkFailedError_CursorDoesNotAdvance verifies a transient
// MarkFailed error stops the batch instead of advancing the cursor past the
// failed row — otherwise that row would never be retried on the next poll.
func TestSyncFailed_MarkFailedError_CursorDoesNotAdvance(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.markFailedErr = errors.New("db down")

	relayerDB.ExpectQuery("SELECT id, parents, message FROM failed").
		WithArgs(int64(0), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "parents", "message"}).AddRow(int64(1), []int64(nil), "timeout"))

	if err := idx.syncFailed(context.Background()); err != nil {
		t.Fatalf("syncFailed() error: %v", err)
	}
	if _, ok := bridgeDB.cursors["failed"]; ok {
		t.Errorf("cursor = %v, want unset (must not advance past a row that failed to mark)", bridgeDB.cursors["failed"])
	}
}

// TestSyncFailed_MarkFailedError_StopsBatchButRetainsEarlierProgress verifies
// that when a later row in the same batch fails, the cursor still advances up
// to the last row that succeeded — it doesn't roll back correctly-processed
// work, it just stops before the failure.
func TestSyncFailed_MarkFailedError_StopsBatchButRetainsEarlierProgress(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)

	callCount := 0
	bridgeDB.markFailedFunc = func(id int64) (bool, error) {
		callCount++
		if id == 2 {
			return false, errors.New("db down")
		}
		return true, nil
	}

	relayerDB.ExpectQuery("SELECT id, parents, message FROM failed").
		WithArgs(int64(0), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "parents", "message"}).
			AddRow(int64(1), []int64(nil), "timeout").
			AddRow(int64(2), []int64(nil), "timeout").
			AddRow(int64(3), []int64(nil), "timeout"))

	if err := idx.syncFailed(context.Background()); err != nil {
		t.Fatalf("syncFailed() error: %v", err)
	}
	if bridgeDB.cursors["failed"] != 1 {
		t.Errorf("cursor = %v, want 1 (stop at the row before the failure, id=3 must not be skipped)", bridgeDB.cursors["failed"])
	}
	if callCount != 2 {
		t.Errorf("MarkFailed calls = %d, want 2 (row id=3 must not be attempted this batch)", callCount)
	}
}

func TestSyncFailed_AncestorTraced(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.findAncestorID = 99

	relayerDB.ExpectQuery("SELECT id, parents, message FROM failed").
		WithArgs(int64(0), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "parents", "message"}).AddRow(int64(1), []int64{5}, "timeout"))

	if err := idx.syncFailed(context.Background()); err != nil {
		t.Fatalf("syncFailed() error: %v", err)
	}
	if len(bridgeDB.markFailedCalls) != 2 {
		t.Fatalf("MarkFailed calls = %d, want 2 (descendant + ancestor)", len(bridgeDB.markFailedCalls))
	}
	if bridgeDB.markFailedCalls[1].id != 99 {
		t.Errorf("second MarkFailed id = %d, want 99", bridgeDB.markFailedCalls[1].id)
	}
}

func TestSyncFailed_AncestorEqualsID_NoSecondMark(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.findAncestorID = 1 // same as the descendant's own id

	relayerDB.ExpectQuery("SELECT id, parents, message FROM failed").
		WithArgs(int64(0), testBatchSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "parents", "message"}).AddRow(int64(1), []int64{5}, "timeout"))

	if err := idx.syncFailed(context.Background()); err != nil {
		t.Fatalf("syncFailed() error: %v", err)
	}
	if len(bridgeDB.markFailedCalls) != 1 {
		t.Errorf("MarkFailed calls = %d, want 1 (no redundant self-mark)", len(bridgeDB.markFailedCalls))
	}
}

func TestSyncFailed_CursorError(t *testing.T) {
	idx, _, bridgeDB := newTestIndexer(t)
	bridgeDB.getCursorErr = errors.New("db down")

	if err := idx.syncFailed(context.Background()); err == nil {
		t.Error("syncFailed() want error, got nil")
	}
}

func TestSyncFailed_QueryError(t *testing.T) {
	idx, relayerDB, _ := newTestIndexer(t)

	relayerDB.ExpectQuery("SELECT id, parents, message FROM failed").
		WithArgs(int64(0), testBatchSize).
		WillReturnError(errors.New("db down"))

	if err := idx.syncFailed(context.Background()); err == nil {
		t.Error("syncFailed() want error, got nil")
	}
}

// ── traceFailedAncestor ──────────────────────────────────────────────────────

func TestTraceFailedAncestor_EmptyParents(t *testing.T) {
	idx, _, _ := newTestIndexer(t)

	id, err := idx.traceFailedAncestor(context.Background(), nil)
	if err != nil {
		t.Fatalf("traceFailedAncestor() error: %v", err)
	}
	if id != 0 {
		t.Errorf("traceFailedAncestor() = %d, want 0", id)
	}
}

func TestTraceFailedAncestor_FoundDirectly(t *testing.T) {
	idx, _, bridgeDB := newTestIndexer(t)
	bridgeDB.findAncestorID = 42

	id, err := idx.traceFailedAncestor(context.Background(), []int64{5})
	if err != nil {
		t.Fatalf("traceFailedAncestor() error: %v", err)
	}
	if id != 42 {
		t.Errorf("traceFailedAncestor() = %d, want 42", id)
	}
}

func TestTraceFailedAncestor_FindAncestorError(t *testing.T) {
	idx, _, bridgeDB := newTestIndexer(t)
	bridgeDB.findAncestorErr = errors.New("db down")

	if _, err := idx.traceFailedAncestor(context.Background(), []int64{5}); err == nil {
		t.Error("traceFailedAncestor() want error, got nil")
	}
}

func TestTraceFailedAncestor_ClimbsToGrandparent(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)

	calls := 0
	bridgeDB.findAncestorFunc = func(ids []int64) (int64, error) {
		calls++
		if calls == 1 {
			return 0, nil // not found among [5]
		}
		return 42, nil // found among [10] (the grandparent)
	}

	relayerDB.ExpectQuery("SELECT parents FROM done WHERE id").
		WithArgs(int64(5)).
		WillReturnRows(pgxmock.NewRows([]string{"parents"}).AddRow([]int64{10}))

	id, err := idx.traceFailedAncestor(context.Background(), []int64{5})
	if err != nil {
		t.Fatalf("traceFailedAncestor() error: %v", err)
	}
	if id != 42 {
		t.Errorf("traceFailedAncestor() = %d, want 42", id)
	}
	if calls != 2 {
		t.Errorf("FindAncestor calls = %d, want 2", calls)
	}
}

func TestTraceFailedAncestor_GivesUpAfterMaxDepth(t *testing.T) {
	idx, relayerDB, bridgeDB := newTestIndexer(t)
	bridgeDB.findAncestorID = 0 // never found

	// Every grandparent lookup returns a fresh parent, so the loop only stops via depth limit.
	relayerDB.ExpectQuery("SELECT parents FROM done WHERE id").
		WillReturnRows(pgxmock.NewRows([]string{"parents"}).AddRow([]int64{6})).
		Times(1)
	relayerDB.MatchExpectationsInOrder(false)
	for i := int64(6); i < 20; i++ {
		relayerDB.ExpectQuery("SELECT parents FROM done WHERE id").
			WillReturnRows(pgxmock.NewRows([]string{"parents"}).AddRow([]int64{i + 1}))
	}

	id, err := idx.traceFailedAncestor(context.Background(), []int64{5})
	if err != nil {
		t.Fatalf("traceFailedAncestor() error: %v", err)
	}
	if id != 0 {
		t.Errorf("traceFailedAncestor() = %d, want 0 (gives up after max depth)", id)
	}
}

// ── poll ─────────────────────────────────────────────────────────────────────

func TestPoll_DelegatesToSyncProcessing(t *testing.T) {
	idx, _, bridgeDB := newTestIndexer(t)
	bridgeDB.getDetectedIDsErr = errors.New("db down")

	if err := idx.poll(context.Background()); err == nil {
		t.Error("poll() want error from syncProcessing, got nil")
	}
}
