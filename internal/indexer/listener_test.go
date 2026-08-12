package indexer

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

// captureLog redirects the standard logger's output for the duration of fn and
// returns what was logged, so tests can assert no misleading "success" line
// was printed when a mutation was actually blocked by a DB guard.
func captureLog(fn func()) string {
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(orig)
	fn()
	return buf.String()
}

// ── markWriteAckResult ───────────────────────────────────────────────────────

func TestMarkWriteAckResult_Success(t *testing.T) {
	fb := newFakeBridgeDB()
	idx := &Indexer{bridgeDB: fb}
	createdAt := time.Now()

	idx.markWriteAckResult(context.Background(), 7, createdAt, &ItemFields{
		AckSuccess: true,
		TxHash:     "0xabc",
	}, "notify")

	if len(fb.markDoneCalls) != 1 {
		t.Fatalf("MarkDone calls = %d, want 1", len(fb.markDoneCalls))
	}
	got := fb.markDoneCalls[0]
	if got.id != 7 || got.txIn != "0xabc" || !got.doneAt.Equal(createdAt) {
		t.Errorf("MarkDone call = %+v, want id=7 txIn=0xabc doneAt=%v", got, createdAt)
	}
	if len(fb.markFailedCalls) != 0 {
		t.Errorf("MarkFailed calls = %d, want 0", len(fb.markFailedCalls))
	}
}

// TestMarkWriteAckResult_Success_AlreadyTerminal verifies that when MarkDone's
// guard blocks the update (bridge already reached a terminal state), the code
// does not log a misleading "done via write_ack" success line.
func TestMarkWriteAckResult_Success_AlreadyTerminal(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markDoneMatched = false // guard blocked: bridge is already done/failed
	idx := &Indexer{bridgeDB: fb}

	out := captureLog(func() {
		idx.markWriteAckResult(context.Background(), 7, time.Now(), &ItemFields{AckSuccess: true, TxHash: "0xabc"}, "notify")
	})

	if len(fb.markDoneCalls) != 1 {
		t.Fatalf("MarkDone calls = %d, want 1", len(fb.markDoneCalls))
	}
	if strings.Contains(out, "done via write_ack") {
		t.Errorf("log = %q, must not claim success when the guard blocked the update", out)
	}
}

func TestMarkWriteAckResult_Success_MarkDoneError(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markDoneErr = errors.New("db down")
	idx := &Indexer{bridgeDB: fb}

	// Should not panic and should not fall through to MarkFailed.
	idx.markWriteAckResult(context.Background(), 7, time.Now(), &ItemFields{AckSuccess: true}, "notify")

	if len(fb.markDoneCalls) != 1 {
		t.Errorf("MarkDone calls = %d, want 1", len(fb.markDoneCalls))
	}
	if len(fb.markFailedCalls) != 0 {
		t.Errorf("MarkFailed calls = %d, want 0 (must not fall through on MarkDone error)", len(fb.markFailedCalls))
	}
}

func TestMarkWriteAckResult_AckFailure(t *testing.T) {
	fb := newFakeBridgeDB()
	idx := &Indexer{bridgeDB: fb}

	idx.markWriteAckResult(context.Background(), 7, time.Now(), &ItemFields{
		AckSuccess: false,
		AckError:   "insufficient funds",
		TxHash:     "0xdef",
	}, "startup catch-up")

	if len(fb.markDoneCalls) != 0 {
		t.Errorf("MarkDone calls = %d, want 0", len(fb.markDoneCalls))
	}
	if len(fb.markFailedCalls) != 1 {
		t.Fatalf("MarkFailed calls = %d, want 1", len(fb.markFailedCalls))
	}
	got := fb.markFailedCalls[0]
	if got.id != 7 || got.txIn != "0xdef" || got.errMsg != "ack error: insufficient funds" {
		t.Errorf("MarkFailed call = %+v, want id=7 txIn=0xdef errMsg=%q", got, "ack error: insufficient funds")
	}
}

func TestMarkWriteAckResult_AckFailure_NoInnerAck(t *testing.T) {
	fb := newFakeBridgeDB()
	idx := &Indexer{bridgeDB: fb}

	idx.markWriteAckResult(context.Background(), 7, time.Now(), &ItemFields{AckSuccess: false}, "notify")

	if len(fb.markFailedCalls) != 1 || fb.markFailedCalls[0].errMsg != "ack error" {
		t.Errorf("MarkFailed calls = %+v, want one call with errMsg=%q", fb.markFailedCalls, "ack error")
	}
}

func TestMarkWriteAckResult_AckFailure_MarkFailedError(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markFailedErr = errors.New("db down")
	idx := &Indexer{bridgeDB: fb}

	// Should not panic.
	idx.markWriteAckResult(context.Background(), 7, time.Now(), &ItemFields{AckSuccess: false}, "notify")

	if len(fb.markFailedCalls) != 1 {
		t.Errorf("MarkFailed calls = %d, want 1", len(fb.markFailedCalls))
	}
}

// TestMarkWriteAckResult_AckFailure_AlreadyDone verifies that when MarkFailed's
// guard blocks the update (bridge already reached a terminal state), the code
// does not log a misleading "failed via write_ack" success line.
func TestMarkWriteAckResult_AckFailure_AlreadyDone(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markFailedMatched = false // guard blocked: bridge is already done/failed
	idx := &Indexer{bridgeDB: fb}

	out := captureLog(func() {
		idx.markWriteAckResult(context.Background(), 7, time.Now(), &ItemFields{AckSuccess: false}, "notify")
	})

	if len(fb.markFailedCalls) != 1 {
		t.Fatalf("MarkFailed calls = %d, want 1", len(fb.markFailedCalls))
	}
	if strings.Contains(out, "failed via write_ack") {
		t.Errorf("log = %q, must not claim success when the guard blocked the update", out)
	}
}

// ── markPacketTimeoutFailed ──────────────────────────────────────────────────

func TestMarkPacketTimeoutFailed_Found(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.findByTimeoutAndChannelID = 7
	idx := &Indexer{bridgeDB: fb}

	idx.markPacketTimeoutFailed(context.Background(), PacketTimeoutFields{TimeoutTimestamp: 123, SrcChannelID: 2}, "notify")

	if len(fb.markFailedCalls) != 1 {
		t.Fatalf("MarkFailed calls = %d, want 1", len(fb.markFailedCalls))
	}
	if fb.markFailedCalls[0].id != 7 {
		t.Errorf("MarkFailed id = %d, want 7", fb.markFailedCalls[0].id)
	}
	if fb.markFailedCalls[0].txIn != "" {
		t.Errorf("MarkFailed txIn = %q, want empty (no tx hash for packet_timeout)", fb.markFailedCalls[0].txIn)
	}
}

func TestMarkPacketTimeoutFailed_NotFound(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.findByTimeoutAndChannelID = 0
	idx := &Indexer{bridgeDB: fb}

	idx.markPacketTimeoutFailed(context.Background(), PacketTimeoutFields{TimeoutTimestamp: 123, SrcChannelID: 2}, "notify")

	if len(fb.markFailedCalls) != 0 {
		t.Errorf("MarkFailed calls = %d, want 0 (no matching transfer)", len(fb.markFailedCalls))
	}
}

func TestMarkPacketTimeoutFailed_FindError(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.findByTimeoutAndChannelErr = errors.New("db down")
	idx := &Indexer{bridgeDB: fb}

	idx.markPacketTimeoutFailed(context.Background(), PacketTimeoutFields{TimeoutTimestamp: 123, SrcChannelID: 2}, "notify")

	if len(fb.markFailedCalls) != 0 {
		t.Errorf("MarkFailed calls = %d, want 0 (find errored)", len(fb.markFailedCalls))
	}
}

func TestMarkPacketTimeoutFailed_MarkFailedError(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.findByTimeoutAndChannelID = 7
	fb.markFailedErr = errors.New("db down")
	idx := &Indexer{bridgeDB: fb}

	// Should not panic.
	idx.markPacketTimeoutFailed(context.Background(), PacketTimeoutFields{TimeoutTimestamp: 123, SrcChannelID: 2}, "notify")

	if len(fb.markFailedCalls) != 1 {
		t.Errorf("MarkFailed calls = %d, want 1", len(fb.markFailedCalls))
	}
}

// TestMarkPacketTimeoutFailed_AlreadyDone verifies that when MarkFailed's guard
// blocks the update (bridge already reached a terminal state — e.g. a
// packet_timeout arriving after the bridge was already marked done), the code
// does not log a misleading "failed via packet_timeout" success line.
func TestMarkPacketTimeoutFailed_AlreadyDone(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.findByTimeoutAndChannelID = 7
	fb.markFailedMatched = false // guard blocked: bridge is already done/failed
	idx := &Indexer{bridgeDB: fb}

	out := captureLog(func() {
		idx.markPacketTimeoutFailed(context.Background(), PacketTimeoutFields{TimeoutTimestamp: 123, SrcChannelID: 2}, "notify")
	})

	if len(fb.markFailedCalls) != 1 {
		t.Fatalf("MarkFailed calls = %d, want 1", len(fb.markFailedCalls))
	}
	if strings.Contains(out, "failed via packet_timeout") {
		t.Errorf("log = %q, must not claim success when the guard blocked the update", out)
	}
}

// ── handleFailedNotification ─────────────────────────────────────────────────

func TestHandleFailedNotification_DirectMatch(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markFailedMatched = true
	idx := &Indexer{bridgeDB: fb}

	idx.handleFailedNotification(context.Background(), 42, nil, "timeout")

	if len(fb.markFailedCalls) != 1 || fb.markFailedCalls[0].id != 42 {
		t.Fatalf("MarkFailed calls = %+v, want one call with id=42", fb.markFailedCalls)
	}
}

func TestHandleFailedNotification_NoDirectMatch_FallsBackToPromiseMatch(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markFailedMatched = false // id=42 isn't a tracked bridge directly
	fb.findByTimeoutAndChannelID = 99
	idx := &Indexer{bridgeDB: fb}

	item := buildPromiseItem("packet_timeout", 123, 2)
	idx.handleFailedNotification(context.Background(), 42, item, "timeout")

	if len(fb.markFailedCalls) != 2 {
		t.Fatalf("MarkFailed calls = %d, want 2 (direct attempt + fallback match)", len(fb.markFailedCalls))
	}
	if fb.markFailedCalls[0].id != 42 {
		t.Errorf("first MarkFailed id = %d, want 42 (direct attempt)", fb.markFailedCalls[0].id)
	}
	if fb.markFailedCalls[1].id != 99 {
		t.Errorf("second MarkFailed id = %d, want 99 (fallback match)", fb.markFailedCalls[1].id)
	}
}

// TestHandleFailedNotification_FallbackMatch_AlreadyTerminal verifies that
// when the fallback's own MarkFailed call is blocked by the terminal-state
// guard, the code does not log a misleading "via promise notify" success
// line — the matched bool must be checked on the fallback path too, not just
// the direct one.
func TestHandleFailedNotification_FallbackMatch_AlreadyTerminal(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.findByTimeoutAndChannelID = 99
	fb.markFailedFunc = func(id int64) (bool, error) {
		if id == 42 {
			return false, nil // direct attempt: no bridge tracked under this id
		}
		return false, nil // fallback attempt (id=99): guard blocked, already terminal
	}
	idx := &Indexer{bridgeDB: fb}

	out := captureLog(func() {
		item := buildPromiseItem("packet_timeout", 123, 2)
		idx.handleFailedNotification(context.Background(), 42, item, "timeout")
	})

	if len(fb.markFailedCalls) != 2 {
		t.Fatalf("MarkFailed calls = %d, want 2 (direct attempt + fallback match)", len(fb.markFailedCalls))
	}
	if strings.Contains(out, "via promise notify") {
		t.Errorf("log = %q, must not claim success when the fallback's guard blocked the update", out)
	}
}

// TestHandleFailedNotification_FallbackMatch_Succeeds is the positive
// counterpart: when the fallback's MarkFailed genuinely matches a row, the
// success line must still be logged.
func TestHandleFailedNotification_FallbackMatch_Succeeds(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.findByTimeoutAndChannelID = 99
	fb.markFailedFunc = func(id int64) (bool, error) {
		return id == 99, nil // only the fallback id actually matches
	}
	idx := &Indexer{bridgeDB: fb}

	out := captureLog(func() {
		item := buildPromiseItem("packet_timeout", 123, 2)
		idx.handleFailedNotification(context.Background(), 42, item, "timeout")
	})

	if !strings.Contains(out, "failed bridge id=99 via promise notify") {
		t.Errorf("log = %q, want it to report success for the fallback match", out)
	}
}

func TestHandleFailedNotification_NoDirectMatch_NoPromiseMatch(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markFailedMatched = false
	fb.findByTimeoutAndChannelID = 0 // fallback also finds nothing
	idx := &Indexer{bridgeDB: fb}

	item := buildPromiseItem("packet_timeout", 123, 2)
	idx.handleFailedNotification(context.Background(), 42, item, "timeout")

	if len(fb.markFailedCalls) != 1 {
		t.Errorf("MarkFailed calls = %d, want 1 (only the direct attempt; no second call when fallback finds nothing)", len(fb.markFailedCalls))
	}
}

func TestHandleFailedNotification_DirectError_NoFallback(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markFailedErr = errors.New("db down")
	idx := &Indexer{bridgeDB: fb}

	item := buildPromiseItem("packet_timeout", 123, 2)
	idx.handleFailedNotification(context.Background(), 42, item, "timeout")

	if len(fb.markFailedCalls) != 1 {
		t.Errorf("MarkFailed calls = %d, want 1 (a genuine error must not trigger the promise fallback)", len(fb.markFailedCalls))
	}
}

func TestHandleFailedNotification_UnparsablePromiseItem(t *testing.T) {
	fb := newFakeBridgeDB()
	fb.markFailedMatched = false
	idx := &Indexer{bridgeDB: fb}

	idx.handleFailedNotification(context.Background(), 42, []byte("not json"), "timeout")

	if len(fb.markFailedCalls) != 1 {
		t.Errorf("MarkFailed calls = %d, want 1 (unparsable item skips the fallback cleanly)", len(fb.markFailedCalls))
	}
}
