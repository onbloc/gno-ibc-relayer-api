package indexer

import (
	"context"
	"time"

	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

// fakeBridgeDB is a hand-rolled BridgeDB test double that records every call
// it receives and returns canned results configured per test.
type fakeBridgeDB struct {
	dedupeN   int64
	dedupeErr error

	insertErr error
	inserted  []*db.BridgeRecord

	markProcessingErr error
	markProcessingIDs [][]int64

	markDoneErr     error
	markDoneMatched bool // returned as MarkDone's matched bool; defaults to true, see newFakeBridgeDB
	markDoneCalls   []markDoneCall

	setTxInErr   error
	setTxInCalls []setTxInCall

	markFailedErr     error
	markFailedMatched bool                         // returned as MarkFailed's matched bool; defaults to true, see newFakeBridgeDB
	markFailedFunc    func(id int64) (bool, error) // overrides the fields above when set
	markFailedCalls   []markFailedCall

	cursors      map[string]int64
	getCursorErr error
	setCursorErr error

	findByTimeoutAndChannelID  int64
	findByTimeoutAndChannelErr error

	findByPacketHashID  int64
	findByPacketHashErr error

	findAncestorID   int64
	findAncestorErr  error
	findAncestorFunc func(ids []int64) (int64, error) // overrides the fields above when set

	detectedIDs       []int64
	getDetectedIDsErr error

	inFlightCreatedAt []time.Time
	getInFlightErr    error
}

type markDoneCall struct {
	id     int64
	doneAt time.Time
	txIn   string
}

type setTxInCall struct {
	packetHash string
	txIn       string
}

type markFailedCall struct {
	id     int64
	errMsg string
	txIn   string
}

func newFakeBridgeDB() *fakeBridgeDB {
	return &fakeBridgeDB{cursors: map[string]int64{}, markDoneMatched: true, markFailedMatched: true}
}

func (f *fakeBridgeDB) DedupeByPacketHash(ctx context.Context) (int64, error) {
	return f.dedupeN, f.dedupeErr
}

func (f *fakeBridgeDB) Insert(ctx context.Context, t *db.BridgeRecord) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserted = append(f.inserted, t)
	return nil
}

func (f *fakeBridgeDB) MarkProcessing(ctx context.Context, ids []int64) error {
	if f.markProcessingErr != nil {
		return f.markProcessingErr
	}
	f.markProcessingIDs = append(f.markProcessingIDs, ids)
	return nil
}

func (f *fakeBridgeDB) MarkDone(ctx context.Context, id int64, doneAt time.Time, txIn string) (bool, error) {
	f.markDoneCalls = append(f.markDoneCalls, markDoneCall{id, doneAt, txIn})
	if f.markDoneErr != nil {
		return false, f.markDoneErr
	}
	return f.markDoneMatched, nil
}

func (f *fakeBridgeDB) SetTxIn(ctx context.Context, packetHash, txIn string) error {
	f.setTxInCalls = append(f.setTxInCalls, setTxInCall{packetHash, txIn})
	return f.setTxInErr
}

func (f *fakeBridgeDB) MarkFailed(ctx context.Context, id int64, errMsg string, txIn string) (bool, error) {
	f.markFailedCalls = append(f.markFailedCalls, markFailedCall{id, errMsg, txIn})
	if f.markFailedFunc != nil {
		return f.markFailedFunc(id)
	}
	if f.markFailedErr != nil {
		return false, f.markFailedErr
	}
	return f.markFailedMatched, nil
}

func (f *fakeBridgeDB) GetCursor(ctx context.Context, name string) (int64, error) {
	if f.getCursorErr != nil {
		return 0, f.getCursorErr
	}
	return f.cursors[name], nil
}

func (f *fakeBridgeDB) SetCursor(ctx context.Context, name string, id int64) error {
	if f.setCursorErr != nil {
		return f.setCursorErr
	}
	f.cursors[name] = id
	return nil
}

func (f *fakeBridgeDB) FindByTimeoutAndChannel(ctx context.Context, timeoutTimestamp int64, srcChannelID int) (int64, error) {
	return f.findByTimeoutAndChannelID, f.findByTimeoutAndChannelErr
}

func (f *fakeBridgeDB) FindByPacketHash(ctx context.Context, packetHash string) (int64, error) {
	return f.findByPacketHashID, f.findByPacketHashErr
}

func (f *fakeBridgeDB) FindAncestor(ctx context.Context, ids []int64) (int64, error) {
	if f.findAncestorFunc != nil {
		return f.findAncestorFunc(ids)
	}
	return f.findAncestorID, f.findAncestorErr
}

func (f *fakeBridgeDB) GetDetectedIDs(ctx context.Context) ([]int64, error) {
	return f.detectedIDs, f.getDetectedIDsErr
}

func (f *fakeBridgeDB) GetInFlightCreatedAt(ctx context.Context) ([]time.Time, error) {
	return f.inFlightCreatedAt, f.getInFlightErr
}
