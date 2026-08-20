package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

// stubStore is a test double for BridgeDB.
type stubStore struct {
	bridge  *db.BridgeRecord
	bridges []*db.BridgeRecord
	count   int64
	summary *db.StatusSummary
	err     error

	recentLimits []int // limit passed to each CountRecentByStatus call, in order
}

func (s *stubStore) GetByPacketHash(_ context.Context, _ string) (*db.BridgeRecord, error) {
	return s.bridge, s.err
}

func (s *stubStore) List(_ context.Context, _ db.ListFilter) ([]*db.BridgeRecord, error) {
	return s.bridges, s.err
}

func (s *stubStore) Count(_ context.Context) (int64, error) {
	return s.count, s.err
}

func (s *stubStore) CountRecentByStatus(_ context.Context, limit int) (*db.StatusSummary, error) {
	s.recentLimits = append(s.recentLimits, limit)
	return s.summary, s.err
}

// ── parsePagination ───────────────────────────────────────────────────────────

func TestParsePagination(t *testing.T) {
	cases := []struct {
		query      string
		wantLimit  int
		wantOffset int
	}{
		{"", 20, 0},
		{"limit=10", 10, 0},
		{"limit=100", 100, 0}, // max allowed
		{"limit=101", 20, 0},  // exceeds max → default
		{"limit=0", 20, 0},    // zero → default
		{"limit=-5", 20, 0},   // negative → default
		{"limit=5&offset=10", 5, 10},
		{"limit=abc", 20, 0},  // non-numeric → default
		{"offset=abc", 20, 0}, // non-numeric offset → 0
	}
	for _, tc := range cases {
		u, _ := url.Parse("/?" + tc.query)
		gotLimit, gotOffset := parsePagination(u.Query())
		if gotLimit != tc.wantLimit || gotOffset != tc.wantOffset {
			t.Errorf("query=%q: got limit=%d offset=%d, want limit=%d offset=%d",
				tc.query, gotLimit, gotOffset, tc.wantLimit, tc.wantOffset)
		}
	}
}

// ── GetByPacketHash ───────────────────────────────────────────────────────────

func TestGetByPacketHash_Found(t *testing.T) {
	want := &db.BridgeRecord{ID: 1, PacketHash: "abc123"}
	h := NewBridgeHandler(&stubStore{bridge: want})

	req := httptest.NewRequest(http.MethodGet, "/status/abc123", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("packet_hash", "abc123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.GetByPacketHash(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var got db.BridgeRecord
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.PacketHash != want.PacketHash {
		t.Errorf("packet_hash = %q, want %q", got.PacketHash, want.PacketHash)
	}
}

func TestGetByPacketHash_NotFound(t *testing.T) {
	h := NewBridgeHandler(&stubStore{err: errors.New("not found")})

	req := httptest.NewRequest(http.MethodGet, "/status/missing", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("packet_hash", "missing")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.GetByPacketHash(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// ── ListByWallet ──────────────────────────────────────────────────────────────

func TestListByWallet(t *testing.T) {
	bridges := []*db.BridgeRecord{
		{ID: 1, PacketHash: "hash1"},
		{ID: 2, PacketHash: "hash2"},
	}
	h := NewBridgeHandler(&stubStore{bridges: bridges})

	req := httptest.NewRequest(http.MethodGet, "/wallet/g1abc?limit=10&offset=5", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("sender_address", "g1abc")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.ListByWallet(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp struct {
		Data   []*db.BridgeRecord `json:"data"`
		Limit  int                `json:"limit"`
		Offset int                `json:"offset"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data) != len(bridges) {
		t.Errorf("data count = %d, want %d", len(resp.Data), len(bridges))
	}
	if resp.Limit != 10 {
		t.Errorf("limit = %d, want 10", resp.Limit)
	}
	if resp.Offset != 5 {
		t.Errorf("offset = %d, want 5", resp.Offset)
	}
}

func TestListByWallet_Error(t *testing.T) {
	h := NewBridgeHandler(&stubStore{err: errors.New("db error")})

	req := httptest.NewRequest(http.MethodGet, "/wallet/g1abc", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("sender_address", "g1abc")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.ListByWallet(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

// ── History ───────────────────────────────────────────────────────────────────

func TestHistory(t *testing.T) {
	bridges := []*db.BridgeRecord{{ID: 1, PacketHash: "h1"}}
	h := NewBridgeHandler(&stubStore{bridges: bridges})

	req := httptest.NewRequest(http.MethodGet, "/history?limit=5", nil)
	w := httptest.NewRecorder()
	h.History(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp struct {
		Data  []*db.BridgeRecord `json:"data"`
		Limit int                `json:"limit"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Errorf("data count = %d, want 1", len(resp.Data))
	}
	if resp.Limit != 5 {
		t.Errorf("limit = %d, want 5", resp.Limit)
	}
}

func TestHistory_Error(t *testing.T) {
	h := NewBridgeHandler(&stubStore{err: errors.New("db error")})

	req := httptest.NewRequest(http.MethodGet, "/history", nil)
	w := httptest.NewRecorder()
	h.History(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

// ── Summary ───────────────────────────────────────────────────────────────────

func TestSummary(t *testing.T) {
	h := NewStatsHandler(&stubStore{count: 42})

	req := httptest.NewRequest(http.MethodGet, "/summary", nil)
	w := httptest.NewRecorder()
	h.Summary(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp struct {
		Total int64 `json:"total"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 42 {
		t.Errorf("total = %d, want 42", resp.Total)
	}
}

func TestSummary_Error(t *testing.T) {
	h := NewStatsHandler(&stubStore{err: errors.New("db error")})

	req := httptest.NewRequest(http.MethodGet, "/summary", nil)
	w := httptest.NewRecorder()
	h.Summary(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

// ── parseRecentLimit ──────────────────────────────────────────────────────────

func TestParseRecentLimit(t *testing.T) {
	cases := []struct {
		query string
		want  int
	}{
		{"", defaultRecentLimit},
		{"limit=500", 500},
		{"limit=5000", 5000},               // max allowed
		{"limit=5001", defaultRecentLimit}, // exceeds max -> default
		{"limit=0", defaultRecentLimit},
		{"limit=-5", defaultRecentLimit},
		{"limit=abc", defaultRecentLimit},
	}
	for _, tc := range cases {
		u, _ := url.Parse("/?" + tc.query)
		if got := parseRecentLimit(u.Query()); got != tc.want {
			t.Errorf("query=%q: parseRecentLimit() = %d, want %d", tc.query, got, tc.want)
		}
	}
}

// ── RecentSummary ─────────────────────────────────────────────────────────────

func TestRecentSummary(t *testing.T) {
	want := &db.StatusSummary{Total: 1000, Detected: 10, Processing: 20, Succeeded: 900, Failed: 70}
	store := &stubStore{summary: want}
	h := NewStatsHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/summary/recent?limit=1000", nil)
	w := httptest.NewRecorder()
	h.RecentSummary(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var got db.StatusSummary
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != *want {
		t.Errorf("RecentSummary() = %+v, want %+v", got, want)
	}
	if len(store.recentLimits) != 1 || store.recentLimits[0] != 1000 {
		t.Errorf("CountRecentByStatus called with limits %v, want [1000]", store.recentLimits)
	}
}

func TestRecentSummary_CachesWithinTTL(t *testing.T) {
	store := &stubStore{summary: &db.StatusSummary{Total: 1000}}
	h := NewStatsHandler(store)

	for i := range 3 {
		req := httptest.NewRequest(http.MethodGet, "/summary/recent", nil)
		w := httptest.NewRecorder()
		h.RecentSummary(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("call %d: status = %d, want 200", i, w.Code)
		}
	}

	if len(store.recentLimits) != 1 {
		t.Errorf("CountRecentByStatus called %d times within TTL, want 1", len(store.recentLimits))
	}
}

func TestRecentSummary_Error(t *testing.T) {
	h := NewStatsHandler(&stubStore{err: errors.New("db error")})

	req := httptest.NewRequest(http.MethodGet, "/summary/recent", nil)
	w := httptest.NewRecorder()
	h.RecentSummary(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}
