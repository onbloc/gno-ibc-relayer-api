package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/onbloc/gno-ibc-relayer-api/internal/model"
	"github.com/onbloc/gno-ibc-relayer-api/internal/repository"
)

// stubRepo is a test double for transferRepository.
type stubRepo struct {
	transfer  *model.Transfer
	transfers []*model.Transfer
	count     int64
	err       error
}

func (s *stubRepo) GetByPacketHash(_ context.Context, _ string) (*model.Transfer, error) {
	return s.transfer, s.err
}

func (s *stubRepo) List(_ context.Context, _ repository.ListFilter) ([]*model.Transfer, error) {
	return s.transfers, s.err
}

func (s *stubRepo) Count(_ context.Context) (int64, error) {
	return s.count, s.err
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
		{"limit=100", 100, 0},  // max allowed
		{"limit=101", 20, 0},   // exceeds max → default
		{"limit=0", 20, 0},     // zero → default
		{"limit=-5", 20, 0},    // negative → default
		{"limit=5&offset=10", 5, 10},
		{"limit=abc", 20, 0},   // non-numeric → default
		{"offset=abc", 20, 0},  // non-numeric offset → 0
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
	want := &model.Transfer{ID: 1, PacketHash: "abc123"}
	h := NewTransferHandler(&stubRepo{transfer: want})

	req := httptest.NewRequest(http.MethodGet, "/status/abc123", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("packet_hash", "abc123")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h.GetByPacketHash(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var got model.Transfer
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.PacketHash != want.PacketHash {
		t.Errorf("packet_hash = %q, want %q", got.PacketHash, want.PacketHash)
	}
}

func TestGetByPacketHash_NotFound(t *testing.T) {
	h := NewTransferHandler(&stubRepo{err: errors.New("not found")})

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
	transfers := []*model.Transfer{
		{ID: 1, PacketHash: "hash1"},
		{ID: 2, PacketHash: "hash2"},
	}
	h := NewTransferHandler(&stubRepo{transfers: transfers})

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
		Data   []*model.Transfer `json:"data"`
		Limit  int               `json:"limit"`
		Offset int               `json:"offset"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data) != len(transfers) {
		t.Errorf("data count = %d, want %d", len(resp.Data), len(transfers))
	}
	if resp.Limit != 10 {
		t.Errorf("limit = %d, want 10", resp.Limit)
	}
	if resp.Offset != 5 {
		t.Errorf("offset = %d, want 5", resp.Offset)
	}
}

func TestListByWallet_Error(t *testing.T) {
	h := NewTransferHandler(&stubRepo{err: errors.New("db error")})

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
	transfers := []*model.Transfer{{ID: 1, PacketHash: "h1"}}
	h := NewTransferHandler(&stubRepo{transfers: transfers})

	req := httptest.NewRequest(http.MethodGet, "/history?limit=5", nil)
	w := httptest.NewRecorder()
	h.History(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp struct {
		Data  []*model.Transfer `json:"data"`
		Limit int               `json:"limit"`
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
	h := NewTransferHandler(&stubRepo{err: errors.New("db error")})

	req := httptest.NewRequest(http.MethodGet, "/history", nil)
	w := httptest.NewRecorder()
	h.History(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

// ── Summary ───────────────────────────────────────────────────────────────────

func TestSummary(t *testing.T) {
	h := NewStatsHandler(&stubRepo{count: 42})

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
	h := NewStatsHandler(&stubRepo{err: errors.New("db error")})

	req := httptest.NewRequest(http.MethodGet, "/summary", nil)
	w := httptest.NewRecorder()
	h.Summary(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}
