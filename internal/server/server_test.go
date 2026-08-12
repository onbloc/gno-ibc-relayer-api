package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
	"github.com/onbloc/gno-ibc-relayer-api/internal/db"
)

func TestNew(t *testing.T) {
	cfg := config.ServerConfig{Port: 8080}
	s := New(cfg, db.New(nil))

	if s == nil {
		t.Fatal("New() = nil")
	}
	if s.cfg != cfg {
		t.Errorf("New() cfg = %+v, want %+v", s.cfg, cfg)
	}
	if s.mux == nil {
		t.Fatal("New() mux = nil")
	}
}

func TestNew_RoutesRegistered(t *testing.T) {
	s := New(config.ServerConfig{Port: 8080}, db.New(nil))

	// An unregistered path must 404, proving the mux was built (without touching
	// any route that would dial the (nil) BridgeDB pool).
	req := httptest.NewRequest(http.MethodGet, "/no-such-route", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for unregistered route", w.Code)
	}
}
