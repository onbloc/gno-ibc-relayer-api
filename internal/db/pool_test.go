package db

import (
	"context"
	"testing"

	"github.com/onbloc/gno-ibc-relayer-api/internal/config"
)

// TestNewPool_ContextCanceled uses an already-canceled context instead of an
// unreachable host + timeout, so the failure is deterministic and instant
// regardless of the CI network/OS routing behavior.
func TestNewPool_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := config.DBConfig{
		Host:    "localhost",
		Port:    5432,
		User:    "postgres",
		DBName:  "bridge",
		SSLMode: "disable",
	}

	if _, err := NewPool(ctx, cfg); err == nil {
		t.Error("NewPool() with canceled context: want error, got nil")
	}
}

func TestNew(t *testing.T) {
	bdb := New(nil)
	if bdb == nil {
		t.Fatal("New() = nil")
	}
}
