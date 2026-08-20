package server

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTTLCache_GetOrLoad_CachesWithinTTL(t *testing.T) {
	c := newTTLCache[int, string](5 * time.Second)
	now := time.Now()
	c.now = func() time.Time { return now }

	var calls int32
	load := func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "value", nil
	}

	for i := range 3 {
		v, err := c.getOrLoad(1, load)
		if err != nil {
			t.Fatalf("call %d: getOrLoad() error: %v", i, err)
		}
		if v != "value" {
			t.Errorf("call %d: getOrLoad() = %q, want %q", i, v, "value")
		}
	}
	if calls != 1 {
		t.Errorf("load called %d times within TTL, want 1", calls)
	}
}

func TestTTLCache_GetOrLoad_ExpiresAfterTTL(t *testing.T) {
	c := newTTLCache[int, string](5 * time.Second)
	now := time.Now()
	c.now = func() time.Time { return now }

	var calls int32
	load := func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "value", nil
	}

	if _, err := c.getOrLoad(1, load); err != nil {
		t.Fatalf("getOrLoad() error: %v", err)
	}
	now = now.Add(6 * time.Second) // past the 5s TTL
	if _, err := c.getOrLoad(1, load); err != nil {
		t.Fatalf("getOrLoad() error: %v", err)
	}

	if calls != 2 {
		t.Errorf("load called %d times across TTL expiry, want 2", calls)
	}
}

func TestTTLCache_GetOrLoad_DistinctKeysLoadIndependently(t *testing.T) {
	c := newTTLCache[int, string](5 * time.Second)
	now := time.Now()
	c.now = func() time.Time { return now }

	var calls int32
	load := func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "value", nil
	}

	if _, err := c.getOrLoad(1, load); err != nil {
		t.Fatalf("getOrLoad(1) error: %v", err)
	}
	if _, err := c.getOrLoad(2, load); err != nil {
		t.Fatalf("getOrLoad(2) error: %v", err)
	}

	if calls != 2 {
		t.Errorf("load called %d times for 2 distinct keys, want 2", calls)
	}
}

func TestTTLCache_GetOrLoad_LoadErrorNotCached(t *testing.T) {
	c := newTTLCache[int, string](5 * time.Second)
	now := time.Now()
	c.now = func() time.Time { return now }

	wantErr := errors.New("db down")
	var calls int32
	load := func() (string, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return "", wantErr
		}
		return "value", nil
	}

	if _, err := c.getOrLoad(1, load); !errors.Is(err, wantErr) {
		t.Fatalf("getOrLoad() error = %v, want %v", err, wantErr)
	}
	v, err := c.getOrLoad(1, load)
	if err != nil {
		t.Fatalf("getOrLoad() error: %v", err)
	}
	if v != "value" {
		t.Errorf("getOrLoad() = %q, want %q", v, "value")
	}
	if calls != 2 {
		t.Errorf("load called %d times, want 2 (error result must not be cached)", calls)
	}
}

// Concurrent callers for the same key should share a single load.
func TestTTLCache_GetOrLoad_CollapsesConcurrentMisses(t *testing.T) {
	c := newTTLCache[int, string](5 * time.Second)

	var calls int32
	release := make(chan struct{})
	load := func() (string, error) {
		atomic.AddInt32(&calls, 1)
		<-release
		return "value", nil
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			v, err := c.getOrLoad(1, load)
			if err != nil {
				t.Errorf("getOrLoad() error: %v", err)
			}
			if v != "value" {
				t.Errorf("getOrLoad() = %q, want %q", v, "value")
			}
		}()
	}

	close(release)
	wg.Wait()

	if calls != 1 {
		t.Errorf("load called %d times for %d concurrent callers, want 1", calls, goroutines)
	}
}
