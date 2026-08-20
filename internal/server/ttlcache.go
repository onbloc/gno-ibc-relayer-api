package server

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// ttlCache is a generic in-process cache with per-entry TTL. Concurrent misses for the same key
// collapse into a single load via singleflight.
type ttlCache[K comparable, V any] struct {
	ttl time.Duration
	now func() time.Time // overridable in tests
	sf  singleflight.Group

	mu      sync.Mutex
	entries map[K]ttlEntry[V]
}

type ttlEntry[V any] struct {
	value     V
	expiresAt time.Time
}

func newTTLCache[K comparable, V any](ttl time.Duration) *ttlCache[K, V] {
	return &ttlCache[K, V]{
		ttl:     ttl,
		now:     time.Now,
		entries: make(map[K]ttlEntry[V]),
	}
}

// getOrLoad returns the cached value for key, loading (and caching) it on a miss. Concurrent
// calls for the same key share one load.
func (c *ttlCache[K, V]) getOrLoad(key K, load func() (V, error)) (V, error) {
	if v, ok := c.get(key); ok {
		return v, nil
	}

	v, err, _ := c.sf.Do(fmt.Sprint(key), func() (any, error) {
		if v, ok := c.get(key); ok {
			return v, nil
		}
		v, err := load()
		if err != nil {
			return v, err
		}
		c.set(key, v)
		return v, nil
	})
	if err != nil {
		var zero V
		return zero, err
	}
	return v.(V), nil
}

func (c *ttlCache[K, V]) get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || c.now().After(e.expiresAt) {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (c *ttlCache[K, V]) set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = ttlEntry[V]{value: value, expiresAt: c.now().Add(c.ttl)}
}
