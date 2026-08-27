package metrics

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// CachingSource wraps a Source with a short TTL cache keyed by (addr, query),
// and collapses concurrent identical queries into a single upstream request
// via singleflight.
//
// Prometheus only refreshes a series once per scrape interval (typically
// 15-30s); querying it faster than that just repeats the same sample at full
// network cost. The cache bounds that cost. singleflight handles the other
// half of the same problem: when many callers ask for the same (addr, query)
// at once -- several ScaledObjects sharing a Prometheus and a query, or
// GetMetrics/IsActive/StreamIsActive all polling the same ScaledObject around
// the same time -- only one of them actually reaches Prometheus; the rest
// share its result.
//
// A CachingSource is safe for concurrent use and is intended to be
// constructed once per process and shared across every ScaledObject the
// scaler serves, so the cache and singleflight collapsing work across
// ScaledObjects, not just within one.
type CachingSource struct {
	// Source is the backend queried on a cache miss.
	Source Source

	group singleflight.Group

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	value     float64
	err       error
	expiresAt time.Time
}

// NewCachingSource wraps source with an initially empty cache.
func NewCachingSource(source Source) *CachingSource {
	return &CachingSource{Source: source, cache: make(map[string]cacheEntry)}
}

// cacheKey combines addr and query into one cache/singleflight key. A NUL
// byte can't appear in either input, so this can't collide two distinct
// (addr, query) pairs onto the same key.
func cacheKey(addr, query string) string {
	return addr + "\x00" + query
}

// Get returns addr/query's value, serving it from cache when a reading
// younger than ttl is already on hand. ttl <= 0 disables caching for this
// call -- the backend is always queried -- though concurrent identical calls
// still collapse into one upstream request via singleflight.
//
// Only a successful result, or ErrMissing (an absent series is itself a
// stable fact until Prometheus's next scrape), is written to the cache. Any
// other error -- a transport failure, a non-200 response, a malformed body --
// is never cached, so a transient upstream problem is retried on the very
// next call instead of being pinned for the full TTL.
func (c *CachingSource) Get(ctx context.Context, addr, query string, ttl time.Duration) (float64, error) {
	key := cacheKey(addr, query)

	if ttl > 0 {
		if v, err, ok := c.load(key); ok {
			return v, err
		}
	}

	res, err, _ := c.group.Do(key, func() (any, error) {
		v, err := c.Source.Instant(ctx, addr, query)
		if ttl > 0 && (err == nil || errors.Is(err, ErrMissing)) {
			c.store(key, v, err, ttl)
		}
		return v, err
	})
	return res.(float64), err
}

func (c *CachingSource) load(key string) (float64, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.cache[key]
	if !ok || time.Now().After(e.expiresAt) {
		return 0, nil, false
	}
	return e.value, e.err, true
}

func (c *CachingSource) store(key string, value float64, err error, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[key] = cacheEntry{value: value, err: err, expiresAt: time.Now().Add(ttl)}
}
