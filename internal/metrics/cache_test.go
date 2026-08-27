package metrics

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingSource is a Source whose Instant counts real upstream calls and
// returns a distinct value (the call count) each time, so a test can tell a
// cache hit (unchanged value, unchanged count) from a fresh upstream call
// (new value, incremented count).
type countingSource struct {
	mu    sync.Mutex
	count int
}

func (c *countingSource) Instant(_ context.Context, _, _ string) (float64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
	return float64(c.count), nil
}

func (c *countingSource) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func TestCachingSourceHonoursTTL(t *testing.T) {
	src := &countingSource{}
	c := NewCachingSource(src)
	ttl := 30 * time.Millisecond

	v1, err := c.Get(context.Background(), "addr", "q", ttl)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	v2, err := c.Get(context.Background(), "addr", "q", ttl)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v1 != v2 {
		t.Fatalf("expected the second call within the TTL window to be served from cache: v1=%v v2=%v", v1, v2)
	}
	if got := src.calls(); got != 1 {
		t.Fatalf("expected exactly 1 upstream call within the TTL window, got %d", got)
	}

	time.Sleep(ttl + 40*time.Millisecond)

	v3, err := c.Get(context.Background(), "addr", "q", ttl)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v3 == v1 {
		t.Fatalf("expected a fresh value once the TTL expired, still got %v", v3)
	}
	if got := src.calls(); got != 2 {
		t.Fatalf("expected exactly 2 upstream calls total after the TTL expired, got %d", got)
	}
}

func TestCachingSourceZeroTTLDisablesCaching(t *testing.T) {
	src := &countingSource{}
	c := NewCachingSource(src)

	if _, err := c.Get(context.Background(), "addr", "q", 0); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.Get(context.Background(), "addr", "q", 0); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := src.calls(); got != 2 {
		t.Fatalf("expected every call to reach the backend with ttl<=0, got %d upstream call(s)", got)
	}
}

// blockingSource is a Source whose Instant counts calls and blocks until
// release is closed, letting a test hold several concurrent callers inside
// singleflight at once and assert only one of them reached the backend.
type blockingSource struct {
	calls   int32
	release chan struct{}
}

func (b *blockingSource) Instant(_ context.Context, _, _ string) (float64, error) {
	atomic.AddInt32(&b.calls, 1)
	<-b.release
	return 42, nil
}

func TestCachingSourceSingleflightCollapsesConcurrentCallers(t *testing.T) {
	src := &blockingSource{release: make(chan struct{})}
	c := NewCachingSource(src)

	const n = 5
	var wg sync.WaitGroup
	results := make([]float64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.Get(context.Background(), "addr", "q", time.Second)
			results[i] = v
			errs[i] = err
		}(i)
	}

	// Give every goroutine a chance to reach Instant and block there before
	// releasing them, so all n calls are genuinely concurrent and in flight
	// together when singleflight collapses them.
	time.Sleep(50 * time.Millisecond)
	close(src.release)
	wg.Wait()

	if got := atomic.LoadInt32(&src.calls); got != 1 {
		t.Fatalf("expected exactly 1 upstream call for %d concurrent callers sharing a query, got %d", n, got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: unexpected error: %v", i, err)
		}
		if results[i] != 42 {
			t.Fatalf("caller %d: got %v, want 42", i, results[i])
		}
	}
}

func TestCachingSourceCachesErrMissingButNotOtherErrors(t *testing.T) {
	t.Run("ErrMissing is cached", func(t *testing.T) {
		src := &erroringSource{err: ErrMissing}
		c := NewCachingSource(src)
		ttl := time.Second

		if _, err := c.Get(context.Background(), "addr", "q", ttl); !errors.Is(err, ErrMissing) {
			t.Fatalf("expected ErrMissing, got %v", err)
		}
		if _, err := c.Get(context.Background(), "addr", "q", ttl); !errors.Is(err, ErrMissing) {
			t.Fatalf("expected cached ErrMissing, got %v", err)
		}
		if got := src.calls(); got != 1 {
			t.Fatalf("expected ErrMissing to be served from cache on the second call, got %d upstream call(s)", got)
		}
	})

	t.Run("other errors are not cached", func(t *testing.T) {
		src := &erroringSource{err: errors.New("boom")}
		c := NewCachingSource(src)
		ttl := time.Second

		if _, err := c.Get(context.Background(), "addr", "q", ttl); err == nil {
			t.Fatal("expected an error")
		}
		if _, err := c.Get(context.Background(), "addr", "q", ttl); err == nil {
			t.Fatal("expected an error")
		}
		if got := src.calls(); got != 2 {
			t.Fatalf("expected a non-ErrMissing error to be retried rather than cached, got %d upstream call(s)", got)
		}
	})
}

// erroringSource is a Source whose Instant always fails with err, counting
// how many times it was actually invoked.
type erroringSource struct {
	mu    sync.Mutex
	count int
	err   error
}

func (e *erroringSource) Instant(_ context.Context, _, _ string) (float64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.count++
	return 0, e.err
}

func (e *erroringSource) calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.count
}
