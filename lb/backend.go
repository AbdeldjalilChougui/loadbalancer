package lb

import (
	"net/url"
	"sync"
	"sync/atomic"
)

// Backend is one upstream server.
type Backend struct {
	URL     *url.URL
	weight  atomic.Int64
	healthy atomic.Bool // set by the active health checker
	active  atomic.Int64
	Breaker *Breaker

	currentWeight int // smooth WRR state, guarded by the pool lock

	// Health check streaks, touched only by the checker goroutine.
	okStreak, failStreak int

	statsMu  sync.Mutex
	requests map[int]uint64 // by status code; 0 = transport error
}

func newBackend(u *url.URL, weight int, br *Breaker) *Backend {
	b := &Backend{URL: u, Breaker: br, requests: map[int]uint64{}}
	b.weight.Store(int64(weight))
	b.healthy.Store(true) // optimistic until the checker says otherwise
	return b
}

func (b *Backend) Weight() int    { return int(b.weight.Load()) }
func (b *Backend) Active() int64  { return b.active.Load() }
func (b *Backend) Healthy() bool  { return b.healthy.Load() }
func (b *Backend) String() string { return b.URL.String() }

// available: passes health checks and the breaker would let a request through.
func (b *Backend) available() bool { return b.Healthy() && b.Breaker.Ready() }

func (b *Backend) record(code int) {
	b.statsMu.Lock()
	b.requests[code]++
	b.statsMu.Unlock()
}

func (b *Backend) snapshotRequests() map[int]uint64 {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	out := make(map[int]uint64, len(b.requests))
	for k, v := range b.requests {
		out[k] = v
	}
	return out
}
