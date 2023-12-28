package lb

import (
	"sync"
	"time"
)

// BreakerState is the classic three-state circuit breaker.
type BreakerState int

const (
	Closed   BreakerState = iota // traffic flows, failures are counted
	Open                         // traffic blocked until the cooldown passes
	HalfOpen                     // one trial request decides: close or re-open
)

func (s BreakerState) String() string {
	return [...]string{"closed", "open", "half_open"}[s]
}

// Breaker implements passive failure detection: real requests that fail are
// counted, and after Threshold consecutive failures the backend is taken out
// of rotation for Cooldown. Then one trial request is let through.
type Breaker struct {
	mu        sync.Mutex
	state     BreakerState
	failures  int
	openedAt  time.Time
	trial     bool // a half-open trial request is in flight
	Threshold int  // 0 disables the breaker
	Cooldown  time.Duration
	Now       func() time.Time
}

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{Threshold: threshold, Cooldown: cooldown, Now: time.Now}
}

// State reports the current state (Open becomes "ready for trial" lazily).
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Ready reports whether a request could be sent now, without reserving it.
// Strategies call this to filter candidates.
func (b *Breaker) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		return !b.trial && b.Now().Sub(b.openedAt) >= b.Cooldown
	default:
		return !b.trial
	}
}

// Acquire reserves the right to send a request. In half-open state only one
// caller wins.
func (b *Breaker) Acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		if b.Now().Sub(b.openedAt) < b.Cooldown {
			return false
		}
		b.state = HalfOpen
		fallthrough
	default:
		if b.trial {
			return false
		}
		b.trial = true
		return true
	}
}

// Success records a good response.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state, b.failures, b.trial = Closed, 0, false
}

// Failure records a failed request (transport error or 5xx).
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Threshold <= 0 {
		return
	}
	switch b.state {
	case HalfOpen:
		b.state, b.openedAt, b.trial = Open, b.Now(), false
	case Closed:
		b.failures++
		if b.failures >= b.Threshold {
			b.state, b.openedAt = Open, b.Now()
		}
	}
}

// Release gives back a half-open reservation that was never used (e.g. the
// request was cancelled by the client).
func (b *Breaker) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.trial = false
}

// SetParams updates the threshold and cooldown (used on config reload).
func (b *Breaker) SetParams(threshold int, cooldown time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Threshold, b.Cooldown = threshold, cooldown
}
