// Package lb is an HTTP load balancer: a reverse proxy that spreads requests
// over backends with a pluggable strategy, takes unhealthy backends out of
// rotation (active health checks plus a passive circuit breaker per backend),
// retries idempotent requests, reloads its config without dropping traffic,
// and exposes Prometheus metrics.
package lb

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AbdeldjalilChougui/loadbalancer/config"
)

// LoadBalancer is an http.Handler. Create it with New and update it with Apply.
type LoadBalancer struct {
	log       *slog.Logger
	transport http.RoundTripper
	now       func() time.Time

	applyMu sync.Mutex // serialises Apply calls

	mu       sync.Mutex
	cfg      *config.Config
	backends []*Backend
	strategy Strategy

	checkerCancel context.CancelFunc
	checkerDone   chan struct{}

	requests     atomic.Uint64
	noBackend    atomic.Uint64
	retries      atomic.Uint64
	reloads      atomic.Uint64
	reloadErrors atomic.Uint64
}

// Option customises a LoadBalancer.
type Option func(*LoadBalancer)

func WithLogger(l *slog.Logger) Option         { return func(lb *LoadBalancer) { lb.log = l } }
func WithTransport(t http.RoundTripper) Option { return func(lb *LoadBalancer) { lb.transport = t } }
func WithClock(now func() time.Time) Option    { return func(lb *LoadBalancer) { lb.now = now } }

// New builds a load balancer and starts health checking.
func New(cfg *config.Config, opts ...Option) (*LoadBalancer, error) {
	l := &LoadBalancer{
		log: slog.Default(),
		now: time.Now,
		transport: &http.Transport{
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	for _, o := range opts {
		o(l)
	}
	if err := l.Apply(cfg); err != nil {
		return nil, err
	}
	return l, nil
}

// Apply swaps in a new configuration. Backends whose URL is unchanged keep
// their health, breaker and connection state, so a reload is invisible to
// traffic. On error the old configuration stays in effect.
func (l *LoadBalancer) Apply(cfg *config.Config) error {
	l.applyMu.Lock()
	defer l.applyMu.Unlock()
	if err := cfg.Validate(); err != nil {
		return err
	}
	strat, err := NewStrategy(cfg.Strategy)
	if err != nil {
		return err
	}
	l.mu.Lock()
	old := map[string]*Backend{}
	for _, b := range l.backends {
		old[b.URL.String()] = b
	}
	var next []*Backend
	for _, bc := range cfg.Backends {
		u, err := url.Parse(bc.URL)
		if err != nil {
			l.mu.Unlock()
			return err
		}
		if b, ok := old[u.String()]; ok {
			b.weight.Store(int64(bc.Weight))
			b.Breaker.SetParams(cfg.CircuitBreaker.FailureThreshold, cfg.CircuitBreaker.Cooldown.D())
			next = append(next, b)
			continue
		}
		br := NewBreaker(cfg.CircuitBreaker.FailureThreshold, cfg.CircuitBreaker.Cooldown.D())
		br.Now = l.now
		next = append(next, newBackend(u, bc.Weight, br))
	}
	// Keep the strategy object (and its rotation state) if the name is the same.
	if l.strategy != nil && l.strategy.Name() == strat.Name() {
		strat = l.strategy
	}
	l.cfg, l.backends, l.strategy = cfg, next, strat
	l.mu.Unlock()

	l.restartChecker(cfg.HealthCheck, next)
	return nil
}

// Backends returns the current backend list.
func (l *LoadBalancer) Backends() []*Backend {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*Backend(nil), l.backends...)
}

// Close stops background health checks.
func (l *LoadBalancer) Close() {
	l.stopChecker()
}

// pick chooses a backend not in tried, reserving it with its breaker.
func (l *LoadBalancer) pick(tried map[*Backend]bool) *Backend {
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		cands := make([]*Backend, 0, len(l.backends))
		for _, b := range l.backends {
			if !tried[b] && b.available() && b.Weight() > 0 {
				cands = append(cands, b)
			}
		}
		if len(cands) == 0 {
			return nil
		}
		b := l.strategy.Pick(cands)
		if b.Breaker.Acquire() {
			return b
		}
		tried[b] = true // lost a half-open race; try someone else
	}
}

func (l *LoadBalancer) retryBudget() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cfg.Retries
}

type ctxKey struct{}

// attempt carries the transport error out of ReverseProxy's ErrorHandler.
type attempt struct{ err error }

func idempotent(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
		return r.Body == nil || r.Body == http.NoBody
	}
	return false
}

// ServeHTTP proxies one request, retrying idempotent ones on another backend
// when the connection to the first one fails.
func (l *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.requests.Add(1)
	attempts := 1
	if idempotent(r) {
		attempts += l.retryBudget()
	}
	tried := map[*Backend]bool{}
	var lastErr error
	for i := 0; i < attempts; i++ {
		b := l.pick(tried)
		if b == nil {
			break
		}
		tried[b] = true
		if i > 0 {
			l.retries.Add(1)
		}
		err := l.forward(w, r, b)
		if err == nil {
			return
		}
		lastErr = err
		if r.Context().Err() != nil {
			return // client went away; nobody to answer
		}
		l.log.Warn("backend failed", "backend", b.URL.String(), "err", err)
	}
	if lastErr != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	l.noBackend.Add(1)
	http.Error(w, "no backend available", http.StatusServiceUnavailable)
}

// forward sends r to b. A non-nil error means nothing was written to w.
func (l *LoadBalancer) forward(w http.ResponseWriter, r *http.Request, b *Backend) error {
	b.active.Add(1)
	defer b.active.Add(-1)

	st := &attempt{}
	sw := &statusWriter{ResponseWriter: w}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(b.URL)
			pr.SetXForwarded()
			pr.Out.Host = pr.In.Host // keep the client's Host header
		},
		Transport: l.transport,
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, err error) {
			st.err = err // do not write: the caller may retry elsewhere
		},
	}
	proxy.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), ctxKey{}, st)))

	if st.err != nil {
		if r.Context().Err() != nil {
			b.Breaker.Release() // the client cancelled; not the backend's fault
		} else {
			b.Breaker.Failure()
			b.record(0)
		}
		return st.err
	}
	if sw.code >= 500 {
		b.Breaker.Failure()
	} else {
		b.Breaker.Success()
	}
	b.record(sw.code)
	return nil
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(p []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach Flush on the real writer, which
// ReverseProxy needs for streaming responses.
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }
