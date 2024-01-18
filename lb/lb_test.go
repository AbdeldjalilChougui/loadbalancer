package lb

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AbdeldjalilChougui/loadbalancer/config"
)

var quiet = WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

// backend is an httptest server that names itself in the response body and
// can be told to fail health checks or requests.
type backend struct {
	*httptest.Server
	name      string
	unhealthy atomic.Bool
	fail      atomic.Bool
	hits      atomic.Int64
	block     chan struct{} // if non-nil, requests wait on it
}

func startBackend(t *testing.T, name string) *backend {
	b := &backend{name: name}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			if b.unhealthy.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		b.hits.Add(1)
		if b.block != nil {
			<-b.block
		}
		if b.fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, b.name)
	}))
	t.Cleanup(b.Close)
	return b
}

func cfgFor(strategy string, bs ...*backend) *config.Config {
	raw := fmt.Sprintf(`{"strategy":%q,"backends":[`, strategy)
	for i, b := range bs {
		if i > 0 {
			raw += ","
		}
		raw += fmt.Sprintf(`{"url":%q}`, b.URL)
	}
	raw += `]}`
	c, err := config.Parse([]byte(raw))
	if err != nil {
		panic(err)
	}
	return c
}

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func TestRoundRobinEndToEnd(t *testing.T) {
	a, b, c := startBackend(t, "a"), startBackend(t, "b"), startBackend(t, "c")
	l, err := New(cfgFor("round_robin", a, b, c), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var got []string
	for i := 0; i < 6; i++ {
		code, body := get(t, l, "/x")
		if code != 200 {
			t.Fatalf("code %d", code)
		}
		got = append(got, body)
	}
	if strings.Join(got, "") != "abcabc" {
		t.Fatalf("order %v", got)
	}
}

func TestProxyPreservesPathQueryAndHeaders(t *testing.T) {
	var seen *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r
		w.Header().Set("X-Backend", "yes")
	}))
	defer srv.Close()
	c, _ := config.Parse([]byte(fmt.Sprintf(`{"backends":[{"url":%q}]}`, srv.URL+"/base")))
	l, _ := New(c, quiet)
	defer l.Close()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://front.example/p?q=1", nil)
	l.ServeHTTP(rec, req)
	if seen.URL.Path != "/base/p" || seen.URL.RawQuery != "q=1" {
		t.Fatalf("upstream saw %s?%s", seen.URL.Path, seen.URL.RawQuery)
	}
	if seen.Header.Get("X-Forwarded-Host") != "front.example" || seen.Host != "front.example" {
		t.Fatalf("headers %v host %s", seen.Header, seen.Host)
	}
	if rec.Header().Get("X-Backend") != "yes" {
		t.Fatal("response header lost")
	}
}

func mkBackends(weights ...int) []*Backend {
	var out []*Backend
	for i, w := range weights {
		u, _ := url.Parse(fmt.Sprintf("http://b%d", i))
		out = append(out, newBackend(u, w, NewBreaker(0, 0)))
	}
	return out
}

func TestWeightedSmooth(t *testing.T) {
	bs := mkBackends(5, 1, 1)
	s := &Weighted{}
	var seq []string
	for i := 0; i < 7; i++ {
		seq = append(seq, s.Pick(bs).URL.Host)
	}
	// nginx's documented sequence for 5,1,1.
	want := "b0 b0 b1 b0 b2 b0 b0"
	if strings.Join(seq, " ") != want {
		t.Fatalf("got %v want %s", seq, want)
	}
	counts := map[string]int{}
	for i := 0; i < 700; i++ {
		counts[s.Pick(bs).URL.Host]++
	}
	if counts["b0"] != 500 || counts["b1"] != 100 || counts["b2"] != 100 {
		t.Fatalf("distribution %v", counts)
	}
}

func TestLeastConn(t *testing.T) {
	bs := mkBackends(1, 1, 1)
	bs[0].active.Store(5)
	bs[1].active.Store(1)
	bs[2].active.Store(3)
	s := &LeastConn{}
	for i := 0; i < 5; i++ {
		if got := s.Pick(bs); got != bs[1] {
			t.Fatalf("picked %s", got.URL)
		}
	}
	// Ties rotate.
	for _, b := range bs {
		b.active.Store(0)
	}
	seen := map[*Backend]bool{}
	for i := 0; i < 3; i++ {
		seen[s.Pick(bs)] = true
	}
	if len(seen) != 3 {
		t.Fatalf("ties not rotated: %d distinct", len(seen))
	}
}

func TestLeastConnEndToEnd(t *testing.T) {
	slow, fast := startBackend(t, "slow"), startBackend(t, "fast")
	slow.block = make(chan struct{})
	l, _ := New(cfgFor("least_conn", slow, fast), quiet)
	defer l.Close()
	var wg sync.WaitGroup
	// The first request goes to one of them; force a long-running one on slow.
	for slow.hits.Load() == 0 {
		wg.Add(1)
		go func() { defer wg.Done(); get(t, l, "/") }()
		time.Sleep(20 * time.Millisecond)
	}
	// While slow is busy, everything else must go to fast.
	before := fast.hits.Load()
	for i := 0; i < 5; i++ {
		if _, body := get(t, l, "/"); body != "fast" {
			t.Fatalf("least_conn sent a request to the busy backend")
		}
	}
	if fast.hits.Load()-before != 5 {
		t.Fatal("fast did not receive the requests")
	}
	close(slow.block)
	wg.Wait()
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestBreakerStates(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := NewBreaker(3, 10*time.Second)
	b.Now = clk.Now
	for i := 0; i < 2; i++ {
		b.Failure()
	}
	if b.State() != Closed || !b.Ready() {
		t.Fatal("opened too early")
	}
	b.Failure()
	if b.State() != Open || b.Ready() || b.Acquire() {
		t.Fatal("should be open")
	}
	clk.Add(10 * time.Second)
	if !b.Ready() || !b.Acquire() {
		t.Fatal("cooldown passed: one trial should be allowed")
	}
	if b.State() != HalfOpen || b.Acquire() {
		t.Fatal("only one half-open trial at a time")
	}
	b.Failure() // trial failed → open again
	if b.State() != Open {
		t.Fatal("failed trial should re-open")
	}
	clk.Add(10 * time.Second)
	b.Acquire()
	b.Success()
	if b.State() != Closed || !b.Acquire() || !b.Acquire() {
		t.Fatal("successful trial should close")
	}
}

func TestPassiveCircuitBreakingAndRetry(t *testing.T) {
	good := startBackend(t, "good")
	bad := startBackend(t, "bad")
	bad.Close() // connection refused from now on

	clk := &fakeClock{t: time.Unix(0, 0)}
	c := cfgFor("round_robin", bad, good)
	c.Retries = 1
	c.CircuitBreaker.FailureThreshold = 2
	c.CircuitBreaker.Cooldown = config.Duration(30 * time.Second)
	l, _ := New(c, quiet, WithClock(clk.Now))
	defer l.Close()

	// Every GET succeeds: a connection failure on "bad" is retried on "good".
	for i := 0; i < 6; i++ {
		if code, body := get(t, l, "/"); code != 200 || body != "good" {
			t.Fatalf("request %d: %d %q", i, code, body)
		}
	}
	var badB *Backend
	for _, b := range l.Backends() {
		if strings.Contains(bad.URL, b.URL.Host) {
			badB = b
		}
	}
	if badB.Breaker.State() != Open {
		t.Fatalf("breaker %v, want open", badB.Breaker.State())
	}
	// POST is not retried, but with the breaker open it never hits "bad".
	rec := httptest.NewRecorder()
	l.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x")))
	if rec.Code != 200 {
		t.Fatalf("post while breaker open: %d", rec.Code)
	}

	var buf strings.Builder
	l.WriteMetrics(&buf)
	if !strings.Contains(buf.String(), `code="error"`) || !strings.Contains(buf.String(), "lb_retries_total 2") {
		t.Fatalf("metrics missing failures/retries:\n%s", buf.String())
	}
}

func TestPostNotRetried(t *testing.T) {
	bad := startBackend(t, "bad")
	bad.Close()
	c := cfgFor("round_robin", bad)
	c.Retries = 3
	l, _ := New(c, quiet)
	defer l.Close()
	rec := httptest.NewRecorder()
	l.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x")))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code %d", rec.Code)
	}
}

func Test5xxOpensBreaker(t *testing.T) {
	a := startBackend(t, "a")
	a.fail.Store(true)
	c := cfgFor("round_robin", a)
	c.CircuitBreaker.FailureThreshold = 3
	l, _ := New(c, quiet)
	defer l.Close()
	for i := 0; i < 3; i++ {
		if code, _ := get(t, l, "/"); code != 500 {
			t.Fatalf("code %d", code)
		}
	}
	if code, _ := get(t, l, "/"); code != http.StatusServiceUnavailable {
		t.Fatalf("breaker open: want 503, got %d", code)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestActiveHealthChecks(t *testing.T) {
	a, b := startBackend(t, "a"), startBackend(t, "b")
	c := cfgFor("round_robin", a, b)
	c.HealthCheck = config.HealthCheck{Path: "/healthz", Interval: config.Duration(10 * time.Millisecond),
		Timeout: config.Duration(time.Second), HealthyThreshold: 2, UnhealthyThreshold: 2}
	l, _ := New(c, quiet)
	defer l.Close()
	bOf := func(name *backend) *Backend {
		for _, x := range l.Backends() {
			if strings.Contains(name.URL, x.URL.Host) {
				return x
			}
		}
		return nil
	}
	a.unhealthy.Store(true)
	waitFor(t, "a to be marked unhealthy", func() bool { return !bOf(a).Healthy() })
	for i := 0; i < 5; i++ {
		if _, body := get(t, l, "/"); body != "b" {
			t.Fatalf("traffic sent to unhealthy backend")
		}
	}
	a.unhealthy.Store(false)
	waitFor(t, "a to recover", func() bool { return bOf(a).Healthy() })

	// All down → 503.
	a.unhealthy.Store(true)
	b.unhealthy.Store(true)
	waitFor(t, "all unhealthy", func() bool { return !bOf(a).Healthy() && !bOf(b).Healthy() })
	if code, _ := get(t, l, "/"); code != http.StatusServiceUnavailable {
		t.Fatalf("code %d", code)
	}
}

func TestApplyKeepsState(t *testing.T) {
	a, b, c := startBackend(t, "a"), startBackend(t, "b"), startBackend(t, "c")
	l, _ := New(cfgFor("round_robin", a, b), quiet)
	defer l.Close()
	before := l.Backends()[0]
	if err := l.Apply(cfgFor("weighted", a, c)); err != nil {
		t.Fatal(err)
	}
	after := l.Backends()
	if after[0] != before {
		t.Fatal("unchanged backend lost its state")
	}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		_, body := get(t, l, "/")
		seen[body] = true
	}
	if !seen["a"] || !seen["c"] || seen["b"] {
		t.Fatalf("after reload got %v", seen)
	}
	if err := l.Apply(&config.Config{Strategy: "nope"}); err == nil {
		t.Fatal("invalid config accepted")
	}
	if len(l.Backends()) != 2 {
		t.Fatal("failed apply changed the pool")
	}
}

func TestWatchConfigReloads(t *testing.T) {
	a, b := startBackend(t, "a"), startBackend(t, "b")
	dir := t.TempDir()
	path := filepath.Join(dir, "lb.json")
	write := func(bs ...*backend) {
		var parts []string
		for _, x := range bs {
			parts = append(parts, fmt.Sprintf(`{"url":%q}`, x.URL))
		}
		// Write to a temp file and rename, as editors do.
		tmp := path + ".tmp"
		os.WriteFile(tmp, []byte(`{"backends":[`+strings.Join(parts, ",")+`]}`), 0o644)
		os.Rename(tmp, path)
	}
	write(a)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	l, _ := New(cfg, quiet)
	defer l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reloads atomic.Int32
	err = WatchConfig(ctx, path, func() {
		if c, err := config.Load(path); err == nil && l.Apply(c) == nil {
			reloads.Add(1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	write(b)
	waitFor(t, "reload", func() bool { return reloads.Load() > 0 })
	if _, body := get(t, l, "/"); body != "b" {
		t.Fatalf("after reload got %q", body)
	}
}

func TestAdminHandler(t *testing.T) {
	a := startBackend(t, "a")
	l, _ := New(cfgFor("round_robin", a), quiet)
	defer l.Close()
	get(t, l, "/")
	code, body := get(t, l.AdminHandler(), "/metrics")
	if code != 200 {
		t.Fatal(code)
	}
	for _, want := range []string{
		"lb_requests_total 1",
		fmt.Sprintf(`lb_backend_requests_total{backend=%q,code="200"} 1`, a.URL),
		fmt.Sprintf(`lb_backend_up{backend=%q} 1`, a.URL),
		"# TYPE lb_backend_breaker_state gauge",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
	if code, body := get(t, l.AdminHandler(), "/status"); code != 200 || !strings.Contains(body, `"breaker":"closed"`) {
		t.Fatalf("status %d %s", code, body)
	}
}
