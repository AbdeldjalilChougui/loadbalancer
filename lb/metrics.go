package lb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// AdminHandler serves /metrics (Prometheus text format) and /status (JSON).
func (l *LoadBalancer) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		l.WriteMetrics(w)
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(l.Status())
	})
	return mux
}

// BackendStatus is one row of /status.
type BackendStatus struct {
	URL      string `json:"url"`
	Weight   int    `json:"weight"`
	Healthy  bool   `json:"healthy"`
	Breaker  string `json:"breaker"`
	Active   int64  `json:"active"`
	Requests uint64 `json:"requests"`
}

func (l *LoadBalancer) Status() map[string]any {
	l.mu.Lock()
	strategy := l.strategy.Name()
	l.mu.Unlock()
	var rows []BackendStatus
	for _, b := range l.Backends() {
		var total uint64
		for _, n := range b.snapshotRequests() {
			total += n
		}
		rows = append(rows, BackendStatus{
			URL: b.URL.String(), Weight: b.Weight(), Healthy: b.Healthy(),
			Breaker: b.Breaker.State().String(), Active: b.Active(), Requests: total,
		})
	}
	return map[string]any{"strategy": strategy, "backends": rows}
}

// Reloaded records the outcome of a config reload attempt for metrics.
func (l *LoadBalancer) Reloaded(err error) {
	if err != nil {
		l.reloadErrors.Add(1)
	} else {
		l.reloads.Add(1)
	}
}

func escapeLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

// WriteMetrics writes all metrics in the Prometheus text exposition format.
func (l *LoadBalancer) WriteMetrics(w io.Writer) {
	metric := func(name, typ, help string) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	metric("lb_requests_total", "counter", "Requests received by the load balancer.")
	fmt.Fprintf(w, "lb_requests_total %d\n", l.requests.Load())
	metric("lb_no_backend_total", "counter", "Requests answered 503 because no backend was available.")
	fmt.Fprintf(w, "lb_no_backend_total %d\n", l.noBackend.Load())
	metric("lb_retries_total", "counter", "Retries of idempotent requests on another backend.")
	fmt.Fprintf(w, "lb_retries_total %d\n", l.retries.Load())
	metric("lb_config_reloads_total", "counter", "Successful config reloads.")
	fmt.Fprintf(w, "lb_config_reloads_total %d\n", l.reloads.Load())
	metric("lb_config_reload_errors_total", "counter", "Failed config reloads.")
	fmt.Fprintf(w, "lb_config_reload_errors_total %d\n", l.reloadErrors.Load())

	backends := l.Backends()
	metric("lb_backend_requests_total", "counter", "Proxied requests by backend and status code (code=\"error\" is a transport failure).")
	for _, b := range backends {
		reqs := b.snapshotRequests()
		codes := make([]int, 0, len(reqs))
		for c := range reqs {
			codes = append(codes, c)
		}
		sort.Ints(codes)
		for _, c := range codes {
			code := strconv.Itoa(c)
			if c == 0 {
				code = "error"
			}
			fmt.Fprintf(w, "lb_backend_requests_total{backend=\"%s\",code=\"%s\"} %d\n", escapeLabel(b.String()), code, reqs[c])
		}
	}
	gauge := func(name, help string, val func(*Backend) int64) {
		metric(name, "gauge", help)
		for _, b := range backends {
			fmt.Fprintf(w, "%s{backend=\"%s\"} %d\n", name, escapeLabel(b.String()), val(b))
		}
	}
	gauge("lb_backend_up", "1 if the backend passes active health checks.", func(b *Backend) int64 {
		if b.Healthy() {
			return 1
		}
		return 0
	})
	gauge("lb_backend_active_requests", "In-flight requests.", (*Backend).Active)
	gauge("lb_backend_weight", "Configured weight.", func(b *Backend) int64 { return int64(b.Weight()) })
	gauge("lb_backend_breaker_state", "Circuit breaker: 0 closed, 1 open, 2 half-open.", func(b *Backend) int64 {
		return int64(b.Breaker.State())
	})
}
