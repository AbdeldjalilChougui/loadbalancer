# loadbalancer

[![Go Reference](https://pkg.go.dev/badge/github.com/AbdeldjalilChougui/loadbalancer.svg)](https://pkg.go.dev/github.com/AbdeldjalilChougui/loadbalancer)

An HTTP reverse-proxy load balancer in Go, using only the standard library and
fsnotify. It has three balancing strategies, active health checks, a passive
circuit breaker per backend, retries for idempotent requests, config hot
reload, and Prometheus metrics.

## What it teaches

- How `httputil.ReverseProxy` works, and how to put retry logic around it. The
  error handler records the failure instead of writing a 502, so the request
  can still go to another backend.
- Balancing algorithms: round robin, nginx's *smooth* weighted round robin
  (weights 5,1,1 give `a a b a c a a`, not `a a a a a b c`), and least
  connections with rotating tie-breaks.
- The two ways to detect a dead backend. Active checks probe `/healthz` on a
  timer. Passive checks (a circuit breaker) watch real traffic and work even
  without a health endpoint.
- Changing configuration without dropping traffic. Unchanged backends keep
  their state, an invalid config is rejected, and the watcher looks at the
  directory because editors replace files by renaming.

## Install

Install the binary with Go:

```sh
go install github.com/AbdeldjalilChougui/loadbalancer/cmd/loadbalancer@latest
```

## Quick start

```sh
cp lb.example.json lb.json      # edit the backend URLs
go run ./cmd/loadbalancer -config lb.json
curl localhost:8080/            # proxied
curl localhost:9090/metrics     # Prometheus metrics
curl localhost:9090/status      # JSON view of the pool
kill -HUP <pid>                 # reload (saving the file also reloads)
```

### Config

```jsonc
{
  "listen": ":8080",
  "admin": ":9090",
  "strategy": "weighted",              // round_robin | weighted | least_conn
  "retries": 2,                        // extra attempts for idempotent requests
  "backends": [{"url": "http://127.0.0.1:9001", "weight": 3}],
  "health_check": {"path": "/healthz", "interval": "5s", "timeout": "1s",
                   "healthy_threshold": 2, "unhealthy_threshold": 2},
  "circuit_breaker": {"failure_threshold": 5, "cooldown": "10s"}
}
```

Leave `health_check.path` empty to turn off active checks. Set
`failure_threshold` to 0 to turn off the breaker.

## How it works

```
request ─► pick(): candidates = healthy ∧ breaker ready ∧ not tried yet
              strategy.Pick(candidates) → breaker.Acquire()
         ─► forward(): ReverseProxy to the backend
              transport error → breaker.Failure(), retry if idempotent
              5xx             → breaker.Failure(), response is passed through
              otherwise       → breaker.Success()
```

- **Circuit breaker.** After `failure_threshold` consecutive failures it goes
  *closed → open*. Once `cooldown` has passed, exactly one trial request is
  let through (*half-open*). A success closes the breaker and a failure opens
  it again. `Ready()` (side-effect free) is used to filter candidates and
  `Acquire()` reserves the single half-open slot.
- **Health checks.** One goroutine probes every backend concurrently each
  interval. Thresholds stop one lost probe from making the pool flap. When a
  backend recovers, its breaker is reset too.
- **Retries.** Only for GET/HEAD/OPTIONS/TRACE/PUT/DELETE requests without a
  body, and only when the backend could not be reached, so nothing has been
  written to the client yet. A 5xx response is never retried.
- **Reload.** `Apply` validates the new config first. It then reuses `*Backend`
  values whose URL is unchanged, keeping their in-flight count, health and
  breaker, and restarts the health checker. SIGHUP and file changes
  (debounced) both trigger it.

## Tests

```sh
go test -race ./...
```

All tests run against `httptest` backends: strategy order and distribution,
least-conn with a blocked backend, breaker state transitions on a fake clock,
retry around a dead backend, health checks taking a backend out and putting it
back, reload keeping state, the fsnotify watcher, and the metrics output.

## Status and limitations

This is a learning project. Things a production LB has that this one doesn't:

- Only HTTP/1.1 and HTTP/2 to the client through `net/http`. There is no TLS
  termination config (put it behind something, or add `ListenAndServeTLS`), no
  WebSocket-specific handling beyond what `ReverseProxy` already does, and no
  TCP (L4) mode.
- No sticky sessions, no consistent hashing, no slow start for recovered
  backends, and no outlier detection beyond consecutive failures.
- Changing `listen` or `admin` needs a restart. The reload logs a warning.
- Metrics are written by hand in the Prometheus text format. There are no
  latency histograms.
- The breaker counts any 5xx from the backend as a failure. That includes a
  500 caused by one bad request.

## License

MIT. See [LICENSE](LICENSE).
