package lb

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/AbdeldjalilChougui/loadbalancer/config"
)

// Active health checking: every Interval each backend gets a GET on Path.
// A 2xx/3xx answer within Timeout is a pass. HealthyThreshold consecutive
// passes bring a backend back; UnhealthyThreshold consecutive failures take
// it out. Thresholds stop one flaky probe from flapping the pool.

func (l *LoadBalancer) stopChecker() {
	l.mu.Lock()
	cancel, done := l.checkerCancel, l.checkerDone
	l.checkerCancel, l.checkerDone = nil, nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done // never run two checkers at once: they share streak counters
	}
}

func (l *LoadBalancer) restartChecker(hc config.HealthCheck, backends []*Backend) {
	l.stopChecker()
	if hc.Path == "" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l.mu.Lock()
	l.checkerCancel, l.checkerDone = cancel, done
	l.mu.Unlock()
	go func() {
		defer close(done)
		client := &http.Client{Transport: l.transport, Timeout: hc.Timeout.D()}
		t := time.NewTicker(hc.Interval.D())
		defer t.Stop()
		for {
			l.checkAll(ctx, client, hc, backends)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

func (l *LoadBalancer) checkAll(ctx context.Context, client *http.Client, hc config.HealthCheck, backends []*Backend) {
	var wg sync.WaitGroup
	for _, b := range backends {
		wg.Add(1)
		go func(b *Backend) {
			defer wg.Done()
			ok := probe(ctx, client, b.URL.JoinPath(hc.Path).String())
			if ctx.Err() != nil {
				return
			}
			l.observe(b, ok, hc)
		}(b)
	}
	wg.Wait()
}

func probe(ctx context.Context, client *http.Client, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

func (l *LoadBalancer) observe(b *Backend, ok bool, hc config.HealthCheck) {
	if ok {
		b.okStreak++
		b.failStreak = 0
		if !b.Healthy() && b.okStreak >= hc.HealthyThreshold {
			b.healthy.Store(true)
			b.Breaker.Success() // a fresh start for passive checks too
			l.log.Info("backend healthy", "backend", b.URL.String())
		}
		return
	}
	b.failStreak++
	b.okStreak = 0
	if b.Healthy() && b.failStreak >= hc.UnhealthyThreshold {
		b.healthy.Store(false)
		l.log.Warn("backend unhealthy", "backend", b.URL.String())
	}
}
