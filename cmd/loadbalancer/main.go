// Command loadbalancer runs the HTTP load balancer.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbdeldjalilChougui/loadbalancer/config"
	"github.com/AbdeldjalilChougui/loadbalancer/lb"
)

func main() {
	path := flag.String("config", "lb.json", "path to the JSON config")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg, err := config.Load(*path)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}
	balancer, err := lb.New(cfg, lb.WithLogger(log))
	if err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	defer balancer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reload := func() {
		next, err := config.Load(*path)
		if err == nil {
			err = balancer.Apply(next)
		}
		balancer.Reloaded(err)
		if err != nil {
			log.Error("reload failed; keeping the old config", "err", err)
			return
		}
		if next.Listen != cfg.Listen || next.Admin != cfg.Admin {
			log.Warn("listen/admin address changes need a restart")
		}
		log.Info("config reloaded", "backends", len(next.Backends), "strategy", next.Strategy)
	}
	if err := lb.WatchConfig(ctx, *path, reload); err != nil {
		log.Warn("config watch disabled", "err", err)
	}

	proxy := &http.Server{Addr: cfg.Listen, Handler: balancer, ReadHeaderTimeout: 10 * time.Second}
	admin := &http.Server{Addr: cfg.Admin, Handler: balancer.AdminHandler(), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 2)
	for _, s := range []*http.Server{proxy, admin} {
		go func(s *http.Server) {
			log.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}(s)
	}
	select {
	case <-ctx.Done():
	case err := <-errc:
		log.Error("server", "err", err)
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proxy.Shutdown(shutdown)
	admin.Shutdown(shutdown)
}
