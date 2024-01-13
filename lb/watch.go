package lb

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WatchConfig calls reload whenever the config file changes on disk or the
// process receives SIGHUP. It watches the parent directory, not the file:
// editors and config-management tools usually replace files by renaming a
// new one over the old, which would silently end a watch on the file itself.
// Bursts of events are debounced into one reload.
func WatchConfig(ctx context.Context, path string, reload func()) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		w.Close()
		return err
	}
	if err := w.Add(filepath.Dir(abs)); err != nil {
		w.Close()
		return err
	}
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)

	go func() {
		defer w.Close()
		defer signal.Stop(hup)
		const debounce = 100 * time.Millisecond
		var timer *time.Timer
		var fire <-chan time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				reload()
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				if filepath.Clean(ev.Name) != abs || ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
					continue
				}
				if timer == nil {
					timer = time.NewTimer(debounce)
				} else {
					timer.Reset(debounce)
				}
				fire = timer.C
			case <-fire:
				fire = nil
				reload()
			case _, ok := <-w.Errors:
				if !ok {
					return
				}
			}
		}
	}()
	return nil
}
