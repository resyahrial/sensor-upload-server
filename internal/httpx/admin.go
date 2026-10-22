package httpx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Checker reports whether a dependency is reachable. *pgxpool.Pool and the
// object store satisfy it directly.
type Checker interface {
	Ping(ctx context.Context) error
}

// CheckFunc adapts a function to Checker.
type CheckFunc func(ctx context.Context) error

// Ping calls f.
func (f CheckFunc) Ping(ctx context.Context) error { return f(ctx) }

// Check is a named readiness dependency.
type Check struct {
	Name    string
	Checker Checker
}

// DirWritable returns a Checker that succeeds if a file can be created in dir.
func DirWritable(dir string) Checker {
	return CheckFunc(func(context.Context) error {
		f, err := os.CreateTemp(dir, ".readyz-*")
		if err != nil {
			return fmt.Errorf("staging dir not writable: %w", err)
		}
		name := f.Name()
		cerr := f.Close()
		if rerr := os.Remove(name); rerr != nil {
			return fmt.Errorf("staging dir cleanup: %w", rerr)
		}
		return cerr
	})
}

// Per-check results in the /readyz body.
const (
	checkOK   = "ok"
	checkFail = "fail"
)

// AdminHandler serves /metrics from g, /healthz (always 200 while the
// process runs) and /readyz (200 only if every check passes within
// checkTimeout, else 503). The /readyz body reports only "ok" or "fail" per
// check. The error behind a failure goes to log instead, because it can name
// hosts, database users, buckets or paths.
func AdminHandler(g prometheus.Gatherer, checks []Check, checkTimeout time.Duration, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), checkTimeout)
		defer cancel()
		results, ok := runChecks(ctx, checks, log)
		status, code := "ok", http.StatusOK
		if !ok {
			status, code = "unavailable", http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": results})
	})
	return mux
}

// runChecks runs checks concurrently and returns each one's result (checkOK
// or checkFail) and whether all passed. Each failure is logged once.
func runChecks(ctx context.Context, checks []Check, log *slog.Logger) (map[string]string, bool) {
	results := make(map[string]string, len(checks))
	ok := true
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, c := range checks {
		wg.Go(func() {
			res := checkOK
			if err := c.Checker.Ping(ctx); err != nil {
				res = checkFail
				log.WarnContext(ctx, "readiness check failed", "check", c.Name, "err", err)
			}
			mu.Lock()
			defer mu.Unlock()
			results[c.Name] = res
			if res != checkOK {
				ok = false
			}
		})
	}
	wg.Wait()
	return results, ok
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v) // client may have gone away; nothing to do
}
