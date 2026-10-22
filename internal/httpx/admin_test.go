package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func get(t *testing.T, h http.Handler, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body map[string]any
	if strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("bad JSON from %s: %v", path, err)
		}
	}
	return rec, body
}

func TestAdminHealthAndReady(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_total", Help: "t"})
	reg.MustRegister(c)
	c.Inc()

	h := AdminHandler(reg, []Check{
		{Name: "staging", Checker: DirWritable(t.TempDir())},
		{Name: "postgres", Checker: CheckFunc(func(context.Context) error { return nil })},
	}, time.Second, discardLogger())

	if rec, body := get(t, h, "/healthz"); rec.Code != http.StatusOK || body["status"] != "ok" {
		t.Errorf("healthz = %d %v", rec.Code, body)
	}
	rec, body := get(t, h, "/readyz")
	if rec.Code != http.StatusOK || body["status"] != "ok" {
		t.Errorf("readyz = %d %v", rec.Code, body)
	}
	if rec, _ := get(t, h, "/metrics"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "test_total 1") {
		t.Errorf("metrics = %d %q", rec.Code, rec.Body.String())
	}
}

func TestReadyFailsWhenACheckFails(t *testing.T) {
	const pgErr = `failed to connect to host=db.internal user=upload database=upload: connection refused`
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	h := AdminHandler(prometheus.NewRegistry(), []Check{
		{Name: "staging", Checker: DirWritable(filepath.Join(t.TempDir(), "missing"))},
		{Name: "postgres", Checker: CheckFunc(func(context.Context) error { return errors.New(pgErr) })},
		{Name: "objstore", Checker: CheckFunc(func(context.Context) error { return nil })},
	}, time.Second, log)
	rec, body := get(t, h, "/readyz")
	if rec.Code != http.StatusServiceUnavailable || body["status"] != "unavailable" {
		t.Fatalf("readyz = %d %v", rec.Code, body)
	}
	checks := body["checks"].(map[string]any)
	want := map[string]any{"postgres": "fail", "staging": "fail", "objstore": "ok"}
	if !maps.Equal(checks, want) {
		t.Errorf("checks = %v, want %v", checks, want)
	}
	if raw := rec.Body.String(); strings.Contains(raw, "db.internal") || strings.Contains(raw, "not writable") {
		t.Errorf("readyz body leaks error details: %s", raw)
	}

	// One log record per failing check, carrying the details.
	failed := map[string]string{}
	for line := range strings.Lines(logs.String()) {
		var rec struct{ Msg, Check, Err string }
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		if rec.Msg != "readiness check failed" {
			continue
		}
		if _, dup := failed[rec.Check]; dup {
			t.Errorf("check %q logged twice", rec.Check)
		}
		failed[rec.Check] = rec.Err
	}
	if len(failed) != 2 || failed["postgres"] != pgErr || !strings.Contains(failed["staging"], "not writable") {
		t.Errorf("failure logs = %v", failed)
	}
}

func TestReadyCheckTimeout(t *testing.T) {
	h := AdminHandler(prometheus.NewRegistry(), []Check{
		{Name: "slow", Checker: CheckFunc(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })},
	}, 20*time.Millisecond, discardLogger())
	if rec, _ := get(t, h, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d, want 503", rec.Code)
	}
}

func TestAdminRejectsOtherMethods(t *testing.T) {
	h := AdminHandler(prometheus.NewRegistry(), nil, time.Second, discardLogger())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz = %d", rec.Code)
	}
}
