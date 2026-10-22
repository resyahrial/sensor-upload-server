package fileapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

type fakeRepo struct {
	rows  map[string]db.Upload
	err   error
	calls int
}

func (f *fakeRepo) FindCompletedBySHA(_ context.Context, sha string) (db.Upload, error) {
	f.calls++
	if f.err != nil {
		return db.Upload{}, f.err
	}
	u, ok := f.rows[sha]
	if !ok {
		return db.Upload{}, fmt.Errorf("find completed upload: %w", db.ErrNotFound)
	}
	return u, nil
}

var (
	shaFound   = strings.Repeat("ab", 32)
	shaMissing = strings.Repeat("0f", 32)
)

func TestHandler(t *testing.T) {
	done := time.Date(2026, 10, 21, 5, 30, 0, 0, time.FixedZone("WIB", 7*3600))
	repo := &fakeRepo{rows: map[string]db.Upload{shaFound: {
		SHA256: shaFound, Size: 1234, Status: db.StatusCompleted, CompletedAt: &done,
		DeviceID: "secret-device", StorageKey: "secret/key", Filename: "secret.flac",
	}}}
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(Config{Repo: repo, Metrics: m})

	tests := []struct {
		name, method, path string
		code               int
		want               map[string]any
		result             string
	}{
		{"found", http.MethodGet, Prefix + shaFound, 200,
			map[string]any{"sha256": shaFound, "status": "completed", "size": 1234.0, "completed_at": "2026-10-20T22:30:00Z"}, obs.LookupFound},
		{"head found", http.MethodHead, Prefix + shaFound, 200, nil, obs.LookupFound},
		{"not found", http.MethodGet, Prefix + shaMissing, 404,
			map[string]any{"sha256": shaMissing, "status": "not_found"}, obs.LookupNotFound},
		{"uppercase", http.MethodGet, Prefix + strings.ToUpper(shaFound), 400, map[string]any{"error": "sha256 must be 64 lowercase hex characters"}, obs.LookupInvalid},
		{"short", http.MethodGet, Prefix + "abc", 400, nil, obs.LookupInvalid},
		{"long", http.MethodGet, Prefix + shaFound + "0", 400, nil, obs.LookupInvalid},
		{"non hex", http.MethodGet, Prefix + strings.Repeat("zz", 32), 400, nil, obs.LookupInvalid},
		{"post", http.MethodPost, Prefix + shaFound, 405, nil, ""},
		{"subpath", http.MethodGet, Prefix + shaFound + "/x", 404, nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			if rec.Code != tc.code {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.code, rec.Body)
			}
			if tc.result == "" {
				return
			}
			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q", got)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q", got)
			}
			if tc.want == nil {
				return
			}
			var got map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("body = %v, want %v", got, tc.want)
			}
		})
	}
	for result, want := range map[string]float64{obs.LookupFound: 2, obs.LookupNotFound: 1, obs.LookupInvalid: 4} {
		if got := testutil.ToFloat64(m.FileLookups.WithLabelValues(result)); got != want {
			t.Errorf("file_lookups_total{result=%q} = %v, want %v", result, got, want)
		}
	}
	if repo.calls != 3 {
		t.Errorf("repo calls = %d, want 3 (invalid input must not reach the DB)", repo.calls)
	}
}

func TestHandlerDBError(t *testing.T) {
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(Config{Repo: &fakeRepo{err: errors.New("dial tcp 10.0.0.5:5432: refused")}, Metrics: m})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, Prefix+shaFound, nil))
	if rec.Code != 500 {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Errorf("body leaks the cause: %s", rec.Body)
	}
	if got := testutil.ToFloat64(m.FileLookups.WithLabelValues(obs.LookupError)); got != 1 {
		t.Errorf("error lookups = %v", got)
	}
}
