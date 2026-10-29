package fileapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

type fakeLister struct {
	rows []db.Upload
	err  error
	got  db.ListFilter
}

func (f *fakeLister) ListCompletedPage(_ context.Context, lf db.ListFilter) ([]db.Upload, error) {
	f.got = lf
	if f.err != nil {
		return nil, f.err
	}
	rows := f.rows
	if lf.After != nil {
		for i, u := range rows {
			if u.ID == lf.After.ID {
				rows = rows[i+1:]
				break
			}
		}
	}
	return rows[:min(len(rows), lf.Limit)], nil
}

func listRows(n int) []db.Upload {
	done := time.Date(2026, 10, 21, 5, 30, 0, 0, time.FixedZone("WIB", 7*3600))
	rows := make([]db.Upload, n)
	for i := range rows {
		at := done.Add(-time.Duration(i) * time.Minute)
		rows[i] = db.Upload{
			ID: uuid.Must(uuid.NewV7()), DeviceID: "wih-001", SHA256: strings.Repeat("ab", 32),
			Filename: "a.jpg", ContentType: "image/jpeg", Size: 10, CapturedAt: at, CompletedAt: &at,
			StorageKey: "secret/key", Ref: "secret-ref", Status: db.StatusCompleted,
		}
	}
	return rows
}

func getList(t *testing.T, h http.Handler, query string) (*httptest.ResponseRecorder, ListResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathList+query, nil))
	var res ListResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("body %q: %v", rec.Body, err)
		}
	}
	return rec, res
}

func TestListHandler(t *testing.T) {
	repo := &fakeLister{rows: listRows(3)}
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h := NewListHandler(ListConfig{Repo: repo, Metrics: m})

	rec, res := getList(t, h, "?device_id=WIH-001&content_type=image/*&limit=2")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body)
	}
	if repo.got.DeviceID != "wih-001" || repo.got.ContentTypePrefix != "image/" || repo.got.ContentType != "" || repo.got.Limit != 3 {
		t.Fatalf("filter = %+v (device lowercased, prefix, limit+1)", repo.got)
	}
	if len(res.Files) != 2 || res.NextCursor == "" {
		t.Fatalf("page = %+v, want 2 files and a cursor", res)
	}
	f := res.Files[0]
	if f.DeviceID != "wih-001" || f.ContentType != "image/jpeg" || f.Size != 10 || f.CompletedAt != "2026-10-20T22:30:00Z" {
		t.Fatalf("file = %+v", f)
	}
	if strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("listing leaks storage key or ref: %s", rec.Body)
	}

	// The cursor points at the last row returned.
	if _, res2 := getList(t, h, "?limit=2&cursor="+res.NextCursor); res2.NextCursor != "" {
		t.Fatalf("second page has a cursor: %+v", res2)
	}
	if a := repo.got.After; a == nil || a.ID != repo.rows[1].ID || !a.CompletedAt.Equal(*repo.rows[1].CompletedAt) {
		t.Fatalf("After = %+v, want row 1", a)
	}

	// Defaults; exact content type; an empty listing is [] not null.
	repo.rows = nil
	rec, _ = getList(t, h, "?content_type=image/png")
	if repo.got.Limit != DefaultListLimit+1 || repo.got.ContentType != "image/png" || repo.got.DeviceID != "" || repo.got.After != nil {
		t.Fatalf("filter = %+v", repo.got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"files":[]}` {
		t.Fatalf("empty body = %s", body)
	}
	if got := testutil.ToFloat64(m.FileLists.WithLabelValues(obs.ListOK)); got != 3 {
		t.Fatalf("ok count = %v", got)
	}
}

func TestListHandlerErrors(t *testing.T) {
	repo := &fakeLister{}
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h := NewListHandler(ListConfig{Repo: repo, Metrics: m})
	for name, q := range map[string]string{
		"bad device":       "?device_id=bad_id",
		"limit zero":       "?limit=0",
		"limit too big":    "?limit=201",
		"limit not number": "?limit=ten",
		"cursor garbage":   "?cursor=!!!",
		"cursor no sep":    "?cursor=MTIz",
		"content type sp":  "?content_type=image%2F%20jpeg",
	} {
		if rec, _ := getList(t, h, q); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}
	repo.err = errors.New("db down")
	if rec, _ := getList(t, h, ""); rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "db down") {
		t.Fatalf("repo error: %d %s", rec.Code, rec.Body)
	}
	if got := testutil.ToFloat64(m.FileLists.WithLabelValues(obs.ListInvalid)); got != 7 {
		t.Fatalf("invalid count = %v", got)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := db.ListCursor{CompletedAt: time.UnixMicro(1761024600123456), ID: uuid.Must(uuid.NewV7())}
	got, err := decodeCursor(encodeCursor(c))
	if err != nil || !got.CompletedAt.Equal(c.CompletedAt) || got.ID != c.ID {
		t.Fatalf("round trip = %+v, %v; want %+v", got, err, c)
	}
}
