package tusapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/finalize"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// fakeRepo is an in-memory Repository.
type fakeRepo struct {
	mu   sync.Mutex
	rows map[string]db.Upload // key protocol/ref
}

func newFakeRepo() *fakeRepo { return &fakeRepo{rows: map[string]db.Upload{}} }

func key(protocol, ref string) string { return protocol + "/" + ref }

func (f *fakeRepo) InsertUploading(_ context.Context, protocol, ref string, m meta.Metadata) (db.Upload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := db.Upload{
		Protocol: protocol, Ref: ref, DeviceID: m.DeviceID, SHA256: m.SHA256, Size: m.Size,
		Filename: m.Filename, ContentType: m.ContentType, CapturedAt: m.CapturedAt, Priority: m.Priority,
		Status: db.StatusUploading, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	f.rows[key(protocol, ref)] = u
	return u, nil
}

func (f *fakeRepo) Get(ctx context.Context, protocol, ref string) (db.Upload, error) {
	if err := ctx.Err(); err != nil {
		return db.Upload{}, err // like pgx
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.rows[key(protocol, ref)]
	if !ok {
		return db.Upload{}, db.ErrNotFound
	}
	return u, nil
}

func (f *fakeRepo) FindCompletedBySHA(_ context.Context, sha string) (db.Upload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.rows {
		if u.SHA256 == sha && u.Status == db.StatusCompleted {
			return u, nil
		}
	}
	return db.Upload{}, db.ErrNotFound
}

func (f *fakeRepo) update(protocol, ref string, fn func(*db.Upload)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.rows[key(protocol, ref)]
	if !ok {
		return db.ErrNotFound
	}
	if u.Status != db.StatusUploading {
		return db.ErrNotUploading
	}
	fn(&u)
	u.UpdatedAt = time.Now()
	f.rows[key(protocol, ref)] = u
	return nil
}

func (f *fakeRepo) IncrResume(_ context.Context, protocol, ref string) error {
	return f.update(protocol, ref, func(u *db.Upload) { u.ResumeCount++ })
}

func (f *fakeRepo) Touch(_ context.Context, protocol, ref string) error {
	return f.update(protocol, ref, func(*db.Upload) {})
}

func (f *fakeRepo) SetStatus(_ context.Context, protocol, ref string, s db.Status) error {
	return f.update(protocol, ref, func(u *db.Upload) { u.Status = s })
}

func (f *fakeRepo) ListStale(_ context.Context, _ time.Duration) ([]db.Upload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []db.Upload
	for _, u := range f.rows {
		if u.Status == db.StatusUploading {
			out = append(out, u)
		}
	}
	return out, nil
}

func (f *fakeRepo) set(protocol, ref string, s db.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := f.rows[key(protocol, ref)]
	u.Status = s
	f.rows[key(protocol, ref)] = u
}

// fakeFinalizer verifies the sha256 like the real one, or fails with err.
type fakeFinalizer struct {
	repo  *fakeRepo
	mu    sync.Mutex
	fail  error // returned (once) instead of finalizing
	calls int
}

func (f *fakeFinalizer) Run(ctx context.Context, in finalize.Input) (string, error) {
	f.mu.Lock()
	f.calls++
	failErr := f.fail
	f.fail = nil
	f.mu.Unlock()
	if failErr != nil {
		return "", failErr
	}
	row, err := f.repo.Get(ctx, in.Protocol, in.Ref)
	if err != nil {
		return "", err
	}
	if row.Status == db.StatusCompleted {
		return "key", nil
	}
	rc, err := in.Open(ctx)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, err = io.Copy(h, rc)
	rc.Close()
	if err != nil {
		return "", err
	}
	if hex.EncodeToString(h.Sum(nil)) != row.SHA256 {
		f.repo.set(in.Protocol, in.Ref, db.StatusFailedChecksum)
		_ = in.Delete(ctx)
		return "", finalize.ErrChecksum
	}
	f.repo.set(in.Protocol, in.Ref, db.StatusCompleted)
	return "key", in.Delete(ctx)
}

func (f *fakeFinalizer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeVerifier accepts "Bearer dev:<device>".
type fakeVerifier struct{}

func (fakeVerifier) VerifyHeader(h string) (string, error) {
	tok, err := auth.BearerToken(h)
	if err != nil {
		return "", err
	}
	dev, ok := strings.CutPrefix(tok, "dev:")
	if !ok {
		return "", auth.ErrUnauthenticated
	}
	return dev, nil
}

type fixture struct {
	t       *testing.T
	srv     *httptest.Server
	repo    *fakeRepo
	fin     *fakeFinalizer
	metrics *obs.Metrics
	dir     string
	logs    *bytes.Buffer
	st      *Staging
	mux     http.Handler
}

const testTTL = 7 * 24 * time.Hour

// newFixture serves both tus endpoints on one file staging; opts may
// adjust the staging before the handlers are built.
func newFixture(t *testing.T, opts ...func(*Staging)) *fixture {
	t.Helper()
	dir := t.TempDir()
	st, err := NewFileStaging(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range opts {
		o(st)
	}
	repo := newFakeRepo()
	fin := &fakeFinalizer{repo: repo}
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(&lockedWriter{w: logs}, nil))
	mux := http.NewServeMux()
	for _, ep := range []struct{ base, proto string }{{BasePathREST, ProtocolREST}, {BasePathRESTHTTP3, ProtocolRESTHTTP3}} {
		h, err := New(Config{
			BasePath: ep.base, Protocol: ep.proto,
			Rules:         meta.Rules{MaxFileBytes: 1 << 20, ContentTypes: []string{"audio/flac"}},
			IncompleteTTL: testTTL, Staging: st, Repo: repo, Finalizer: fin,
			Verifier: fakeVerifier{}, Metrics: m, Log: log,
		})
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle(ep.base, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &fixture{t: t, srv: srv, repo: repo, fin: fin, metrics: m, dir: dir, logs: logs, st: st, mux: mux}
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func metadata(device, sha string) string {
	pairs := [][2]string{
		{"device_id", device}, {"sha256", sha}, {"filename", "a.flac"},
		{"content_type", "audio/flac"}, {"captured_at", "2026-10-21T05:30:00+07:00"}, {"priority", "2"},
	}
	var out []string
	for _, p := range pairs {
		out = append(out, p[0]+" "+base64.StdEncoding.EncodeToString([]byte(p[1])))
	}
	return strings.Join(out, ",")
}

func (f *fixture) do(method, path, device string, body []byte, hdr map[string]string) *http.Response {
	f.t.Helper()
	url := path
	if !strings.HasPrefix(path, "http") {
		url = f.srv.URL + path
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	req.Header.Set("Tus-Resumable", "1.0.0")
	if device != "" {
		req.Header.Set("Authorization", "Bearer dev:"+device)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := f.srv.Client().Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { res.Body.Close() })
	return res
}

func readBody(t *testing.T, res *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func (f *fixture) create(base, device string, data []byte) string {
	f.t.Helper()
	res := f.do(http.MethodPost, base, device, nil, map[string]string{
		"Upload-Length": strconv.Itoa(len(data)), "Upload-Metadata": metadata(device, shaHex(data)),
	})
	if res.StatusCode != http.StatusCreated {
		f.t.Fatalf("create: %d %s", res.StatusCode, readBody(f.t, res))
	}
	return res.Header.Get("Location")
}

func (f *fixture) patch(url, device string, off int, chunk []byte) *http.Response {
	return f.do(http.MethodPatch, url, device, chunk, map[string]string{
		"Upload-Offset": strconv.Itoa(off), "Content-Type": "application/offset+octet-stream",
	})
}

func refOf(url string) string { return url[strings.LastIndex(url, "/")+1:] }

func TestOptionsAdvertisesExtensions(t *testing.T) {
	f := newFixture(t)
	res := f.do(http.MethodOptions, BasePathREST, "dev-0001", nil, nil)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	if got := res.Header.Get("Tus-Extension"); got != Extensions {
		t.Errorf("Tus-Extension = %q", got)
	}
	if got := res.Header.Get("Tus-Max-Size"); got != strconv.Itoa(1<<20) {
		t.Errorf("Tus-Max-Size = %q", got)
	}
	if res.Header.Get("Tus-Version") != "1.0.0" {
		t.Errorf("Tus-Version = %q", res.Header.Get("Tus-Version"))
	}
}

func TestCreateAndUpload(t *testing.T) {
	f := newFixture(t)
	data := []byte("hello sensor world")
	res := f.do(http.MethodPost, BasePathREST, "dev-0001", nil, map[string]string{
		"Upload-Length": strconv.Itoa(len(data)), "Upload-Metadata": metadata("dev-0001", shaHex(data)),
	})
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d %s", res.StatusCode, readBody(t, res))
	}
	loc := res.Header.Get("Location")
	if !strings.HasPrefix(loc, f.srv.URL+BasePathREST) || !validRef(refOf(loc)) {
		t.Errorf("Location = %q", loc)
	}
	exp, err := http.ParseTime(res.Header.Get("Upload-Expires"))
	if err != nil || time.Until(exp) < testTTL-time.Minute {
		t.Errorf("Upload-Expires = %q (%v)", res.Header.Get("Upload-Expires"), err)
	}
	if got := testutil.ToFloat64(f.metrics.Started.WithLabelValues(ProtocolREST)); got != 1 {
		t.Errorf("started = %v", got)
	}

	res = f.patch(loc, "dev-0001", 0, data[:5])
	if res.StatusCode != http.StatusNoContent || res.Header.Get("Upload-Offset") != "5" || res.Header.Get("Upload-Expires") == "" {
		t.Fatalf("patch 1: %d %v", res.StatusCode, res.Header)
	}
	res = f.do(http.MethodHead, loc, "dev-0001", nil, nil)
	if res.StatusCode != http.StatusOK || res.Header.Get("Upload-Offset") != "5" || res.Header.Get("Upload-Expires") == "" {
		t.Fatalf("head: %d %v", res.StatusCode, res.Header)
	}
	row, _ := f.repo.Get(context.Background(), ProtocolREST, refOf(loc))
	if row.ResumeCount != 1 {
		t.Errorf("resume_count = %d", row.ResumeCount)
	}
	res = f.patch(loc, "dev-0001", 5, data[5:])
	if res.StatusCode != http.StatusNoContent || res.Header.Get("Upload-Offset") != strconv.Itoa(len(data)) {
		t.Fatalf("patch 2: %d %v", res.StatusCode, res.Header)
	}
	if res.Header.Get("Upload-Expires") != "" {
		t.Error("completed upload must not carry Upload-Expires")
	}
	row, _ = f.repo.Get(context.Background(), ProtocolREST, refOf(loc))
	if row.Status != db.StatusCompleted {
		t.Errorf("status = %s", row.Status)
	}
	if _, err := os.Stat(filepath.Join(f.dir, refOf(loc))); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("staging not deleted: %v", err)
	}
	if got := testutil.ToFloat64(f.metrics.BytesReceived.WithLabelValues(ProtocolREST)); got != float64(len(data)) {
		t.Errorf("bytes received = %v", got)
	}

	// Completed: HEAD shows the full offset, a repeated final PATCH gets 204.
	res = f.do(http.MethodHead, loc, "dev-0001", nil, nil)
	if res.StatusCode != http.StatusOK || res.Header.Get("Upload-Offset") != strconv.Itoa(len(data)) {
		t.Errorf("head after completion: %d %v", res.StatusCode, res.Header)
	}
	res = f.patch(loc, "dev-0001", len(data), nil)
	if res.StatusCode != http.StatusNoContent {
		t.Errorf("patch after completion: %d", res.StatusCode)
	}
	res = f.patch(loc, "dev-0001", 0, data)
	if res.StatusCode != http.StatusConflict {
		t.Errorf("patch at wrong offset after completion: %d", res.StatusCode)
	}
	// A new POST of the same file is a duplicate.
	res = f.do(http.MethodPost, BasePathREST, "dev-0001", nil, map[string]string{
		"Upload-Length": strconv.Itoa(len(data)), "Upload-Metadata": metadata("dev-0001", shaHex(data)),
	})
	if body := readBody(t, res); res.StatusCode != StatusAlreadyExists || body != "already exists" {
		t.Errorf("duplicate create: %d %q", res.StatusCode, body)
	}
	if !strings.Contains(f.logs.String(), `"offset_before":0,"offset_after":5`) {
		t.Errorf("request log missing offsets:\n%s", f.logs.String())
	}
	if strings.Contains(f.logs.String(), "dev:dev-0001") {
		t.Error("token logged")
	}
}

func TestCreateValidation(t *testing.T) {
	f := newFixture(t)
	data := []byte("x")
	tests := []struct {
		name     string
		device   string
		metadata string
		length   string
		status   int
		body     string
	}{
		{"no token", "", metadata("dev-0001", shaHex(data)), "1", http.StatusUnauthorized, "unauthenticated"},
		{"bad sha", "dev-0001", metadata("dev-0001", "abc"), "1", http.StatusBadRequest, "sha256"},
		{"malformed header", "dev-0001", "device_id !!!", "1", http.StatusBadRequest, "Upload-Metadata"},
		{"missing field", "dev-0001", "device_id " + base64.StdEncoding.EncodeToString([]byte("dev-0001")), "1", http.StatusBadRequest, "sha256"},
		{"device mismatch", "dev-0002", metadata("dev-0001", shaHex(data)), "1", http.StatusForbidden, ""},
		{"size zero", "dev-0001", metadata("dev-0001", shaHex(data)), "0", http.StatusBadRequest, "size"},
		{"too large", "dev-0001", metadata("dev-0001", shaHex(data)), strconv.Itoa(1<<20 + 1), http.StatusRequestEntityTooLarge, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := f.do(http.MethodPost, BasePathREST, tc.device, nil, map[string]string{
				"Upload-Length": tc.length, "Upload-Metadata": tc.metadata,
			})
			body := readBody(t, res)
			if res.StatusCode != tc.status || !strings.Contains(body, tc.body) {
				t.Errorf("got %d %q, want %d containing %q", res.StatusCode, body, tc.status, tc.body)
			}
			if strings.Contains(body, "/") && tc.status >= 500 {
				t.Errorf("internal detail leaked: %q", body)
			}
		})
	}
	if len(f.repo.rows) != 0 {
		t.Errorf("rows inserted for rejected creates: %d", len(f.repo.rows))
	}
}

func TestOwnershipAndState(t *testing.T) {
	f := newFixture(t)
	data := []byte("0123456789")
	loc := f.create(BasePathREST, "dev-0001", data)
	ref := refOf(loc)

	for _, m := range []string{http.MethodHead, http.MethodPatch, http.MethodDelete} {
		res := f.do(m, loc, "dev-0002", []byte("x"), map[string]string{"Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("cross-device %s: %d", m, res.StatusCode)
		}
	}
	// Other base path.
	res := f.do(http.MethodHead, BasePathRESTHTTP3+ref, "dev-0001", nil, nil)
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("other base path: %d", res.StatusCode)
	}
	// Garbage IDs never reach the store.
	for _, id := range []string{"..%2F..%2Fetc", "not-a-uuid", strings.ToUpper(ref)} {
		if res := f.do(http.MethodHead, BasePathREST+id, "dev-0001", nil, nil); res.StatusCode != http.StatusNotFound {
			t.Errorf("id %q: %d", id, res.StatusCode)
		}
	}
	// Method override cannot turn a POST into a DELETE.
	res = f.do(http.MethodPost, loc, "dev-0002", nil, map[string]string{"X-HTTP-Method-Override": "DELETE"})
	if res.StatusCode == http.StatusNoContent {
		t.Error("method override bypassed the ownership check")
	}

	for _, tc := range []struct {
		status db.Status
		want   int
	}{
		{db.StatusExpired, http.StatusGone},
		{db.StatusCancelled, http.StatusNotFound},
		{db.StatusFailedChecksum, StatusChecksumMismatch},
	} {
		f.repo.set(ProtocolREST, ref, tc.status)
		for _, m := range []string{http.MethodHead, http.MethodPatch} {
			res := f.patchOrHead(m, loc)
			if res.StatusCode != tc.want {
				t.Errorf("%s on %s: %d, want %d", m, tc.status, res.StatusCode, tc.want)
			}
		}
	}
}

func (f *fixture) patchOrHead(method, loc string) *http.Response {
	if method == http.MethodHead {
		return f.do(method, loc, "dev-0001", nil, nil)
	}
	return f.patch(loc, "dev-0001", 0, []byte("x"))
}

func TestDeleteCancels(t *testing.T) {
	f := newFixture(t)
	loc := f.create(BasePathREST, "dev-0001", []byte("0123456789"))
	res := f.do(http.MethodDelete, loc, "dev-0001", nil, nil)
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: %d %s", res.StatusCode, readBody(t, res))
	}
	row, _ := f.repo.Get(context.Background(), ProtocolREST, refOf(loc))
	if row.Status != db.StatusCancelled {
		t.Errorf("status = %s", row.Status)
	}
	if got := testutil.ToFloat64(f.metrics.Failed.WithLabelValues(ProtocolREST, obs.ReasonCancelled)); got != 1 {
		t.Errorf("failed{cancelled} = %v", got)
	}
	if res := f.do(http.MethodHead, loc, "dev-0001", nil, nil); res.StatusCode != http.StatusNotFound {
		t.Errorf("head after delete: %d", res.StatusCode)
	}
}

func TestChecksumMismatch(t *testing.T) {
	f := newFixture(t)
	data := []byte("0123456789")
	res := f.do(http.MethodPost, BasePathREST, "dev-0001", nil, map[string]string{
		"Upload-Length": "10", "Upload-Metadata": metadata("dev-0001", shaHex([]byte("other data"))),
	})
	loc := res.Header.Get("Location")
	res = f.patch(loc, "dev-0001", 0, data)
	if res.StatusCode != StatusChecksumMismatch {
		t.Fatalf("final patch: %d %s", res.StatusCode, readBody(t, res))
	}
	if _, err := os.Stat(filepath.Join(f.dir, refOf(loc))); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("staging not deleted: %v", err)
	}
	if res := f.do(http.MethodHead, loc, "dev-0001", nil, nil); res.StatusCode != StatusChecksumMismatch {
		t.Errorf("head after mismatch: %d", res.StatusCode)
	}
}

// TestShortCircuitGuard: finalization fails on the last PATCH; tusd
// would answer a retried PATCH with 204 without finalizing. The
// middleware must finalize first and never report success falsely.
func TestShortCircuitGuard(t *testing.T) {
	for _, method := range []string{http.MethodPatch, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			f := newFixture(t)
			data := []byte("0123456789")
			loc := f.create(BasePathREST, "dev-0001", data)
			f.fin.fail = errors.New("object store down")
			if res := f.patch(loc, "dev-0001", 0, data); res.StatusCode != http.StatusInternalServerError {
				t.Fatalf("final patch: %d", res.StatusCode)
			} else if body := readBody(t, res); strings.Contains(body, "object store") {
				t.Errorf("internal error leaked: %q", body)
			}
			// Still failing: the retry must not succeed.
			f.fin.fail = errors.New("object store down")
			if res := f.patchOrHeadFull(method, loc, len(data)); res.StatusCode != http.StatusInternalServerError {
				t.Fatalf("retry while failing: %d", res.StatusCode)
			}
			row, _ := f.repo.Get(context.Background(), ProtocolREST, refOf(loc))
			if row.Status != db.StatusUploading {
				t.Fatalf("status = %s", row.Status)
			}
			// Recovered: the retry finalizes.
			res := f.patchOrHeadFull(method, loc, len(data))
			want := http.StatusNoContent
			if method == http.MethodHead {
				want = http.StatusOK
			}
			if res.StatusCode != want || res.Header.Get("Upload-Offset") != strconv.Itoa(len(data)) {
				t.Fatalf("retry after recovery: %d %v", res.StatusCode, res.Header)
			}
			row, _ = f.repo.Get(context.Background(), ProtocolREST, refOf(loc))
			if row.Status != db.StatusCompleted {
				t.Errorf("status = %s", row.Status)
			}
			if got := f.fin.callCount(); got != 3 {
				t.Errorf("finalizer calls = %d", got)
			}
		})
	}
}

func (f *fixture) patchOrHeadFull(method, loc string, size int) *http.Response {
	if method == http.MethodHead {
		return f.do(method, loc, "dev-0001", nil, nil)
	}
	return f.patch(loc, "dev-0001", size, nil)
}

func TestRecover(t *testing.T) {
	f := newFixture(t)
	data := []byte("0123456789")
	loc := f.create(BasePathREST, "dev-0001", data)
	f.fin.fail = errors.New("crash")
	f.patch(loc, "dev-0001", 0, data)
	partial := f.create(BasePathREST, "dev-0001", []byte("abc"))
	f.patch(partial, "dev-0001", 0, []byte("a"))

	st, err := NewFileStaging(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := Recover(context.Background(), f.repo, f.fin, st, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	if row, _ := f.repo.Get(context.Background(), ProtocolREST, refOf(loc)); row.Status != db.StatusCompleted {
		t.Errorf("full upload status = %s", row.Status)
	}
	if row, _ := f.repo.Get(context.Background(), ProtocolREST, refOf(partial)); row.Status != db.StatusUploading {
		t.Errorf("partial upload status = %s", row.Status)
	}
}

func TestStaleLocksCleared(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.lock", "a.stop", "a", "a.info"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewFileStaging(dir); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if fmt.Sprint(names) != "[a a.info]" {
		t.Errorf("remaining files = %v", names)
	}
}

func TestResponseWriterUnwrapAndHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	var seen int
	w := &responseWriter{ResponseWriter: rec, beforeHeader: func(h http.Header, s int) {
		seen = s
		h.Set("X-Edited", "1")
	}}
	if w.Unwrap() != rec {
		t.Error("Unwrap must return the wrapped writer")
	}
	w.Header().Set("X-Edited", "0")
	_, _ = w.Write([]byte("body"))
	w.WriteHeader(http.StatusTeapot) // ignored: already written
	if seen != http.StatusOK || rec.Code != http.StatusOK || rec.Header().Get("X-Edited") != "1" {
		t.Errorf("seen=%d code=%d header=%q", seen, rec.Code, rec.Header().Get("X-Edited"))
	}
	if err := http.NewResponseController(w).SetReadDeadline(time.Now()); err != nil && !errors.Is(err, http.ErrNotSupported) {
		t.Errorf("ResponseController: %v", err)
	}
}

func TestTusdLoggerAdapter(t *testing.T) {
	var buf bytes.Buffer
	l := newTusdLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	l.Info("dropped", "x", 1)
	l.With("id", "abc").WithGroup("g").Warn("kept", "n", 2)
	out := buf.String()
	if strings.Contains(out, "dropped") {
		t.Errorf("info record forwarded: %s", out)
	}
	for _, want := range []string{`"msg":"kept"`, `"id":"abc"`, `"g":{"n":2}`, `"component":"tusd"`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in %s", want, out)
		}
	}
}

func TestRefFromID(t *testing.T) {
	if got := refFromID("0190e5b8-0000-7000-8000-000000000000+mpid"); got != "0190e5b8-0000-7000-8000-000000000000" {
		t.Errorf("got %q", got)
	}
}

func TestUseRegistry(t *testing.T) {
	r := newUseRegistry()
	leave, err := r.enter(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.tryExclusive("a"); ok {
		t.Fatal("exclusive while a request uses the upload")
	}
	leave()
	leave() // idempotent
	release, ok := r.tryExclusive("a")
	if !ok {
		t.Fatal("exclusive refused on an idle upload")
	}
	if _, ok := r.tryExclusive("a"); ok {
		t.Fatal("second exclusive granted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := r.enter(ctx, "a"); err == nil {
		t.Fatal("request entered while the cleaner holds the upload")
	}
	entered := make(chan struct{})
	go func() {
		l, err := r.enter(context.Background(), "a")
		if err == nil {
			l()
		}
		close(entered)
	}()
	release()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("waiting request not released")
	}
}
