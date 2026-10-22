package tusapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tus/tusd/v2/pkg/handler"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// staleStore makes the next GetUpload, once armed, report one byte less
// than is staged: the view the middleware gets when it reads the upload
// info just before a concurrent final PATCH writes its last bytes.
type staleStore struct {
	handler.DataStore
	armed atomic.Bool
}

func (s *staleStore) GetUpload(ctx context.Context, id string) (handler.Upload, error) {
	up, err := s.DataStore.GetUpload(ctx, id)
	if err != nil || !s.armed.CompareAndSwap(true, false) {
		return up, err
	}
	return staleUpload{up}, nil
}

type staleUpload struct{ handler.Upload }

func (u staleUpload) GetInfo(ctx context.Context) (handler.FileInfo, error) {
	info, err := u.Upload.GetInfo(ctx)
	if err == nil && info.Offset > 0 {
		info.Offset--
	}
	return info, err
}

// TestStaleInfoGuard: the middleware's offset check ran on stale info, so
// tusd itself answers a fully staged upload whose row is not completed
// (HEAD 200 / PATCH 204 with Upload-Offset == Upload-Length). The
// middleware must finalize instead of passing that answer on.
func TestStaleInfoGuard(t *testing.T) {
	for _, method := range []string{http.MethodHead, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			stale := &staleStore{}
			f := newFixture(t, func(s *Staging) {
				stale.DataStore = s.core
				s.core = stale
			})
			data := []byte("0123456789")
			loc := f.create(BasePathREST, "dev-0001", data)
			f.fin.fail = errors.New("object store down")
			if res := f.patch(loc, "dev-0001", 0, data); res.StatusCode != http.StatusInternalServerError {
				t.Fatalf("final patch: %d", res.StatusCode)
			}

			f.fin.fail = errors.New("object store down")
			stale.armed.Store(true)
			res := f.patchOrHeadFull(method, loc, len(data))
			if res.StatusCode != http.StatusInternalServerError {
				t.Fatalf("stale %s while finalize fails: %d %v, want 500", method, res.StatusCode, res.Header)
			}
			if row, _ := f.repo.Get(context.Background(), ProtocolREST, refOf(loc)); row.Status != db.StatusUploading {
				t.Fatalf("status = %s", row.Status)
			}

			stale.armed.Store(true)
			res = f.patchOrHeadFull(method, loc, len(data))
			want := map[string]int{http.MethodHead: http.StatusOK, http.MethodPatch: http.StatusNoContent}[method]
			if res.StatusCode != want || res.Header.Get("Upload-Offset") != strconv.Itoa(len(data)) {
				t.Fatalf("stale %s after recovery: %d %v", method, res.StatusCode, res.Header)
			}
			if res.Header.Get("Upload-Expires") != "" {
				t.Error("completed upload must not carry Upload-Expires")
			}
			if row, _ := f.repo.Get(context.Background(), ProtocolREST, refOf(loc)); row.Status != db.StatusCompleted {
				t.Errorf("status = %s", row.Status)
			}
			if got := f.fin.callCount(); got != 3 {
				t.Errorf("finalizer calls = %d", got)
			}
		})
	}
}

// TestStoreErrorBodyIsGeneric: tusd answers a store error that is not a
// handler.Error with 500 and err.Error() as the body (here the staging
// path). The client must get a generic body; the log keeps the cause.
func TestStoreErrorBodyIsGeneric(t *testing.T) {
	f := newFixture(t)
	if err := os.Chmod(f.dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(f.dir, 0o700) })
	data := []byte("0123456789")
	res := f.do(http.MethodPost, BasePathREST, "dev-0001", nil, map[string]string{
		"Upload-Length": strconv.Itoa(len(data)), "Upload-Metadata": metadata("dev-0001", shaHex(data)),
	})
	body := readBody(t, res)
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("create on unwritable staging: %d %q", res.StatusCode, body)
	}
	if body != "internal error\n" || res.Header.Get("Content-Length") != strconv.Itoa(len(body)) {
		t.Errorf("body %q (Content-Length %q), want generic", body, res.Header.Get("Content-Length"))
	}
	if !strings.Contains(f.logs.String(), "permission denied") {
		t.Errorf("request log lost the cause:\n%s", f.logs.String())
	}
}

func TestResponseWriterGeneric5xx(t *testing.T) {
	const interrupted = "ERR_UPLOAD_INTERRUPTED: upload has been interrupted by another request for this upload resource\n"
	for _, tc := range []struct {
		method, body string
		status       int
		want         string
	}{
		{http.MethodPatch, "ERR_INTERNAL_SERVER_ERROR: open /data/staging/tus/x: no space left on device\n", 500, "internal error\n"},
		{http.MethodPatch, "ERR_SERVER_SHUTDOWN: request has been interrupted because the server is shutting down\n", 503, "server shutting down\n"},
		{http.MethodPatch, "internal error\n", 500, "internal error\n"},
		{http.MethodHead, "", 500, ""},
		{http.MethodPatch, interrupted, 400, interrupted},
	} {
		rec := httptest.NewRecorder()
		w := &responseWriter{ResponseWriter: rec, head: tc.method == http.MethodHead}
		w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)))
		w.WriteHeader(tc.status)
		_, _ = io.WriteString(w, tc.body)
		if got := rec.Body.String(); got != tc.want {
			t.Errorf("%s %d: body %q, want %q", tc.method, tc.status, got, tc.want)
		}
		if tc.method != http.MethodHead && rec.Header().Get("Content-Length") != strconv.Itoa(len(tc.want)) {
			t.Errorf("%s %d: Content-Length %q", tc.method, tc.status, rec.Header().Get("Content-Length"))
		}
		if !strings.HasPrefix(tc.body, string(w.errBody)) || len(w.errBody) != min(len(tc.body), maxErrBody) {
			t.Errorf("%s %d: logged body %q", tc.method, tc.status, w.errBody)
		}
	}
}

// TestCreateMetadataSizeCap: an oversized Upload-Metadata header is a 400
// naming the header and creates nothing; unknown keys are accepted.
func TestCreateMetadataSizeCap(t *testing.T) {
	f := newFixture(t)
	data := []byte("0123456789")
	md := metadata("dev-0001", shaHex(data))
	big := md + ",junk " + strings.Repeat("A", 4<<10)
	res := f.do(http.MethodPost, BasePathREST, "dev-0001", nil, map[string]string{
		"Upload-Length": strconv.Itoa(len(data)), "Upload-Metadata": big,
	})
	if body := readBody(t, res); res.StatusCode != http.StatusBadRequest || !strings.Contains(body, "Upload-Metadata") {
		t.Errorf("oversized metadata: %d %q, want 400 naming Upload-Metadata", res.StatusCode, body)
	}
	if n := len(f.repo.rows); n != 0 {
		t.Errorf("rows inserted: %d", n)
	}
	if entries, _ := os.ReadDir(f.dir); len(entries) != 0 {
		t.Errorf("staged files: %d", len(entries))
	}
	res = f.do(http.MethodPost, BasePathREST, "dev-0001", nil, map[string]string{
		"Upload-Length": strconv.Itoa(len(data)), "Upload-Metadata": md + ",filetype YXVkaW8vZmxhYw==,name YS5mbGFj",
	})
	if res.StatusCode != http.StatusCreated {
		t.Errorf("create with unknown keys: %d %q", res.StatusCode, readBody(t, res))
	}
}

// TestGuardLockTimeout:the guard cannot get the upload lock in time
// (§7 concurrent access) → 423, not 500.
func TestGuardLockTimeout(t *testing.T) {
	defer func(d time.Duration) { lockTimeout = d }(lockTimeout)
	lockTimeout = 300 * time.Millisecond
	for _, method := range []string{http.MethodHead, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			f := newFixture(t)
			data := []byte("0123456789")
			loc := f.create(BasePathREST, "dev-0001", data)
			f.fin.fail = errors.New("object store down")
			if res := f.patch(loc, "dev-0001", 0, data); res.StatusCode != http.StatusInternalServerError {
				t.Fatalf("final patch: %d", res.StatusCode)
			}
			lock, err := f.st.lock(context.Background(), refOf(loc))
			if err != nil {
				t.Fatal(err)
			}
			res := f.patchOrHeadFull(method, loc, len(data))
			if err := lock.Unlock(); err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != http.StatusLocked {
				t.Errorf("%s while the lock is held: %d, want 423", method, res.StatusCode)
			}
			if row, _ := f.repo.Get(context.Background(), ProtocolREST, refOf(loc)); row.Status != db.StatusUploading {
				t.Errorf("status = %s", row.Status)
			}
		})
	}
}

// TestShutdownBeforeTusd: when the request context is cancelled for
// shutdown, the middleware's own failures answer 503 like tusd does.
func TestShutdownBeforeTusd(t *testing.T) {
	f := newFixture(t)
	loc := f.create(BasePathREST, "dev-0001", []byte("0123456789"))
	ref := refOf(loc)
	serve := func() *httptest.ResponseRecorder {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(handler.ErrServerShutdown)
		req := httptest.NewRequestWithContext(ctx, http.MethodHead, BasePathREST+ref, nil)
		req.Header.Set("Tus-Resumable", "1.0.0")
		req.Header.Set("Authorization", "Bearer dev:dev-0001")
		rec := httptest.NewRecorder()
		f.mux.ServeHTTP(rec, req)
		return rec
	}
	// Repo.Get fails with the cancelled context.
	if rec := serve(); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("repo failure during shutdown: %d, want 503", rec.Code)
	}
	// Waiting for the expiry cleaner (use registry) is cut short.
	release, ok := f.st.use.tryExclusive(ref)
	if !ok {
		t.Fatal("tryExclusive")
	}
	defer release()
	if rec := serve(); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("enter failure during shutdown: %d, want 503", rec.Code)
	}
}
