package finalize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// fakeRepo mimics db.Repo semantics in memory, including the unique
// completed-sha256 index. Hooks inject failures.
type fakeRepo struct {
	mu   sync.Mutex
	rows map[string]*db.Upload

	getErr      error
	findErr     error
	setErr      error
	completeErr func(r *fakeRepo) error // runs before Complete; non-nil error is returned
}

func newFakeRepo() *fakeRepo { return &fakeRepo{rows: map[string]*db.Upload{}} }

func (r *fakeRepo) add(u db.Upload) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[u.Protocol+"/"+u.Ref] = &u
}

func (r *fakeRepo) row(protocol, ref string) db.Upload {
	r.mu.Lock()
	defer r.mu.Unlock()
	return *r.rows[protocol+"/"+ref]
}

func (r *fakeRepo) Get(_ context.Context, protocol, ref string) (db.Upload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getErr != nil {
		return db.Upload{}, r.getErr
	}
	u, ok := r.rows[protocol+"/"+ref]
	if !ok {
		return db.Upload{}, db.ErrNotFound
	}
	return *u, nil
}

func (r *fakeRepo) FindCompletedBySHA(_ context.Context, sha string) (db.Upload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.findErr != nil {
		return db.Upload{}, r.findErr
	}
	return r.completedLocked(sha)
}

func (r *fakeRepo) completedLocked(sha string) (db.Upload, error) {
	for _, u := range r.rows {
		if u.SHA256 == sha && u.Status == db.StatusCompleted {
			return *u, nil
		}
	}
	return db.Upload{}, db.ErrNotFound
}

func (r *fakeRepo) Complete(_ context.Context, protocol, ref, key string) (db.Upload, error) {
	if r.completeErr != nil {
		hook := r.completeErr
		if err := hook(r); err != nil {
			return db.Upload{}, err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	u, ok := r.rows[protocol+"/"+ref]
	if !ok {
		return db.Upload{}, db.ErrNotFound
	}
	if u.Status != db.StatusUploading {
		return db.Upload{}, db.ErrNotUploading
	}
	if _, err := r.completedLocked(u.SHA256); err == nil {
		return db.Upload{}, db.ErrDuplicate
	}
	now := time.Now()
	u.Status, u.StorageKey, u.CompletedAt = db.StatusCompleted, key, &now
	return *u, nil
}

func (r *fakeRepo) SetStatus(_ context.Context, protocol, ref string, s db.Status) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.setErr != nil {
		return r.setErr
	}
	u, ok := r.rows[protocol+"/"+ref]
	if !ok {
		return db.ErrNotFound
	}
	if u.Status != db.StatusUploading {
		return db.ErrNotUploading
	}
	u.Status = s
	return nil
}

// fakeStore is an idempotent in-memory object store.
type fakeStore struct {
	mu        sync.Mutex
	objects   map[string][]byte
	puts      int
	putErr    error
	deleteErr error
}

func newFakeStore() *fakeStore { return &fakeStore{objects: map[string][]byte{}} }

func (s *fakeStore) Put(_ context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if s.putErr != nil {
		return s.putErr
	}
	if b, ok := s.objects[key]; ok && int64(len(b)) == size {
		return nil
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.objects[key] = b
	return nil
}

func (s *fakeStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.objects, key)
	return nil
}

func (s *fakeStore) has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key]
	return ok
}

// staging is an in-memory staged file.
type staging struct {
	mu        sync.Mutex
	data      []byte
	deleted   bool
	openErr   error
	deleteErr error
}

func (s *staging) open(context.Context) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openErr != nil {
		return nil, s.openErr
	}
	if s.deleted {
		return nil, errors.New("staged data deleted")
	}
	return io.NopCloser(bytes.NewReader(s.data)), nil
}

func (s *staging) delete(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deleted = true
	return nil
}

func (s *staging) isDeleted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleted
}

type recNotifier struct {
	got []db.Upload
	err error
}

func (n *recNotifier) Completed(_ context.Context, u db.Upload) error {
	n.got = append(n.got, u)
	return n.err
}

type env struct {
	repo     *fakeRepo
	store    *fakeStore
	metrics  *obs.Metrics
	notifier *recNotifier
	f        *Finalizer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{repo: newFakeRepo(), store: newFakeStore(), metrics: m, notifier: &recNotifier{}}
	e.f = New(e.repo, e.store, m, slog.New(slog.DiscardHandler), e.notifier)
	return e
}

var capturedAt = time.Date(2026, 10, 21, 1, 30, 0, 0, time.FixedZone("WIB", 7*3600))

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// upload adds an 'uploading' row for data and returns its Input and staging.
func (e *env) upload(protocol, ref, device string, data []byte, declaredSHA string) (Input, *staging) {
	e.repo.add(db.Upload{
		Protocol: protocol, Ref: ref, DeviceID: device, SHA256: declaredSHA,
		Size: int64(len(data)), Filename: "f.flac", ContentType: "audio/flac",
		CapturedAt: capturedAt, Priority: 2, Status: db.StatusUploading,
		CreatedAt: time.Now().Add(-time.Minute),
	})
	st := &staging{data: data}
	return Input{Protocol: protocol, Ref: ref, Open: st.open, Delete: st.delete}, st
}

func (e *env) failed(protocol, reason string) float64 {
	return testutil.ToFloat64(e.metrics.Failed.WithLabelValues(protocol, reason))
}

func (e *env) completed(protocol string) float64 {
	return testutil.ToFloat64(e.metrics.Completed.WithLabelValues(protocol))
}

var data = []byte("sensor payload bytes")

func TestRunHappyPath(t *testing.T) {
	e := newEnv(t)
	in, st := e.upload("rest", "r1", "bioac-0142", data, shaHex(data))

	key, err := e.f.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// captured_at 01:30+07:00 is 2026-10-20 in UTC.
	want := "bioac-0142/2026/10/20/" + shaHex(data) + ".flac"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	row := e.repo.row("rest", "r1")
	if row.Status != db.StatusCompleted || row.StorageKey != key {
		t.Fatalf("row = %+v", row)
	}
	if !bytes.Equal(e.store.objects[key], data) {
		t.Fatal("object content differs")
	}
	if !st.isDeleted() {
		t.Fatal("staging not deleted")
	}
	if e.completed("rest") != 1 || len(e.notifier.got) != 1 {
		t.Fatalf("completed=%v notified=%d", e.completed("rest"), len(e.notifier.got))
	}
	if testutil.CollectAndCount(e.metrics.Duration) != 1 || testutil.CollectAndCount(e.metrics.FinalizeDuration) != 1 {
		t.Fatal("duration histograms not observed")
	}

	// Retry (e.g. the response was lost): same key, no double counting.
	key2, err := e.f.Run(context.Background(), in)
	if err != nil || key2 != key {
		t.Fatalf("rerun = %q, %v", key2, err)
	}
	if e.completed("rest") != 1 {
		t.Fatal("rerun counted completion twice")
	}
}

func TestRunChecksumMismatch(t *testing.T) {
	e := newEnv(t)
	in, st := e.upload("grpc", "g1", "bioac-0142", data, shaHex([]byte("other")))

	if _, err := e.f.Run(context.Background(), in); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Run = %v, want ErrChecksum", err)
	}
	if s := e.repo.row("grpc", "g1").Status; s != db.StatusFailedChecksum {
		t.Fatalf("status = %s", s)
	}
	if !st.isDeleted() || len(e.store.objects) != 0 || e.store.puts != 0 {
		t.Fatalf("deleted=%v objects=%d puts=%d", st.isDeleted(), len(e.store.objects), e.store.puts)
	}
	// Re-run reports the stored outcome without double counting.
	if _, err := e.f.Run(context.Background(), in); !errors.Is(err, ErrChecksum) {
		t.Fatalf("rerun = %v, want ErrChecksum", err)
	}
	if got := e.failed("grpc", obs.ReasonChecksum); got != 1 {
		t.Fatalf("failed{checksum} = %v, want 1", got)
	}
}

func TestRunDuplicateAlreadyCompleted(t *testing.T) {
	e := newEnv(t)
	winIn, _ := e.upload("rest", "w", "dev-aaa", data, shaHex(data))
	winKey, err := e.f.Run(context.Background(), winIn)
	if err != nil {
		t.Fatal(err)
	}
	in, st := e.upload("grpc", "l", "dev-bbb", data, shaHex(data))

	if _, err := e.f.Run(context.Background(), in); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("Run = %v, want ErrDuplicate", err)
	}
	if e.store.puts != 1 || len(e.store.objects) != 1 || !e.store.has(winKey) {
		t.Fatalf("puts=%d objects=%v", e.store.puts, e.store.objects)
	}
	if s := e.repo.row("grpc", "l").Status; s != db.StatusCancelled {
		t.Fatalf("loser status = %s", s)
	}
	if !st.isDeleted() {
		t.Fatal("staging not deleted")
	}
	// A retry still says duplicate.
	if _, err := e.f.Run(context.Background(), in); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("rerun = %v, want ErrDuplicate", err)
	}
	if got := e.failed("grpc", obs.ReasonDuplicate); got != 1 {
		t.Fatalf("failed{duplicate} = %v", got)
	}
}

func TestRunDuplicateRaceAtComplete(t *testing.T) {
	tests := []struct {
		name       string
		winnerDev  string
		keepObject bool
	}{
		{"other device: orphan deleted", "dev-other", false},
		{"same device and key: object kept", "bioac-0142", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			in, st := e.upload("rest", "l", "bioac-0142", data, shaHex(data))
			winKey := tt.winnerDev + "/2026/10/20/" + shaHex(data) + ".flac"
			e.repo.completeErr = func(r *fakeRepo) error {
				// The other protocol completes between our Put and Complete.
				r.add(db.Upload{Protocol: "grpc", Ref: "w", DeviceID: tt.winnerDev, SHA256: shaHex(data),
					Status: db.StatusCompleted, StorageKey: winKey})
				r.completeErr = nil
				return nil
			}
			ourKey := "bioac-0142/2026/10/20/" + shaHex(data) + ".flac"

			if _, err := e.f.Run(context.Background(), in); !errors.Is(err, ErrDuplicate) {
				t.Fatalf("Run = %v, want ErrDuplicate", err)
			}
			if got := e.store.has(ourKey); got != tt.keepObject {
				t.Fatalf("our object present = %v, want %v", got, tt.keepObject)
			}
			if s := e.repo.row("rest", "l").Status; s != db.StatusCancelled || !st.isDeleted() {
				t.Fatalf("status=%s staging deleted=%v", s, st.isDeleted())
			}
		})
	}
}

func TestRunRetryAfterCrashBetweenPutAndComplete(t *testing.T) {
	e := newEnv(t)
	in, st := e.upload("grpc", "g", "bioac-0142", data, shaHex(data))
	crash := errors.New("crash")
	e.repo.completeErr = func(r *fakeRepo) error { r.completeErr = nil; return crash }

	if _, err := e.f.Run(context.Background(), in); !errors.Is(err, crash) {
		t.Fatalf("first Run = %v, want crash", err)
	}
	if st.isDeleted() || len(e.store.objects) != 1 {
		t.Fatal("state after crash: staging must remain, object must exist")
	}
	if got := e.failed("grpc", obs.ReasonInternal); got != 1 {
		t.Fatalf("failed{internal} = %v", got)
	}
	key, err := e.f.Run(context.Background(), in)
	if err != nil || !st.isDeleted() || e.repo.row("grpc", "g").Status != db.StatusCompleted {
		t.Fatalf("retry = %q, %v", key, err)
	}
}

func TestRunRetryAfterCrashBetweenCompleteAndDelete(t *testing.T) {
	e := newEnv(t)
	in, st := e.upload("rest", "r", "bioac-0142", data, shaHex(data))
	// Complete commits, then the process "dies" before deleting staging.
	key := "bioac-0142/2026/10/20/" + shaHex(data) + ".flac"
	if _, err := e.repo.Complete(context.Background(), "rest", "r", key); err != nil {
		t.Fatal(err)
	}
	got, err := e.f.Run(context.Background(), in)
	if err != nil || got != key || !st.isDeleted() {
		t.Fatalf("Run = %q, %v, staging deleted %v", got, err, st.isDeleted())
	}
}

func TestRunConcurrentCompletion(t *testing.T) {
	e := newEnv(t)
	in, _ := e.upload("rest", "r", "bioac-0142", data, shaHex(data))
	// Another run of the same ref completes between our Put and Complete.
	e.repo.completeErr = func(r *fakeRepo) error {
		r.completeErr = nil
		u := r.row("rest", "r")
		now := time.Now()
		u.Status, u.StorageKey, u.CompletedAt = db.StatusCompleted, "the/key", &now
		r.add(u)
		return nil
	}
	key, err := e.f.Run(context.Background(), in)
	if err != nil || key != "the/key" {
		t.Fatalf("Run = %q, %v", key, err)
	}
}

func TestRunInactiveAndUnknown(t *testing.T) {
	e := newEnv(t)
	in, st := e.upload("rest", "c", "bioac-0142", data, shaHex(data))
	if err := e.repo.SetStatus(context.Background(), "rest", "c", db.StatusExpired); err != nil {
		t.Fatal(err)
	}
	if _, err := e.f.Run(context.Background(), in); !errors.Is(err, ErrInactive) {
		t.Fatalf("expired: %v, want ErrInactive", err)
	}
	if !st.isDeleted() {
		t.Fatal("staging of expired upload not deleted")
	}
	in.Ref = "missing"
	if _, err := e.f.Run(context.Background(), in); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown: %v, want db.ErrNotFound", err)
	}
	if _, err := e.f.Run(context.Background(), Input{Protocol: "rest", Ref: "c"}); err == nil {
		t.Fatal("nil Open/Delete: want error")
	}
}

func TestRunChecksumRaceWithCancel(t *testing.T) {
	e := newEnv(t)
	in, _ := e.upload("rest", "r", "bioac-0142", data, shaHex([]byte("x")))
	// The client cancels while we hash: SetStatus finds the row cancelled.
	orig := in.Open
	in.Open = func(ctx context.Context) (io.ReadCloser, error) {
		if err := e.repo.SetStatus(ctx, "rest", "r", db.StatusCancelled); err != nil && !errors.Is(err, db.ErrNotUploading) {
			return nil, err
		}
		return orig(ctx)
	}
	if _, err := e.f.Run(context.Background(), in); !errors.Is(err, ErrInactive) {
		t.Fatalf("Run = %v, want ErrInactive", err)
	}
}

func TestRunInternalErrors(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name  string
		setup func(e *env, st *staging)
		sha   string
	}{
		{"get", func(e *env, _ *staging) { e.repo.getErr = boom }, ""},
		{"open", func(_ *env, st *staging) { st.openErr = boom }, ""},
		{"find completed", func(e *env, _ *staging) { e.repo.findErr = boom }, ""},
		{"put", func(e *env, _ *staging) { e.store.putErr = boom }, ""},
		{"complete", func(e *env, _ *staging) {
			e.repo.completeErr = func(*fakeRepo) error { return boom }
		}, ""},
		{"set failed_checksum", func(e *env, _ *staging) { e.repo.setErr = boom }, shaHex([]byte("bad"))},
		{"size mismatch", func(_ *env, st *staging) { st.data = append(st.data, 'x') }, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			sha := tt.sha
			if sha == "" {
				sha = shaHex(data)
			}
			in, st := e.upload("rest", "r", "bioac-0142", data, sha)
			tt.setup(e, st)
			_, err := e.f.Run(context.Background(), in)
			if err == nil || errors.Is(err, ErrChecksum) || errors.Is(err, ErrDuplicate) {
				t.Fatalf("Run = %v, want internal error", err)
			}
			if got := e.failed("rest", obs.ReasonInternal); got != 1 {
				t.Fatalf("failed{internal} = %v", got)
			}
			if st.isDeleted() {
				t.Fatal("staging deleted after an internal error")
			}
		})
	}
}

func TestRunDuplicateErrors(t *testing.T) {
	boom := errors.New("boom")
	for name, setup := range map[string]func(e *env){
		"orphan delete": func(e *env) { e.store.deleteErr = boom },
		"cancel":        func(e *env) { e.repo.setErr = boom },
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.repo.add(db.Upload{Protocol: "grpc", Ref: "w", DeviceID: "dev-x", SHA256: shaHex(data),
				Status: db.StatusCompleted, StorageKey: "dev-x/key"})
			in, _ := e.upload("rest", "r", "bioac-0142", data, shaHex(data))
			setup(e)
			if _, err := e.f.Run(context.Background(), in); err == nil || errors.Is(err, ErrDuplicate) {
				t.Fatalf("Run = %v, want internal error", err)
			}
		})
	}
}

func TestRunInactiveLookupError(t *testing.T) {
	e := newEnv(t)
	in, _ := e.upload("rest", "r", "bioac-0142", data, shaHex(data))
	if err := e.repo.SetStatus(context.Background(), "rest", "r", db.StatusCancelled); err != nil {
		t.Fatal(err)
	}
	e.repo.findErr = errors.New("boom")
	if _, err := e.f.Run(context.Background(), in); err == nil || errors.Is(err, ErrInactive) {
		t.Fatalf("Run = %v, want internal error", err)
	}
}

func TestRunDuplicateRaceWinnerLookupError(t *testing.T) {
	e := newEnv(t)
	in, _ := e.upload("rest", "r", "bioac-0142", data, shaHex(data))
	e.repo.completeErr = func(r *fakeRepo) error {
		r.findErr = errors.New("boom")
		return db.ErrDuplicate
	}
	if _, err := e.f.Run(context.Background(), in); err == nil || errors.Is(err, ErrDuplicate) {
		t.Fatalf("Run = %v, want internal error", err)
	}
}

func TestRunSideEffectFailuresDoNotFail(t *testing.T) {
	e := newEnv(t)
	e.notifier.err = errors.New("notify down")
	in, st := e.upload("rest", "r", "bioac-0142", data, shaHex(data))
	st.deleteErr = errors.New("disk busy")
	if _, err := e.f.Run(context.Background(), in); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if e.repo.row("rest", "r").Status != db.StatusCompleted {
		t.Fatal("not completed")
	}
}

func TestRunCanceledContext(t *testing.T) {
	e := newEnv(t)
	in, _ := e.upload("rest", "r", "bioac-0142", data, shaHex(data))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.f.Run(ctx, in); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

func TestNewDefaultsNotifier(t *testing.T) {
	f := New(newFakeRepo(), newFakeStore(), nil, slog.New(slog.DiscardHandler), nil)
	if _, ok := f.notifier.(NopNotifier); !ok {
		t.Fatalf("notifier = %T, want NopNotifier", f.notifier)
	}
	if err := (NopNotifier{}).Completed(context.Background(), db.Upload{}); err != nil {
		t.Fatal(err)
	}
}
