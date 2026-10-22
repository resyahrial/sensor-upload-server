package cleanup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/finalize"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

var errBoom = errors.New("boom")

type key struct{ protocol, ref string }

// fakeRepo holds rows; ListStale returns every 'uploading' row.
type fakeRepo struct {
	mu        sync.Mutex
	rows      map[key]db.Upload
	order     []key
	listErr   error
	fresh     map[key]bool // not returned by ListStale
	getErr    map[string]error
	setErr    map[string]error
	listCalls int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{rows: map[key]db.Upload{}, fresh: map[key]bool{}, getErr: map[string]error{}, setErr: map[string]error{}}
}

func (r *fakeRepo) add(protocol, ref string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key{protocol, ref}
	r.rows[k] = db.Upload{Protocol: protocol, Ref: ref, Status: db.StatusUploading, UpdatedAt: time.Unix(1000, 0)}
	r.order = append(r.order, k)
}

func (r *fakeRepo) change(protocol, ref string, f func(*db.Upload)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	u := r.rows[key{protocol, ref}]
	f(&u)
	r.rows[key{protocol, ref}] = u
}

func (r *fakeRepo) status(protocol, ref string) db.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rows[key{protocol, ref}].Status
}

func (r *fakeRepo) ListStale(_ context.Context, ttl time.Duration) ([]db.Upload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls++
	if ttl <= 0 {
		return nil, fmt.Errorf("ttl %v", ttl)
	}
	if r.listErr != nil {
		return nil, r.listErr
	}
	var out []db.Upload
	for _, k := range r.order {
		if u := r.rows[k]; u.Status == db.StatusUploading && !r.fresh[k] {
			out = append(out, u)
		}
	}
	return out, nil
}

func (r *fakeRepo) ListFullyCommitted(context.Context) ([]db.Upload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listErr != nil {
		return nil, r.listErr
	}
	var out []db.Upload
	for _, k := range r.order {
		if u := r.rows[k]; u.Status == db.StatusUploading && u.CommittedBytes == u.Size {
			out = append(out, u)
		}
	}
	return out, nil
}

func (r *fakeRepo) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listCalls
}

func (r *fakeRepo) Get(_ context.Context, protocol, ref string) (db.Upload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.getErr[ref]; err != nil {
		return db.Upload{}, err
	}
	u, ok := r.rows[key{protocol, ref}]
	if !ok {
		return db.Upload{}, db.ErrNotFound
	}
	return u, nil
}

func (r *fakeRepo) SetStatus(_ context.Context, protocol, ref string, s db.Status) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.setErr[ref]; err != nil {
		return err
	}
	k := key{protocol, ref}
	u := r.rows[k]
	if u.Status != db.StatusUploading {
		return db.ErrNotUploading
	}
	u.Status = s
	r.rows[k] = u
	return nil
}

// fakeLocks is a lock table with deletion, used for both the gRPC registry
// (Acquire) and the gRPC/tus staging (Delete).
type fakeLocks struct {
	mu      sync.Mutex
	held    map[string]bool
	deleted []string
	lockErr map[string]error
	delErr  map[string]error
	onLock  func(ref string) // runs after a successful lock
	dir     string           // gRPC staging dir (Reader, Dir)
	staged  map[string]int64 // tus staged offsets (Offset, Open)
}

func newFakeLocks(dir string) *fakeLocks {
	return &fakeLocks{held: map[string]bool{}, lockErr: map[string]error{}, delErr: map[string]error{},
		dir: dir, staged: map[string]int64{}}
}

func (l *fakeLocks) Dir() string { return l.dir }

func (l *fakeLocks) Reader(id string) (*os.File, error) {
	return os.Open(filepath.Join(l.dir, id+".bin"))
}

func (l *fakeLocks) Acquire(id string) (func(), bool) {
	l.mu.Lock()
	if l.held[id] {
		l.mu.Unlock()
		return nil, false
	}
	l.held[id] = true
	hook := l.onLock
	l.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	return func() {
		l.mu.Lock()
		delete(l.held, id)
		l.mu.Unlock()
	}, true
}

func (l *fakeLocks) TryLock(_ context.Context, ref string) (func(), bool, error) {
	l.mu.Lock()
	err := l.lockErr[ref]
	l.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	unlock, ok := l.Acquire(ref)
	return unlock, ok, nil
}

func (l *fakeLocks) Delete(ref string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held[ref] {
		return fmt.Errorf("delete %s without holding the lock", ref)
	}
	if err := l.delErr[ref]; err != nil {
		return err
	}
	l.deleted = append(l.deleted, ref)
	return nil
}

// tusLocks adapts fakeLocks to TusStaging (Delete with a context).
type tusLocks struct{ *fakeLocks }

func (t tusLocks) Delete(_ context.Context, ref string) error { return t.fakeLocks.Delete(ref) }

// tusFiles adapts fakeLocks to TusFiles.
type tusFiles struct{ *fakeLocks }

func (t tusFiles) Offset(_ context.Context, ref string) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.held[ref] {
		return 0, fmt.Errorf("offset of %s without holding the lock", ref)
	}
	n, ok := t.staged[ref]
	if !ok {
		return 0, fmt.Errorf("tus %s: %w", ref, fs.ErrNotExist)
	}
	return n, nil
}

func (t tusFiles) Open(_ context.Context, ref string) (io.ReadCloser, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return io.NopCloser(strings.NewReader(strings.Repeat("x", int(t.staged[ref])))), nil
}

// fakeFinalizer completes the row (reading and deleting staging through
// the input) unless err[ref] is set.
type fakeFinalizer struct {
	mu    sync.Mutex
	repo  *fakeRepo
	err   map[string]error
	calls []string
}

func (f *fakeFinalizer) Run(ctx context.Context, in finalize.Input) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, in.Ref)
	err := f.err[in.Ref]
	f.mu.Unlock()
	if err != nil {
		return "", err
	}
	rc, err := in.Open(ctx)
	if err != nil {
		return "", err
	}
	n, err := io.Copy(io.Discard, rc)
	_ = rc.Close()
	if err != nil {
		return "", err
	}
	u, _ := f.repo.Get(ctx, in.Protocol, in.Ref)
	if n != u.Size {
		return "", fmt.Errorf("staged %d bytes, want %d", n, u.Size)
	}
	f.repo.change(in.Protocol, in.Ref, func(u *db.Upload) { u.Status = db.StatusCompleted })
	if err := in.Delete(ctx); err != nil {
		return "", err
	}
	return "key/" + in.Ref, nil
}

func (f *fakeFinalizer) runs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (l *fakeLocks) lock(ref string) { l.held[ref] = true }

func (l *fakeLocks) deletedRefs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.deleted)
}

func (l *fakeLocks) heldCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.held)
}

type env struct {
	repo    *fakeRepo
	grpc    *fakeLocks
	tus     *fakeLocks
	fin     *fakeFinalizer
	metrics *obs.Metrics
	c       *Cleaner
}

func newEnv(t *testing.T, interval time.Duration) *env {
	t.Helper()
	metrics, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{repo: newFakeRepo(), grpc: newFakeLocks(t.TempDir()), tus: newFakeLocks(""), metrics: metrics}
	e.fin = &fakeFinalizer{repo: e.repo, err: map[string]error{}}
	e.c, err = New(Config{TTL: time.Hour, Interval: interval}, Deps{
		Repo: e.repo, GRPC: e.grpc, GRPCStaging: e.grpc, Finalizer: e.fin,
		Tus:      map[string]TusStaging{"rest": tusLocks{e.tus}, "rest-http3": tusLocks{e.tus}},
		TusFiles: tusFiles{e.tus},
		Metrics:  metrics,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) expiredMetric(protocol string) float64 {
	return testutil.ToFloat64(e.metrics.Failed.WithLabelValues(protocol, obs.ReasonExpired))
}

func (e *env) runOnce(t *testing.T, wantN int, wantErr bool) {
	t.Helper()
	n, err := e.c.RunOnce(t.Context())
	if n != wantN || (err != nil) != wantErr {
		t.Fatalf("RunOnce = %d, %v; want %d, error %v", n, err, wantN, wantErr)
	}
	if e.grpc.heldCount() != 0 || e.tus.heldCount() != 0 {
		t.Fatal("a lock was not released")
	}
}

func TestExpireBothProtocols(t *testing.T) {
	e := newEnv(t, time.Hour)
	e.repo.add("grpc", "g1")
	e.repo.add("rest", "t1")
	e.repo.add("rest-http3", "t2")

	e.runOnce(t, 3, false)
	for _, k := range []key{{"grpc", "g1"}, {"rest", "t1"}, {"rest-http3", "t2"}} {
		if s := e.repo.status(k.protocol, k.ref); s != db.StatusExpired {
			t.Fatalf("%v status = %s", k, s)
		}
		if got := e.expiredMetric(k.protocol); got != 1 {
			t.Fatalf("%s expired metric = %v", k.protocol, got)
		}
	}
	if got := e.grpc.deletedRefs(); !slices.Equal(got, []string{"g1"}) {
		t.Fatalf("grpc deleted = %v", got)
	}
	if got := e.tus.deletedRefs(); !slices.Equal(got, []string{"t1", "t2"}) {
		t.Fatalf("tus deleted = %v", got)
	}
	e.runOnce(t, 0, false) // nothing left
}

func TestBusySkipped(t *testing.T) {
	e := newEnv(t, time.Hour)
	e.repo.add("grpc", "g1")
	e.repo.add("rest", "t1")
	e.grpc.lock("g1")
	e.tus.lock("t1")

	n, err := e.c.RunOnce(t.Context())
	if n != 0 || err != nil {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if e.repo.status("grpc", "g1") != db.StatusUploading || e.repo.status("rest", "t1") != db.StatusUploading {
		t.Fatal("busy upload was expired")
	}
	if len(e.grpc.deletedRefs())+len(e.tus.deletedRefs()) != 0 {
		t.Fatal("busy upload's staging was deleted")
	}
}

func TestChangedMeanwhile(t *testing.T) {
	t.Run("touched", func(t *testing.T) {
		e := newEnv(t, time.Hour)
		e.repo.add("rest", "t1")
		e.tus.onLock = func(ref string) {
			e.repo.change("rest", ref, func(u *db.Upload) { u.UpdatedAt = u.UpdatedAt.Add(time.Second) })
		}
		e.runOnce(t, 0, false)
		if e.repo.status("rest", "t1") != db.StatusUploading || len(e.tus.deletedRefs()) != 0 {
			t.Fatal("touched upload was expired")
		}
	})
	t.Run("completed before lock", func(t *testing.T) {
		e := newEnv(t, time.Hour)
		e.repo.add("grpc", "g1")
		e.grpc.onLock = func(ref string) {
			e.repo.change("grpc", ref, func(u *db.Upload) { u.Status = db.StatusCompleted })
		}
		e.runOnce(t, 0, false)
		if e.repo.status("grpc", "g1") != db.StatusCompleted || len(e.grpc.deletedRefs()) != 0 {
			t.Fatal("completed upload was touched")
		}
	})
	t.Run("completed before mark", func(t *testing.T) {
		e := newEnv(t, time.Hour)
		e.repo.add("grpc", "g1")
		e.repo.setErr["g1"] = fmt.Errorf("set status: %w", db.ErrNotUploading)
		e.runOnce(t, 0, false)
		if got := e.expiredMetric("grpc"); got != 0 {
			t.Fatalf("expired metric = %v", got)
		}
	})
}

func TestErrorsDoNotStopOthers(t *testing.T) {
	e := newEnv(t, time.Hour)
	for _, ref := range []string{"lockerr", "geterr", "delerr", "seterr", "ok"} {
		e.repo.add("rest", ref)
	}
	e.repo.add("ftp", "unknown")
	e.tus.lockErr["lockerr"] = errBoom
	e.repo.getErr["geterr"] = errBoom
	e.tus.delErr["delerr"] = errBoom
	e.repo.setErr["seterr"] = errBoom

	n, err := e.c.RunOnce(t.Context())
	if n != 1 || !errors.Is(err, errBoom) {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	for _, ref := range []string{"lockerr", "geterr", "delerr", "seterr", "unknown"} {
		if !strings.Contains(err.Error(), "/"+ref+":") {
			t.Fatalf("error %q does not mention %s", err, ref)
		}
	}
	if e.repo.status("rest", "ok") != db.StatusExpired {
		t.Fatal("ok row not expired")
	}
	if e.repo.status("rest", "delerr") != db.StatusUploading {
		t.Fatal("row marked expired although its staging delete failed")
	}
}

func TestListError(t *testing.T) {
	e := newEnv(t, time.Hour)
	e.repo.listErr = errBoom
	if _, err := e.c.RunOnce(t.Context()); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
}

func TestCancelledContextStops(t *testing.T) {
	e := newEnv(t, time.Hour)
	for _, ref := range []string{"t1", "t2"} {
		e.repo.add("rest", ref)
		// not fully committed, so only expire takes the lock
		e.repo.change("rest", ref, func(u *db.Upload) { u.Size = 1 })
	}
	ctx, cancel := context.WithCancel(t.Context())
	e.tus.onLock = func(string) { cancel() }
	n, err := e.c.RunOnce(ctx)
	if n != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if e.repo.status("rest", "t2") != db.StatusUploading {
		t.Fatal("ran past cancellation")
	}
}

func TestRun(t *testing.T) {
	e := newEnv(t, 10*time.Millisecond)
	e.repo.listErr = errBoom // errors are logged, the loop goes on
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		e.c.Run(ctx)
		close(done)
	}()
	deadline := time.After(5 * time.Second)
	for e.repo.calls() < 3 {
		select {
		case <-deadline:
			t.Fatalf("only %d runs", e.repo.calls())
		case <-time.After(5 * time.Millisecond):
		}
	}
	e.repo.mu.Lock()
	e.repo.listErr = nil
	e.repo.mu.Unlock()
	e.repo.add("grpc", "g1")
	for e.repo.status("grpc", "g1") != db.StatusExpired {
		select {
		case <-deadline:
			t.Fatal("g1 not expired by Run")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on cancel")
	}
}

func TestNew(t *testing.T) {
	metrics, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	locks := newFakeLocks(t.TempDir())
	ok := Deps{Repo: newFakeRepo(), GRPC: locks, GRPCStaging: locks, Finalizer: &fakeFinalizer{},
		Metrics: metrics, Logger: slog.New(slog.DiscardHandler)}
	good := Config{TTL: time.Hour, Interval: time.Minute}
	if _, err := New(good, ok); err != nil {
		t.Fatalf("New: %v", err)
	}
	for name, tc := range map[string]struct {
		cfg Config
		d   func(Deps) Deps
	}{
		"ttl":        {Config{Interval: time.Minute}, nil},
		"interval":   {Config{TTL: time.Hour}, nil},
		"repo":       {good, func(d Deps) Deps { d.Repo = nil; return d }},
		"grpc":       {good, func(d Deps) Deps { d.GRPC = nil; return d }},
		"finalizer":  {good, func(d Deps) Deps { d.Finalizer = nil; return d }},
		"tus grpc":   {good, func(d Deps) Deps { d.Tus = map[string]TusStaging{"grpc": tusLocks{locks}}; return d }},
		"tus nil":    {good, func(d Deps) Deps { d.Tus = map[string]TusStaging{"rest": nil}; return d }},
		"no metrics": {good, func(d Deps) Deps { d.Metrics = nil; return d }},
	} {
		t.Run(name, func(t *testing.T) {
			d := ok
			if tc.d != nil {
				d = tc.d(d)
			}
			if _, err := New(tc.cfg, d); err == nil {
				t.Fatal("New succeeded")
			}
		})
	}
}
