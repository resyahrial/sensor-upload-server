package cleanup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

var retNow = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

// fakeDisk is a filesystem of total bytes; the object store adds and
// frees object sizes.
type fakeDisk struct {
	mu          sync.Mutex
	total, used uint64
	err         error
	calls       int
}

func (d *fakeDisk) stat(path string) (DiskUsage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if path != "/data" {
		return DiskUsage{}, fmt.Errorf("stat %q: unexpected path", path)
	}
	if d.err != nil {
		return DiskUsage{}, d.err
	}
	return DiskUsage{Total: d.total, Used: d.used}, nil
}

func (d *fakeDisk) free(n uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.used -= n
}

// fakeObjects maps keys to sizes; disk (if set) is freed on Delete.
type fakeObjects struct {
	mu      sync.Mutex
	objs    map[string]uint64
	failKey map[string]bool
	disk    *fakeDisk
}

func (o *fakeObjects) Delete(_ context.Context, key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failKey[key] {
		return errBoom
	}
	n, ok := o.objs[key]
	if !ok {
		return nil // missing is fine, as for the real stores
	}
	delete(o.objs, key)
	if o.disk != nil {
		o.disk.free(n)
	}
	return nil
}

func (o *fakeObjects) has(key string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.objs[key]
	return ok
}

// fakeRetRepo holds completed rows (and anything else, which it never
// lists, like the real query).
type fakeRetRepo struct {
	mu        sync.Mutex
	rows      []db.Upload
	listErr   error
	goneIDs   map[uuid.UUID]bool // DeleteCompleted reports ErrNotFound
	listCalls []int              // limit of each ListCompleted
}

func (r *fakeRetRepo) ListCompleted(_ context.Context, minAge time.Duration, limit int) ([]db.Upload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls = append(r.listCalls, limit)
	if r.listErr != nil {
		return nil, r.listErr
	}
	var out []db.Upload
	for _, u := range r.rows {
		if u.Status == db.StatusCompleted && u.CompletedAt.Before(retNow.Add(-minAge)) {
			out = append(out, u)
		}
	}
	slices.SortFunc(out, func(a, b db.Upload) int { return a.CompletedAt.Compare(*b.CompletedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRetRepo) DeleteCompleted(ctx context.Context, id uuid.UUID, del func(context.Context, string) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := slices.IndexFunc(r.rows, func(u db.Upload) bool { return u.ID == id && u.Status == db.StatusCompleted })
	if i < 0 || r.goneIDs[id] {
		return fmt.Errorf("delete completed upload: %w", db.ErrNotFound)
	}
	if err := del(ctx, r.rows[i].StorageKey); err != nil {
		return err
	}
	r.rows = slices.Delete(r.rows, i, i+1)
	return nil
}

func (r *fakeRetRepo) ids() []uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []uuid.UUID
	for _, u := range r.rows {
		out = append(out, u.ID)
	}
	return out
}

type retEnv struct {
	repo    *fakeRetRepo
	objects *fakeObjects
	disk    *fakeDisk
	metrics *obs.Metrics
	logs    *syncBuffer
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func newRetEnv(t *testing.T) *retEnv {
	t.Helper()
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	disk := &fakeDisk{total: 100, used: 50}
	return &retEnv{
		repo:    &fakeRetRepo{goneIDs: map[uuid.UUID]bool{}},
		objects: &fakeObjects{objs: map[string]uint64{}, failKey: map[string]bool{}, disk: disk},
		disk:    disk,
		metrics: m,
		logs:    &syncBuffer{},
	}
}

// add stores a completed upload of size bytes, completed age ago, and
// returns its row; status other than completed adds a row only.
func (e *retEnv) add(age time.Duration, size uint64, status db.Status) db.Upload {
	at := retNow.Add(-age)
	id := uuid.Must(uuid.NewV7())
	u := db.Upload{ID: id, Protocol: "rest", Ref: id.String(), Status: status, Size: int64(size),
		StorageKey: "dev/" + id.String(), CompletedAt: &at}
	e.repo.rows = append(e.repo.rows, u)
	if status == db.StatusCompleted {
		e.objects.objs[u.StorageKey] = size // counted in disk.used by the test
	}
	return u
}

func (e *retEnv) cfg() RetentionConfig {
	return RetentionConfig{
		MaxAge: 7 * 24 * time.Hour, Interval: time.Hour,
		DiskPath: "/data", HighWatermark: 85, LowWatermark: 70, DiskInterval: time.Minute,
	}
}

func (e *retEnv) deps() RetentionDeps {
	return RetentionDeps{Repo: e.repo, Store: e.objects, Stat: e.disk.stat, Metrics: e.metrics,
		Logger: slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
}

func (e *retEnv) newRetention(t *testing.T, cfg RetentionConfig) *Retention {
	t.Helper()
	r, err := NewRetention(cfg, e.deps())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (e *retEnv) deleted(reason string) float64 {
	return testutil.ToFloat64(e.metrics.RetentionDeleted.WithLabelValues(reason))
}

func TestNewRetentionValidates(t *testing.T) {
	e := newRetEnv(t)
	mod := func(f func(*RetentionConfig)) RetentionConfig { c := e.cfg(); f(&c); return c }
	bad := map[string]RetentionConfig{
		"negative max age": mod(func(c *RetentionConfig) { c.MaxAge = -time.Hour }),
		"zero interval":    mod(func(c *RetentionConfig) { c.Interval = 0 }),
		"zero disk check":  mod(func(c *RetentionConfig) { c.DiskInterval = 0 }),
		"no disk path":     mod(func(c *RetentionConfig) { c.DiskPath = "" }),
		"low >= high":      mod(func(c *RetentionConfig) { c.LowWatermark = 85 }),
		"high > 100":       mod(func(c *RetentionConfig) { c.HighWatermark = 101 }),
		"low < 1":          mod(func(c *RetentionConfig) { c.LowWatermark = 0 }),
		"negative batch":   mod(func(c *RetentionConfig) { c.BatchSize = -1 }),
	}
	for name, cfg := range bad {
		if _, err := NewRetention(cfg, e.deps()); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	d := e.deps()
	d.Repo = nil
	if _, err := NewRetention(e.cfg(), d); err == nil {
		t.Error("missing repo: want error")
	}
	// DISK_PATH is checked at startup, so a missing mount fails fast.
	e.disk.err = errBoom
	if _, err := NewRetention(e.cfg(), e.deps()); !errors.Is(err, errBoom) {
		t.Errorf("stat failure at start = %v, want errBoom", err)
	}
}

func TestPurgeAgedDeletesOnlyOldCompleted(t *testing.T) {
	e := newRetEnv(t)
	old1 := e.add(8*24*time.Hour, 1, db.StatusCompleted)
	old2 := e.add(30*24*time.Hour, 1, db.StatusCompleted)
	recent := e.add(24*time.Hour, 1, db.StatusCompleted)
	uploading := e.add(60*24*time.Hour, 0, db.StatusUploading)
	r := e.newRetention(t, e.cfg())

	n, err := r.PurgeAged(t.Context())
	if err != nil || n != 2 {
		t.Fatalf("PurgeAged = %d, %v; want 2, nil", n, err)
	}
	if got := e.repo.ids(); !slices.Equal(got, []uuid.UUID{recent.ID, uploading.ID}) {
		t.Fatalf("rows left = %v", got)
	}
	if e.objects.has(old1.StorageKey) || e.objects.has(old2.StorageKey) || !e.objects.has(recent.StorageKey) {
		t.Fatalf("objects left = %v", e.objects.objs)
	}
	if got := e.deleted(obs.RetentionAge); got != 2 {
		t.Errorf("retention_deleted{age} = %v", got)
	}
	if !strings.Contains(e.logs.String(), "retention batch") {
		t.Errorf("no batch log: %s", e.logs)
	}
}

func TestPurgeAgedBatches(t *testing.T) {
	e := newRetEnv(t)
	for i := range 5 {
		e.add(time.Duration(8+i)*24*time.Hour, 1, db.StatusCompleted)
	}
	cfg := e.cfg()
	cfg.BatchSize = 2
	r := e.newRetention(t, cfg)
	n, err := r.PurgeAged(t.Context())
	if err != nil || n != 5 {
		t.Fatalf("PurgeAged = %d, %v; want 5", n, err)
	}
	if !slices.Equal(e.repo.listCalls, []int{2, 2, 2}) {
		t.Errorf("ListCompleted limits = %v, want three batches of 2", e.repo.listCalls)
	}
	if strings.Count(e.logs.String(), "retention batch") != 3 {
		t.Errorf("want one log line per batch:\n%s", e.logs)
	}
}

func TestPurgeAgedDefaultBatchSize(t *testing.T) {
	e := newRetEnv(t)
	r := e.newRetention(t, e.cfg())
	if _, err := r.PurgeAged(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(e.repo.listCalls, []int{DefaultRetentionBatch}) || DefaultRetentionBatch != 500 {
		t.Errorf("ListCompleted limits = %v", e.repo.listCalls)
	}
}

func TestPurgeAgedDisabled(t *testing.T) {
	e := newRetEnv(t)
	e.add(365*24*time.Hour, 1, db.StatusCompleted)
	cfg := e.cfg()
	cfg.MaxAge = 0
	r := e.newRetention(t, cfg)
	if n, err := r.PurgeAged(t.Context()); n != 0 || err != nil {
		t.Fatalf("PurgeAged disabled = %d, %v", n, err)
	}
	if len(e.repo.listCalls) != 0 || len(e.repo.rows) != 1 {
		t.Fatalf("disabled retention listed %v, rows %d", e.repo.listCalls, len(e.repo.rows))
	}
}

func TestPurgeAgedIdempotent(t *testing.T) {
	e := newRetEnv(t)
	// Object already gone (crash after the object delete): the row goes.
	gone := e.add(8*24*time.Hour, 1, db.StatusCompleted)
	delete(e.objects.objs, gone.StorageKey)
	// Row deleted meanwhile (another pass): not counted, not an error.
	raced := e.add(9*24*time.Hour, 1, db.StatusCompleted)
	e.repo.goneIDs[raced.ID] = true
	r := e.newRetention(t, e.cfg())

	n, err := r.PurgeAged(t.Context())
	if err != nil || n != 1 {
		t.Fatalf("PurgeAged = %d, %v; want 1, nil", n, err)
	}
	if slices.Contains(e.repo.ids(), gone.ID) {
		t.Fatal("row with a missing object was not deleted")
	}
}

func TestPurgeAgedObjectDeleteFails(t *testing.T) {
	e := newRetEnv(t)
	bad := e.add(10*24*time.Hour, 1, db.StatusCompleted)
	good := e.add(9*24*time.Hour, 1, db.StatusCompleted)
	e.objects.failKey[bad.StorageKey] = true
	cfg := e.cfg()
	cfg.BatchSize = 1 // the failing row heads every batch
	r := e.newRetention(t, cfg)

	n, err := r.PurgeAged(t.Context())
	if !errors.Is(err, errBoom) {
		t.Fatalf("PurgeAged error = %v, want errBoom", err)
	}
	if n != 0 || !slices.Contains(e.repo.ids(), bad.ID) || !slices.Contains(e.repo.ids(), good.ID) {
		t.Fatalf("deleted %d, rows %v", n, e.repo.ids())
	}
	if len(e.repo.listCalls) != 1 {
		t.Fatalf("a failing batch must end the pass, listed %d times", len(e.repo.listCalls))
	}
	// A bigger batch gets past it; the failed row stays for the next pass.
	cfg.BatchSize = 10
	r = e.newRetention(t, cfg)
	if n, err := r.PurgeAged(t.Context()); !errors.Is(err, errBoom) || n != 1 {
		t.Fatalf("PurgeAged = %d, %v; want 1, errBoom", n, err)
	}
	if got := e.repo.ids(); !slices.Equal(got, []uuid.UUID{bad.ID}) {
		t.Fatalf("rows left = %v", got)
	}
}

func TestPurgeAgedListError(t *testing.T) {
	e := newRetEnv(t)
	e.repo.listErr = errBoom
	r := e.newRetention(t, e.cfg())
	if _, err := r.PurgeAged(t.Context()); !errors.Is(err, errBoom) {
		t.Fatalf("PurgeAged = %v, want errBoom", err)
	}
}

func TestRelieveDiskBelowHighDoesNothing(t *testing.T) {
	e := newRetEnv(t)
	e.disk.used = 84
	e.add(time.Hour, 10, db.StatusCompleted)
	r := e.newRetention(t, e.cfg())
	n, err := r.RelieveDisk(t.Context())
	if err != nil || n != 0 || len(e.repo.listCalls) != 0 {
		t.Fatalf("RelieveDisk = %d, %v (listed %v)", n, err, e.repo.listCalls)
	}
	if got := testutil.ToFloat64(e.metrics.DiskUsedRatio); got != 0.84 {
		t.Errorf("disk_used_ratio = %v, want 0.84", got)
	}
}

func TestRelieveDiskDeletesOldestUntilBelowLow(t *testing.T) {
	e := newRetEnv(t)
	e.disk.used = 90
	var rows []db.Upload
	for i := range 8 { // newest first; ages 8h, 7h, ... 1h
		rows = append(rows, e.add(time.Duration(8-i)*time.Hour, 5, db.StatusCompleted))
	}
	inProgress := e.add(100*time.Hour, 0, db.StatusUploading)
	cfg := e.cfg()
	cfg.BatchSize = 3
	r := e.newRetention(t, cfg)

	n, err := r.RelieveDisk(t.Context())
	// 90 → 85 → ... → 65: five deletes reach < 70.
	if err != nil || n != 5 {
		t.Fatalf("RelieveDisk = %d, %v; want 5", n, err)
	}
	want := []uuid.UUID{rows[5].ID, rows[6].ID, rows[7].ID, inProgress.ID}
	if got := e.repo.ids(); !slices.Equal(got, want) {
		t.Fatalf("rows left = %v, want the 3 newest and the in-progress row", got)
	}
	if got := e.deleted(obs.RetentionDisk); got != 5 {
		t.Errorf("retention_deleted{disk} = %v", got)
	}
	if got := testutil.ToFloat64(e.metrics.DiskUsedRatio); got != 0.65 {
		t.Errorf("disk_used_ratio = %v, want 0.65", got)
	}
	if got := strings.Count(e.logs.String(), "retention batch"); got != 2 {
		t.Errorf("batch logs = %d, want 2:\n%s", got, e.logs)
	}
}

func TestRelieveDiskNothingLeftWarns(t *testing.T) {
	e := newRetEnv(t)
	e.disk.used = 95
	e.add(time.Hour, 1, db.StatusCompleted)
	e.add(2*time.Hour, 1, db.StatusCompleted)
	r := e.newRetention(t, e.cfg())
	n, err := r.RelieveDisk(t.Context())
	if err != nil || n != 2 {
		t.Fatalf("RelieveDisk = %d, %v; want 2", n, err)
	}
	if logs := e.logs.String(); !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "no completed uploads left") {
		t.Errorf("no warning about nothing left to delete:\n%s", logs)
	}
}

func TestRelieveDiskStopsWithoutProgress(t *testing.T) {
	e := newRetEnv(t)
	e.disk.used = 90
	e.objects.disk = nil // deleting objects frees nothing on DISK_PATH
	for i := range 10 {
		e.add(time.Duration(i+1)*time.Hour, 5, db.StatusCompleted)
	}
	cfg := e.cfg()
	cfg.BatchSize = 3
	r := e.newRetention(t, cfg)
	n, err := r.RelieveDisk(t.Context())
	if err != nil || n != 3 {
		t.Fatalf("RelieveDisk = %d, %v; want one batch (3) then stop", n, err)
	}
	if !strings.Contains(e.logs.String(), "did not drop") {
		t.Errorf("no warning about usage not dropping:\n%s", e.logs)
	}
}

func TestRelieveDiskErrors(t *testing.T) {
	e := newRetEnv(t)
	r := e.newRetention(t, e.cfg())
	e.disk.err = errBoom
	if _, err := r.RelieveDisk(t.Context()); !errors.Is(err, errBoom) {
		t.Fatalf("stat error = %v", err)
	}

	e = newRetEnv(t)
	e.disk.used = 90
	bad := e.add(2*time.Hour, 5, db.StatusCompleted)
	e.add(time.Hour, 5, db.StatusCompleted)
	e.objects.failKey[bad.StorageKey] = true
	cfg := e.cfg()
	cfg.BatchSize = 1
	r = e.newRetention(t, cfg)
	if n, err := r.RelieveDisk(t.Context()); !errors.Is(err, errBoom) || n != 0 {
		t.Fatalf("RelieveDisk with a failing delete = %d, %v", n, err)
	}
	if len(e.repo.listCalls) != 1 {
		t.Fatalf("a failing batch must end the check, listed %d times", len(e.repo.listCalls))
	}
}

func TestRetentionRun(t *testing.T) {
	e := newRetEnv(t)
	e.disk.used = 50
	old := e.add(8*24*time.Hour, 1, db.StatusCompleted)
	cfg := e.cfg()
	cfg.DiskInterval = 5 * time.Millisecond
	r := e.newRetention(t, cfg)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		e.disk.mu.Lock()
		calls := e.disk.calls
		e.disk.mu.Unlock()
		if calls >= 4 && !slices.Contains(e.repo.ids(), old.ID) {
			break // the age pass ran at start, disk checks keep running
		}
		if time.Now().After(deadline) {
			t.Fatalf("Run: %d disk checks, rows %v", calls, e.repo.ids())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestStatfs(t *testing.T) {
	du, err := Statfs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if du.Total == 0 || du.Used > du.Total || du.Ratio() < 0 || du.Ratio() > 1 {
		t.Fatalf("Statfs = %+v (ratio %v)", du, du.Ratio())
	}
	if _, err := Statfs(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("Statfs of a missing path: want error")
	}
	if (DiskUsage{}).Ratio() != 0 {
		t.Fatal("empty DiskUsage ratio must be 0")
	}
}
