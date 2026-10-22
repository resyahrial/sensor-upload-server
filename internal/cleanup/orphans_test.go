package cleanup

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// touchFiles creates dir/name for each name with mtime age ago.
func touchFiles(t *testing.T, dir string, age time.Duration, names ...string) {
	t.Helper()
	mt := time.Now().Add(-age)
	for _, n := range names {
		p := filepath.Join(dir, n)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
}

func exists(t *testing.T, dir, name string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

func newID() string { return uuid.Must(uuid.NewV7()).String() }

// sweepEnv is an env whose tus staging is a real directory.
func sweepEnv(t *testing.T) (*env, string) {
	t.Helper()
	e := newEnv(t, time.Hour)
	tusDir := t.TempDir()
	e.c.cfg.TusDir = tusDir
	return e, tusDir
}

func (e *env) orphanMetric(area string) float64 {
	return testutil.ToFloat64(e.metrics.StagingOrphans.WithLabelValues(area))
}

func TestSweepGRPCOrphans(t *testing.T) {
	e, _ := sweepEnv(t)
	dir := e.grpc.dir
	old := 2 * time.Hour
	terminal := map[db.Status]string{}
	for _, s := range []db.Status{db.StatusCompleted, db.StatusFailedChecksum, db.StatusCancelled, db.StatusExpired} {
		id := newID()
		terminal[s] = id
		e.repo.add("grpc", id)
		e.repo.change("grpc", id, func(u *db.Upload) { u.Status = s })
		touchFiles(t, dir, old, id+".bin", id+".json", id+".json.tmp-123")
	}
	missing := newID() // no row (crash mid-SaveInfo after the row was purged)
	touchFiles(t, dir, old, missing+".json.tmp-9")

	recent := newID() // terminal, but touched recently
	e.repo.add("grpc", recent)
	e.repo.change("grpc", recent, func(u *db.Upload) { u.Status = db.StatusCompleted })
	touchFiles(t, dir, old, recent+".bin")
	touchFiles(t, dir, time.Minute, recent+".json")

	recentMissing := newID() // no row yet: Init in flight
	touchFiles(t, dir, time.Minute, recentMissing+".bin", recentMissing+".json")

	live := newID() // old files of an uploading row (expiry's business)
	e.repo.add("grpc", live)
	e.repo.change("grpc", live, func(u *db.Upload) { u.Size = 5; u.UpdatedAt = time.Now() })
	e.repo.fresh[key{"grpc", live}] = true
	touchFiles(t, dir, old, live+".bin", live+".json")

	busy := newID() // terminal but held
	e.repo.add("grpc", busy)
	e.repo.change("grpc", busy, func(u *db.Upload) { u.Status = db.StatusCancelled })
	touchFiles(t, dir, old, busy+".bin")
	e.grpc.lock(busy)

	touchFiles(t, dir, old, "not-a-uuid.bin", "README")

	n, err := e.c.RunOnce(t.Context())
	if n != 0 || err != nil {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	for s, id := range terminal {
		for _, ext := range []string{".bin", ".json", ".json.tmp-123"} {
			if exists(t, dir, id+ext) {
				t.Errorf("%s orphan %s left", s, ext)
			}
		}
	}
	if exists(t, dir, missing+".json.tmp-9") {
		t.Error("orphan of a missing row left")
	}
	for _, name := range []string{recent + ".bin", recent + ".json", recentMissing + ".bin", live + ".bin", live + ".json", busy + ".bin", "not-a-uuid.bin", "README"} {
		if !exists(t, dir, name) {
			t.Errorf("%s deleted", name)
		}
	}
	if got := e.orphanMetric("grpc"); got != 5 {
		t.Errorf("orphan metric = %v, want 5", got)
	}
	// The store's Delete ran under Acquire for every terminal row.
	got := e.grpc.deletedRefs()
	for _, id := range terminal {
		if !slices.Contains(got, id) {
			t.Errorf("Delete(%s) not called (got %v)", id, got)
		}
	}
}

func TestSweepTusOrphans(t *testing.T) {
	e, dir := sweepEnv(t)
	old := 2 * time.Hour
	done := newID()
	e.repo.add("rest-http3", done)
	e.repo.change("rest-http3", done, func(u *db.Upload) { u.Status = db.StatusCancelled })
	touchFiles(t, dir, old, done, done+".info", done+".lock", done+".stop")

	missing := newID()
	touchFiles(t, dir, old, missing+".info")

	live := newID()
	e.repo.add("rest", live)
	e.repo.change("rest", live, func(u *db.Upload) { u.Size = 5 })
	e.repo.fresh[key{"rest", live}] = true
	touchFiles(t, dir, old, live, live+".info")

	recent := newID()
	e.repo.add("rest", recent)
	e.repo.change("rest", recent, func(u *db.Upload) { u.Status = db.StatusCompleted })
	touchFiles(t, dir, time.Second, recent, recent+".info")

	if _, err := e.c.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{done, done + ".info", done + ".lock", done + ".stop", missing + ".info"} {
		if exists(t, dir, name) {
			t.Errorf("orphan %s left", name)
		}
	}
	for _, name := range []string{live, live + ".info", recent, recent + ".info"} {
		if !exists(t, dir, name) {
			t.Errorf("%s deleted", name)
		}
	}
	if got := e.tus.deletedRefs(); !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values([]string{done, missing}))) {
		t.Errorf("tus Delete calls = %v", got)
	}
	if got := e.orphanMetric("tus"); got != 2 {
		t.Errorf("orphan metric = %v, want 2", got)
	}
}

func TestSweepErrors(t *testing.T) {
	e, dir := sweepEnv(t)
	old := 2 * time.Hour
	geterr := newID()
	e.repo.getErr[geterr] = errBoom
	touchFiles(t, dir, old, geterr+".info")
	delerr := newID()
	e.grpc.delErr[delerr] = errBoom
	touchFiles(t, e.grpc.dir, old, delerr+".bin")
	ok := newID()
	touchFiles(t, e.grpc.dir, old, ok+".bin")

	if _, err := e.c.RunOnce(t.Context()); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
	if !exists(t, dir, geterr+".info") || !exists(t, e.grpc.dir, delerr+".bin") {
		t.Error("files deleted despite an error")
	}
	if exists(t, e.grpc.dir, ok+".bin") {
		t.Error("an error stopped the sweep")
	}

	// A missing directory is not an error.
	e.c.cfg.TusDir = filepath.Join(dir, "nope")
	delete(e.repo.getErr, geterr)
	delete(e.grpc.delErr, delerr)
	if _, err := e.c.RunOnce(t.Context()); err != nil {
		t.Fatalf("err = %v", err)
	}
}
