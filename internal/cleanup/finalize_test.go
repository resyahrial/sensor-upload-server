package cleanup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/finalize"
)

const stagedSize = 10

// addGRPCStaged adds a gRPC row with every byte committed and its .bin.
func (e *env) addGRPCStaged(t *testing.T, ref string) {
	t.Helper()
	e.repo.add("grpc", ref)
	e.repo.change("grpc", ref, func(u *db.Upload) { u.Size, u.CommittedBytes = stagedSize, stagedSize })
	if err := os.WriteFile(filepath.Join(e.grpc.dir, ref+".bin"), make([]byte, stagedSize), 0o600); err != nil {
		t.Fatal(err)
	}
}

// addTusStaged adds a tus row (committed_bytes stays 0, as tus never
// updates it before completion) whose staged offset equals its size.
func (e *env) addTusStaged(ref string) {
	e.repo.add("rest", ref)
	e.repo.change("rest", ref, func(u *db.Upload) { u.Size = stagedSize })
	e.tus.staged[ref] = stagedSize
}

func TestStaleFullyStagedIsFinalized(t *testing.T) {
	e := newEnv(t, time.Hour)
	e.addGRPCStaged(t, "g1")
	e.addTusStaged("t1")

	e.runOnce(t, 0, false)
	for _, k := range []key{{"grpc", "g1"}, {"rest", "t1"}} {
		if s := e.repo.status(k.protocol, k.ref); s != db.StatusCompleted {
			t.Fatalf("%v status = %s, want completed", k, s)
		}
		if got := e.expiredMetric(k.protocol); got != 0 {
			t.Fatalf("%s expired metric = %v", k.protocol, got)
		}
	}
	if got := e.fin.runs(); !slices.Equal(got, []string{"g1", "t1"}) {
		t.Fatalf("finalizer runs = %v", got)
	}
	// The finalizer deleted the staging through the cleaner's input while
	// the lock was held (fakeLocks.Delete checks).
	if !slices.Equal(e.grpc.deletedRefs(), []string{"g1"}) || !slices.Equal(e.tus.deletedRefs(), []string{"t1"}) {
		t.Fatalf("deleted grpc=%v tus=%v", e.grpc.deletedRefs(), e.tus.deletedRefs())
	}
}

func TestStaleFinalizeInactiveIsExpired(t *testing.T) {
	e := newEnv(t, time.Hour)
	e.addGRPCStaged(t, "g1")
	e.addTusStaged("t1")
	e.fin.err["g1"] = fmt.Errorf("upload is cancelled: %w", finalize.ErrInactive)
	e.fin.err["t1"] = fmt.Errorf("load upload: %w", db.ErrNotFound)

	e.runOnce(t, 2, false)
	for _, k := range []key{{"grpc", "g1"}, {"rest", "t1"}} {
		if s := e.repo.status(k.protocol, k.ref); s != db.StatusExpired {
			t.Fatalf("%v status = %s, want expired", k, s)
		}
	}
}

func TestStaleFinalizeOutcomeIsKept(t *testing.T) {
	for _, outcome := range []error{finalize.ErrChecksum, finalize.ErrDuplicate} {
		t.Run(outcome.Error(), func(t *testing.T) {
			e := newEnv(t, time.Hour)
			e.addGRPCStaged(t, "g1")
			e.fin.err["g1"] = fmt.Errorf("upload g1: %w", outcome)
			e.runOnce(t, 0, false)
			if got := e.expiredMetric("grpc"); got != 0 {
				t.Fatalf("expired metric = %v", got)
			}
			if len(e.grpc.deletedRefs()) != 0 {
				t.Fatal("cleaner deleted staging itself after a final outcome")
			}
		})
	}
}

func TestStaleFinalizeErrorIsNotExpired(t *testing.T) {
	e := newEnv(t, time.Hour)
	e.addGRPCStaged(t, "g1")
	e.addTusStaged("t1")
	e.fin.err["g1"] = errBoom
	e.fin.err["t1"] = errBoom

	e.runOnce(t, 0, true)
	for _, k := range []key{{"grpc", "g1"}, {"rest", "t1"}} {
		if s := e.repo.status(k.protocol, k.ref); s != db.StatusUploading {
			t.Fatalf("%v status = %s, want uploading", k, s)
		}
	}
	if len(e.grpc.deletedRefs())+len(e.tus.deletedRefs()) != 0 {
		t.Fatal("staging of a complete upload was deleted")
	}
	// g1 is listed both as fully committed and as stale: one attempt per pass.
	if got := e.fin.runs(); !slices.Equal(got, []string{"g1", "t1"}) {
		t.Fatalf("finalizer runs = %v, want one per upload", got)
	}
}

func TestNotFullyStagedIsExpired(t *testing.T) {
	e := newEnv(t, time.Hour)
	// gRPC: every byte on disk but not every byte committed (crash before
	// the DB update): committed_bytes is authoritative.
	e.addGRPCStaged(t, "g1")
	e.repo.change("grpc", "g1", func(u *db.Upload) { u.CommittedBytes = stagedSize - 1 })
	// gRPC: committed but the .bin is short (data lost).
	e.addGRPCStaged(t, "g2")
	if err := os.Truncate(filepath.Join(e.grpc.dir, "g2.bin"), 3); err != nil {
		t.Fatal(err)
	}
	// gRPC: committed, no .bin.
	e.addGRPCStaged(t, "g3")
	if err := os.Remove(filepath.Join(e.grpc.dir, "g3.bin")); err != nil {
		t.Fatal(err)
	}
	// tus: partly staged, and nothing staged.
	e.addTusStaged("t1")
	e.tus.staged["t1"] = stagedSize - 1
	e.addTusStaged("t2")
	delete(e.tus.staged, "t2")

	e.runOnce(t, 5, false)
	if got := e.fin.runs(); len(got) != 0 {
		t.Fatalf("finalizer ran for %v", got)
	}
}

func TestFreshFullyCommittedIsFinalized(t *testing.T) {
	e := newEnv(t, time.Hour)
	e.addGRPCStaged(t, "g1")
	e.addGRPCStaged(t, "busy")
	e.repo.fresh[key{"grpc", "g1"}] = true
	e.repo.fresh[key{"grpc", "busy"}] = true
	e.grpc.lock("busy") // a stream is finalizing it
	e.repo.add("grpc", "partial")
	e.repo.change("grpc", "partial", func(u *db.Upload) { u.Size = stagedSize })
	e.repo.fresh[key{"grpc", "partial"}] = true

	n, err := e.c.RunOnce(t.Context())
	if n != 0 || err != nil {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if s := e.repo.status("grpc", "g1"); s != db.StatusCompleted {
		t.Fatalf("g1 status = %s, want completed", s)
	}
	if got := e.fin.runs(); !slices.Equal(got, []string{"g1"}) {
		t.Fatalf("finalizer runs = %v", got)
	}
	if e.repo.status("grpc", "busy") != db.StatusUploading || e.repo.status("grpc", "partial") != db.StatusUploading {
		t.Fatal("busy or partial upload changed")
	}
}

func TestFinalizeRetryErrors(t *testing.T) {
	e := newEnv(t, time.Hour)
	e.addGRPCStaged(t, "g1")
	e.repo.fresh[key{"grpc", "g1"}] = true
	e.fin.err["g1"] = errBoom
	e.addGRPCStaged(t, "geterr")
	e.repo.fresh[key{"grpc", "geterr"}] = true
	e.repo.getErr["geterr"] = errBoom

	e.runOnce(t, 0, true)
	if e.repo.status("grpc", "g1") != db.StatusUploading {
		t.Fatal("g1 changed")
	}

	e.repo.listErr = errBoom
	if _, err := e.c.RunOnce(t.Context()); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
}
