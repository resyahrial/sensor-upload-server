//go:build integration

package db

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

func newRepo(t *testing.T) *Repo {
	t.Helper()
	ctx := testCtx(t)
	pool, err := Open(ctx, testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return NewRepo(pool)
}

func randSHA(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func testMeta(t *testing.T) meta.Metadata {
	return meta.Metadata{
		DeviceID:    "bioac-0142",
		SHA256:      randSHA(t),
		Filename:    "rec.flac",
		ContentType: "audio/flac",
		CapturedAt:  time.Date(2026, 10, 21, 5, 30, 0, 0, time.FixedZone("WIB", 7*3600)),
		Priority:    3,
		Size:        100,
	}
}

func newRef() string { return uuid.Must(uuid.NewV7()).String() }

func TestRepoInsertAndGet(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	m := testMeta(t)
	ref := newRef()

	ins, err := r.InsertUploading(ctx, "rest", ref, m)
	if err != nil {
		t.Fatalf("InsertUploading: %v", err)
	}
	if ins.ID.Version() != 7 || ins.Status != StatusUploading || ins.StorageKey != "" || ins.CompletedAt != nil {
		t.Fatalf("inserted row = %+v", ins)
	}
	got, err := r.Get(ctx, "rest", ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != ins.ID || !got.CapturedAt.Equal(m.CapturedAt) {
		t.Fatalf("Get = %+v, want %+v", got, ins)
	}
	gm := got.Metadata()
	gm.CapturedAt = m.CapturedAt
	if gm != m {
		t.Fatalf("Metadata() = %+v, want %+v", gm, m)
	}
	if _, err := r.Get(ctx, "grpc", ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get other protocol = %v, want ErrNotFound", err)
	}
	if _, err := r.InsertUploading(ctx, "rest", ref, m); err == nil {
		t.Fatal("duplicate (protocol, ref): want error")
	}
}

func TestRepoCompleteAndDuplicate(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	m := testMeta(t)
	refA, refB := newRef(), newRef()
	if _, err := r.InsertUploading(ctx, "rest", refA, m); err != nil {
		t.Fatal(err)
	}
	if _, err := r.InsertUploading(ctx, "grpc", refB, m); err != nil {
		t.Fatal(err)
	}
	if _, err := r.FindCompletedBySHA(ctx, m.SHA256); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindCompletedBySHA before = %v, want ErrNotFound", err)
	}

	done, err := r.Complete(ctx, "rest", refA, "k/a")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if done.Status != StatusCompleted || done.StorageKey != "k/a" || done.CompletedAt == nil || done.CommittedBytes != m.Size {
		t.Fatalf("completed row = %+v", done)
	}
	found, err := r.FindCompletedBySHA(ctx, m.SHA256)
	if err != nil || found.Ref != refA {
		t.Fatalf("FindCompletedBySHA = %+v, %v", found, err)
	}

	if _, err := r.Complete(ctx, "grpc", refB, "k/b"); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("Complete duplicate = %v, want ErrDuplicate", err)
	}
	if _, err := r.Complete(ctx, "rest", refA, "k/a"); !errors.Is(err, ErrNotUploading) {
		t.Fatalf("Complete twice = %v, want ErrNotUploading", err)
	}
	if _, err := r.Complete(ctx, "rest", newRef(), "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Complete unknown = %v, want ErrNotFound", err)
	}
	// The loser stays uploading.
	if b, _ := r.Get(ctx, "grpc", refB); b.Status != StatusUploading {
		t.Fatalf("loser status = %s", b.Status)
	}
}

func TestRepoFindInProgress(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	m := testMeta(t)
	if _, err := r.FindInProgress(ctx, m.SHA256, m.DeviceID, "grpc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindInProgress empty = %v", err)
	}
	old, newer := newRef(), newRef()
	for _, ref := range []string{old, newer} {
		if _, err := r.InsertUploading(ctx, "grpc", ref, m); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.InsertUploading(ctx, "rest", newRef(), m); err != nil {
		t.Fatal(err)
	}
	got, err := r.FindInProgress(ctx, m.SHA256, m.DeviceID, "grpc")
	if err != nil || got.Ref != newer {
		t.Fatalf("FindInProgress = %+v, %v; want ref %s", got, err, newer)
	}
	if err := r.SetStatus(ctx, "grpc", newer, StatusCancelled); err != nil {
		t.Fatal(err)
	}
	if got, err := r.FindInProgress(ctx, m.SHA256, m.DeviceID, "grpc"); err != nil || got.Ref != old {
		t.Fatalf("FindInProgress after cancel = %+v, %v", got, err)
	}
	if _, err := r.FindInProgress(ctx, m.SHA256, "other-dev", "grpc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindInProgress other device = %v", err)
	}
}

func TestRepoUpdates(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	m := testMeta(t)
	ref := newRef()
	ins, err := r.InsertUploading(ctx, "grpc", ref, m)
	if err != nil {
		t.Fatal(err)
	}

	if err := r.UpdateCommitted(ctx, "grpc", ref, 40); err != nil {
		t.Fatal(err)
	}
	if err := r.IncrResume(ctx, "grpc", ref); err != nil {
		t.Fatal(err)
	}
	if err := r.IncrResume(ctx, "grpc", ref); err != nil {
		t.Fatal(err)
	}
	if err := r.Touch(ctx, "grpc", ref); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(ctx, "grpc", ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.CommittedBytes != 40 || got.ResumeCount != 2 || !got.UpdatedAt.After(ins.UpdatedAt) {
		t.Fatalf("after updates = %+v (inserted %v)", got, ins.UpdatedAt)
	}

	unknown := newRef()
	for name, err := range map[string]error{
		"UpdateCommitted": r.UpdateCommitted(ctx, "grpc", unknown, 1),
		"IncrResume":      r.IncrResume(ctx, "grpc", unknown),
		"Touch":           r.Touch(ctx, "grpc", unknown),
		"SetStatus":       r.SetStatus(ctx, "grpc", unknown, StatusExpired),
	} {
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("%s unknown = %v, want ErrNotFound", name, err)
		}
	}

	if err := r.SetStatus(ctx, "grpc", ref, StatusUploading); err == nil {
		t.Fatal("SetStatus uploading: want error")
	}
	if err := r.SetStatus(ctx, "grpc", ref, StatusFailedChecksum); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.Get(ctx, "grpc", ref); got.Status != StatusFailedChecksum {
		t.Fatalf("status = %s", got.Status)
	}
	for name, err := range map[string]error{
		"UpdateCommitted": r.UpdateCommitted(ctx, "grpc", ref, 41),
		"IncrResume":      r.IncrResume(ctx, "grpc", ref),
		"Touch":           r.Touch(ctx, "grpc", ref),
		"SetStatus":       r.SetStatus(ctx, "grpc", ref, StatusCancelled),
	} {
		if !errors.Is(err, ErrNotUploading) {
			t.Errorf("%s on failed row = %v, want ErrNotUploading", name, err)
		}
	}
}

func TestRepoListStaleAndFullyCommitted(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	stale, fresh, full, done := newRef(), newRef(), newRef(), newRef()
	for _, ref := range []string{stale, fresh, full, done} {
		if _, err := r.InsertUploading(ctx, "grpc", ref, testMeta(t)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.pool.Exec(ctx,
		`UPDATE uploads SET updated_at = now() - interval '2 hours' WHERE upload_ref = ANY($1)`,
		[]string{stale, done}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Complete(ctx, "grpc", done, "k/"+done); err != nil {
		t.Fatal(err)
	}
	if _, err := r.pool.Exec(ctx,
		`UPDATE uploads SET updated_at = now() - interval '2 hours' WHERE upload_ref = $1`, done); err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateCommitted(ctx, "grpc", full, 100); err != nil {
		t.Fatal(err)
	}

	got, err := r.ListStale(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	refs := refsOf(got)
	if !slices.Contains(refs, stale) || slices.Contains(refs, fresh) || slices.Contains(refs, done) {
		t.Fatalf("ListStale refs = %v, want %s only among ours", refs, stale)
	}

	got, err = r.ListFullyCommitted(ctx)
	if err != nil {
		t.Fatal(err)
	}
	refs = refsOf(got)
	if !slices.Contains(refs, full) || slices.Contains(refs, fresh) || slices.Contains(refs, done) {
		t.Fatalf("ListFullyCommitted refs = %v, want %s among ours", refs, full)
	}
}

func refsOf(us []Upload) []string {
	refs := make([]string, len(us))
	for i, u := range us {
		refs[i] = u.Ref
	}
	return refs
}
