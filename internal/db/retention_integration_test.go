//go:build integration

package db

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// completeRow inserts and completes a rest upload and returns it.
func completeRow(t *testing.T, r *Repo) Upload {
	t.Helper()
	ctx := testCtx(t)
	ref := newRef()
	if _, err := r.InsertUploading(ctx, "rest", ref, testMeta(t)); err != nil {
		t.Fatal(err)
	}
	u, err := r.Complete(ctx, "rest", ref, "k/"+ref)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func setCompletedAt(t *testing.T, r *Repo, id uuid.UUID, at time.Time) {
	t.Helper()
	if _, err := r.pool.Exec(testCtx(t), `UPDATE uploads SET completed_at = $2 WHERE id = $1`, id, at); err != nil {
		t.Fatal(err)
	}
}

func idsOf(us []Upload) []uuid.UUID {
	out := make([]uuid.UUID, len(us))
	for i, u := range us {
		out[i] = u.ID
	}
	return out
}

func TestRepoListCompleted(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	// Far in the past, so they sort before every other test's rows.
	base := time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
	var old []uuid.UUID
	for i := range 3 {
		u := completeRow(t, r)
		setCompletedAt(t, r, u.ID, base.Add(time.Duration(2-i)*time.Hour)) // inserted newest first
		old = append(old, u.ID)
	}
	slices.Reverse(old) // oldest first
	recent := completeRow(t, r)
	// An in-progress row is never listed, however old.
	inProgress := newRef()
	if _, err := r.InsertUploading(ctx, "grpc", inProgress, testMeta(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.pool.Exec(ctx, `UPDATE uploads SET created_at = $2, updated_at = $2 WHERE upload_ref = $1`,
		inProgress, base.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	got, err := r.ListCompleted(ctx, 24*time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	if ids := idsOf(got); !slices.Equal(ids, old[:2]) {
		t.Fatalf("ListCompleted(24h, 2) = %v, want %v", ids, old[:2])
	}
	for _, u := range got {
		if u.Status != StatusCompleted || u.StorageKey == "" {
			t.Fatalf("listed row = %+v", u)
		}
	}

	got, err = r.ListCompleted(ctx, 24*time.Hour, 1000)
	if err != nil {
		t.Fatal(err)
	}
	ids := idsOf(got)
	if !slices.Equal(ids[:3], old) || slices.Contains(ids, recent.ID) || slices.Contains(refsOf(got), inProgress) {
		t.Fatalf("ListCompleted(24h) = %v; want %v first, without the recent and in-progress rows", ids, old)
	}

	// minAge 0 lists every completed row, oldest first (disk pressure).
	got, err = r.ListCompleted(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if ids := idsOf(got); !slices.Equal(ids[:3], old) || !slices.Contains(ids, recent.ID) {
		t.Fatalf("ListCompleted(0) = %v", ids)
	}
	if _, err := r.ListCompleted(ctx, 0, 0); err == nil {
		t.Fatal("limit 0: want error")
	}
}

func TestRepoDeleteCompleted(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	u := completeRow(t, r)

	// The object delete fails: the row stays.
	if err := r.DeleteCompleted(ctx, u.ID, func(context.Context, string) error { return errors.New("s3 down") }); err == nil {
		t.Fatal("failing object delete: want error")
	}
	if got, err := r.Get(ctx, u.Protocol, u.Ref); err != nil || got.Status != StatusCompleted {
		t.Fatalf("row after failed delete = %+v, %v", got, err)
	}

	var keys []string
	del := func(_ context.Context, key string) error { keys = append(keys, key); return nil }
	if err := r.DeleteCompleted(ctx, u.ID, del); err != nil {
		t.Fatalf("DeleteCompleted: %v", err)
	}
	if !slices.Equal(keys, []string{u.StorageKey}) {
		t.Fatalf("object keys deleted = %v, want [%s]", keys, u.StorageKey)
	}
	if _, err := r.Get(ctx, u.Protocol, u.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete = %v, want ErrNotFound", err)
	}
	if err := r.DeleteCompleted(ctx, u.ID, del); !errors.Is(err, ErrNotFound) || len(keys) != 1 {
		t.Fatalf("second DeleteCompleted = %v (keys %v), want ErrNotFound without an object delete", err, keys)
	}

	// The same file can be uploaded again.
	if _, err := r.FindCompletedBySHA(ctx, u.SHA256); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindCompletedBySHA after delete = %v", err)
	}
	ref := newRef()
	if _, err := r.InsertUploading(ctx, "grpc", ref, u.Metadata()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Complete(ctx, "grpc", ref, u.StorageKey); err != nil {
		t.Fatalf("re-upload Complete: %v", err)
	}

	// In-progress rows are never deleted.
	up, err := r.InsertUploading(ctx, "rest", newRef(), testMeta(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteCompleted(ctx, up.ID, del); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteCompleted on uploading row = %v, want ErrNotFound", err)
	}
	if got, err := r.Get(ctx, up.Protocol, up.Ref); err != nil || got.Status != StatusUploading {
		t.Fatalf("uploading row after delete attempt = %+v, %v", got, err)
	}
}

// A dedup lookup that races a retention delete waits for it, so a new
// upload of the same file is not rejected as a duplicate of a row (and
// object) that is being deleted.
func TestRepoFindCompletedWaitsForDelete(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	u := completeRow(t, r)

	inDelete, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock() // on failure, so the transaction ends and the pool can close
	delDone := make(chan error, 1)
	go func() {
		delDone <- r.DeleteCompleted(ctx, u.ID, func(context.Context, string) error {
			close(inDelete)
			<-release
			return nil
		})
	}()
	<-inDelete

	found := make(chan error, 1)
	go func() {
		_, err := r.FindCompletedBySHA(ctx, u.SHA256)
		found <- err
	}()
	select {
	case err := <-found:
		t.Fatalf("FindCompletedBySHA returned %v while the delete was in progress", err)
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	if err := <-delDone; err != nil {
		t.Fatal(err)
	}
	if err := <-found; !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindCompletedBySHA after delete = %v, want ErrNotFound", err)
	}
}
