//go:build integration

package db

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestRepoListCompletedPage: filters, newest-first order with the id
// tie-break, and keyset paging that neither skips nor repeats rows.
func TestRepoListCompletedPage(t *testing.T) {
	r := newRepo(t)
	ctx := testCtx(t)
	device := "list-" + randSHA(t)[:8] // isolates this test's rows
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	insert := func(contentType string, completedAt time.Time, complete bool) Upload {
		t.Helper()
		m := testMeta(t)
		m.DeviceID, m.ContentType = device, contentType
		ref := newRef()
		u, err := r.InsertUploading(ctx, "rest", ref, m)
		if err != nil {
			t.Fatal(err)
		}
		if !complete {
			return u
		}
		if u, err = r.Complete(ctx, "rest", ref, "k/"+ref); err != nil {
			t.Fatal(err)
		}
		setCompletedAt(t, r, u.ID, completedAt)
		return u
	}
	jpeg := insert("image/jpeg", at, true)
	png1 := insert("image/png", at.Add(time.Hour), true)
	png2 := insert("image/png", at.Add(time.Hour), true) // same time: id breaks the tie
	flac := insert("audio/flac", at.Add(2*time.Hour), true)
	insert("image/jpeg", at, false) // in progress: never listed

	list := func(f ListFilter) []uuid.UUID {
		t.Helper()
		f.DeviceID = device
		us, err := r.ListCompletedPage(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		return idsOf(us)
	}
	samePair := []uuid.UUID{png1.ID, png2.ID}
	slices.SortFunc(samePair, func(a, b uuid.UUID) int { return -slices.Compare(a[:], b[:]) })
	all := append(append([]uuid.UUID{flac.ID}, samePair...), jpeg.ID)

	if got := list(ListFilter{Limit: 10}); !slices.Equal(got, all) {
		t.Fatalf("all = %v, want %v", got, all)
	}
	if got := list(ListFilter{ContentTypePrefix: "image/", Limit: 10}); !slices.Equal(got, all[1:]) {
		t.Fatalf("image/* = %v, want %v", got, all[1:])
	}
	if got := list(ListFilter{ContentType: "image/jpeg", Limit: 10}); !slices.Equal(got, []uuid.UUID{jpeg.ID}) {
		t.Fatalf("image/jpeg = %v", got)
	}

	// Page through two at a time.
	var paged []uuid.UUID
	var after *ListCursor
	for range 3 {
		us, err := r.ListCompletedPage(ctx, ListFilter{DeviceID: device, After: after, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(us) == 0 {
			break
		}
		paged = append(paged, idsOf(us)...)
		last := us[len(us)-1]
		after = &ListCursor{CompletedAt: *last.CompletedAt, ID: last.ID}
	}
	if !slices.Equal(paged, all) {
		t.Fatalf("paged = %v, want %v", paged, all)
	}
	if _, err := r.ListCompletedPage(ctx, ListFilter{}); err == nil {
		t.Fatal("limit 0 accepted")
	}
}
