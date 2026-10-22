//go:build integration

package grpcadapt

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

const device = "dev-it-0001"

func TestFullUpload(t *testing.T) {
	st := newState(t)
	s := startServer(t, st)
	f := newFile(t, device, 5*chunkSize+100)

	res := s.init(f)
	if res.GetCommittedOffset() != 0 || res.GetMaxChunkBytes() != chunkSize {
		t.Fatalf("Init = %v", res)
	}
	id := res.GetUploadId()
	if u := st.row(t, id); u.Status != db.StatusUploading || u.Ref != id || u.Protocol != "grpc" {
		t.Fatalf("row after Init = %+v", u)
	}
	if err := s.uploadRest(f, id, 0); err != nil {
		t.Fatal(err)
	}

	u := st.row(t, id)
	key := meta.FinalKey(f.meta(t))
	if u.Status != db.StatusCompleted || u.StorageKey != key || u.CommittedBytes != int64(len(f.data)) || u.CompletedAt == nil {
		t.Fatalf("row = %+v, want completed at %s", u, key)
	}
	if size, ok, err := st.objects.Stat(context.Background(), key); err != nil || !ok || size != int64(len(f.data)) {
		t.Fatalf("object Stat = %d %v %v", size, ok, err)
	}
	if st.staged(t, id) {
		t.Fatal("staging data left behind")
	}
	stRes, err := s.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
	if err != nil || !stRes.GetComplete() {
		t.Fatalf("Status = %v, %v", stRes, err)
	}
	_, err = s.client.Init(ctxFor(t, device), f.req)
	wantCode(t, err, codes.AlreadyExists)
}

// partial sends the first half of f on a stream and then breaks it.
func partial(t *testing.T, s *server, f file, id string) int {
	t.Helper()
	half := 3 * chunkSize
	ctx, cancel := context.WithCancel(ctxFor(t, device))
	a, _, err := s.sendTo(ctx, f, id, 0, half)
	cancel()
	if err != nil || a.GetCommittedOffset() != int64(half) {
		t.Fatalf("partial upload: %v, %v", a, err)
	}
	return half
}

func TestInitResumeAfterBrokenStream(t *testing.T) {
	st := newState(t)
	s := startServer(t, st)
	f := newFile(t, device, 6*chunkSize)
	id := s.init(f).GetUploadId()
	half := partial(t, s, f, id)

	res := s.init(f)
	if res.GetUploadId() != id || res.GetCommittedOffset() != int64(half) {
		t.Fatalf("Init after break = %v, want %s at %d", res, id, half)
	}
	if err := s.uploadRest(f, id, half); err != nil {
		t.Fatal(err)
	}
	u := st.row(t, id)
	if u.Status != db.StatusCompleted || u.ResumeCount != 1 {
		t.Fatalf("row = %+v", u)
	}
}

func TestRestartResume(t *testing.T) {
	st := newState(t)
	s := startServer(t, st)
	f := newFile(t, device, 6*chunkSize)
	id := s.init(f).GetUploadId()
	half := partial(t, s, f, id)

	// Drop the service and build a new one over the same DB and staging dir.
	s.gs.Stop()
	s2 := startServer(t, st)
	stRes, err := s2.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
	if err != nil || stRes.GetCommittedOffset() != int64(half) || stRes.GetSize() != int64(len(f.data)) || stRes.GetComplete() {
		t.Fatalf("Status after restart = %v, %v", stRes, err)
	}
	res := s2.init(f)
	if res.GetUploadId() != id || res.GetCommittedOffset() != int64(half) {
		t.Fatalf("Init after restart = %v", res)
	}
	if err := s2.uploadRest(f, id, half); err != nil {
		t.Fatal(err)
	}
	if u := st.row(t, id); u.Status != db.StatusCompleted {
		t.Fatalf("row = %+v", u)
	}
	if keys := st.objectKeys(t); !slices.Equal(keys, []string{meta.FinalKey(f.meta(t))}) {
		t.Fatalf("objects = %v", keys)
	}
}

func TestChecksumMismatch(t *testing.T) {
	st := newState(t)
	s := startServer(t, st)
	f := newFile(t, device, 2*chunkSize)
	bad := newFile(t, device, 2*chunkSize)
	f.req.Sha256 = bad.req.GetSha256() // declared sha256 of other bytes

	id := s.init(f).GetUploadId()
	err := s.uploadRest(f, id, 0)
	wantCode(t, err, codes.DataLoss)

	if u := st.row(t, id); u.Status != db.StatusFailedChecksum {
		t.Fatalf("status = %s, want failed_checksum", u.Status)
	}
	if keys := st.objectKeys(t); len(keys) != 0 {
		t.Fatalf("objects = %v, want none", keys)
	}
	if st.staged(t, id) {
		t.Fatal("staging data left behind")
	}
	_, err = s.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
	wantCode(t, err, codes.NotFound)
}

func TestDuplicateOfOtherProtocol(t *testing.T) {
	st := newState(t)
	s := startServer(t, st)
	f := newFile(t, device, chunkSize)
	ctx := context.Background()

	// Same file completed over tus.
	ref := uuid.NewString()
	m := f.meta(t)
	if _, err := st.repo.InsertUploading(ctx, "rest", ref, m); err != nil {
		t.Fatal(err)
	}
	if _, err := st.repo.Complete(ctx, "rest", ref, meta.FinalKey(m)); err != nil {
		t.Fatal(err)
	}

	_, err := s.client.Init(ctxFor(t, device), f.req)
	wantCode(t, err, codes.AlreadyExists)
	if got := st.rowsBySHA(t, f.req.GetSha256()); got[db.StatusCompleted] != 1 || len(got) != 1 {
		t.Fatalf("rows = %v", got)
	}
}

func TestDuplicateRace(t *testing.T) {
	st := newState(t)
	s := startServer(t, st)
	a := newFile(t, "dev-race-a", 4*chunkSize)
	b := fileOf("dev-race-b", a.data) // same bytes, other device → other key
	files := []file{a, b}

	ids := make([]string, len(files))
	streams := make([]stream, len(files))
	last := len(a.data) - chunkSize
	for i, f := range files {
		ids[i] = s.init(f).GetUploadId()
		ack, str, err := s.sendTo(ctxFor(t, f.req.GetDeviceId()), f, ids[i], 0, last)
		if err != nil || ack.GetCommittedOffset() != int64(last) {
			t.Fatalf("upload %d: %v, %v", i, ack, err)
		}
		streams[i] = str
	}

	// Send both final chunks at once.
	codesGot := make([]codes.Code, len(files))
	var wg sync.WaitGroup
	for i := range files {
		wg.Go(func() {
			str := streams[i]
			if err := str.Send(&uploadv1.Chunk{UploadId: ids[i], Offset: int64(last), Data: a.data[last:]}); err != nil {
				codesGot[i] = status.Code(err)
				return
			}
			ack, err := str.Recv()
			if err == nil && !ack.GetComplete() {
				codesGot[i] = codes.Unknown
				return
			}
			codesGot[i] = status.Code(err)
		})
	}
	wg.Wait()

	slices.Sort(codesGot)
	if want := []codes.Code{codes.OK, codes.AlreadyExists}; !slices.Equal(codesGot, want) {
		t.Fatalf("codes = %v, want %v", codesGot, want)
	}
	if got := st.rowsBySHA(t, a.req.GetSha256()); got[db.StatusCompleted] != 1 || got[db.StatusCancelled] != 1 {
		t.Fatalf("rows = %v, want one completed and one cancelled", got)
	}
	if keys := st.objectKeys(t); len(keys) != 1 {
		t.Fatalf("objects = %v, want exactly one", keys)
	}
	for _, id := range ids {
		if st.staged(t, id) {
			t.Fatalf("staging of %s left behind", id)
		}
	}
}
