package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
)

func TestHappyPath(t *testing.T) {
	for _, prefix := range []bool{true, false} {
		t.Run(map[bool]string{true: "prefixed", false: "bare"}[prefix], func(t *testing.T) {
			e := newEnv(t)
			e.client = uploadv1.NewUploadServiceClient(e.dial(prefix))
			f := newFile(t, 3*maxChunk+100)
			res := e.init(f)
			if res.GetCommittedOffset() != 0 || res.GetMaxChunkBytes() != maxChunk {
				t.Fatalf("Init = %v", res)
			}
			if _, err := uuid.Parse(res.GetUploadId()); err != nil || uuid.MustParse(res.GetUploadId()).Version() != 7 {
				t.Fatalf("upload_id %q is not a UUIDv7", res.GetUploadId())
			}
			acks, err := e.upload(f, res.GetUploadId(), 0)
			if err != nil {
				t.Fatalf("stream: %v", err)
			}
			want := []int64{maxChunk, 2 * maxChunk, 3 * maxChunk, int64(len(f.data))}
			if len(acks) != len(want) {
				t.Fatalf("got %d acks, want %d", len(acks), len(want))
			}
			for i, a := range acks {
				if a.GetCommittedOffset() != want[i] || a.GetComplete() != (i == len(want)-1) {
					t.Fatalf("ack %d = %v", i, a)
				}
			}
			equalBytes(t, e.fin.data(f.req.GetSha256()), f.data)
			if u := e.repo.row(t, res.GetUploadId()); u.Status != StatusCompleted || u.Committed != int64(len(f.data)) {
				t.Fatalf("row = %+v", u)
			}
			if e.stagingExists(res.GetUploadId()) {
				t.Fatal("staging not deleted")
			}
			st, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: res.GetUploadId()})
			if err != nil || !st.GetComplete() || st.GetCommittedOffset() != int64(len(f.data)) || st.GetSize() != int64(len(f.data)) {
				t.Fatalf("Status = %v, %v", st, err)
			}
			if got := testutil.ToFloat64(e.metrics.Started.WithLabelValues(Protocol)); got != 1 {
				t.Fatalf("started = %v", got)
			}
			if got := testutil.ToFloat64(e.metrics.BytesReceived.WithLabelValues(Protocol)); got != float64(len(f.data)) {
				t.Fatalf("bytes = %v", got)
			}
			if got := testutil.ToFloat64(e.metrics.InProgress.WithLabelValues(Protocol)); got != 0 {
				t.Fatalf("in progress = %v", got)
			}
			if got := testutil.ToFloat64(e.metrics.GRPCStreamResets.WithLabelValues(Protocol)); got != 0 {
				t.Fatalf("resets = %v", got)
			}
		})
	}
}

func TestInitResumeIsIdempotent(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 4*maxChunk)
	res := e.init(f)
	// A fresh Init before any byte does not count as a resume.
	if again := e.init(f); again.GetUploadId() != res.GetUploadId() || again.GetCommittedOffset() != 0 {
		t.Fatalf("Init again = %v", again)
	}
	if n := e.repo.resumeCount(res.GetUploadId()); n != 0 {
		t.Fatalf("resumes = %d", n)
	}

	s := e.stream(ctxFor(t, device))
	sendRange(t, s, res.GetUploadId(), f.data, 0, 2*maxChunk)
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := recvAll(s); err != nil {
		t.Fatalf("partial stream: %v", err)
	}

	again := e.init(f)
	if again.GetUploadId() != res.GetUploadId() || again.GetCommittedOffset() != 2*maxChunk {
		t.Fatalf("resume Init = %v", again)
	}
	st, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: res.GetUploadId()})
	if err != nil || st.GetCommittedOffset() != 2*maxChunk || st.GetComplete() {
		t.Fatalf("Status = %v, %v", st, err)
	}
	if n := e.repo.resumeCount(res.GetUploadId()); n != 2 {
		t.Fatalf("resumes = %d, want 2", n)
	}
	if got := testutil.ToFloat64(e.metrics.Resumes.WithLabelValues(Protocol)); got != 2 {
		t.Fatalf("resume metric = %v", got)
	}

	acks, err := e.upload(f, res.GetUploadId(), 2*maxChunk)
	if err != nil || !acks[len(acks)-1].GetComplete() {
		t.Fatalf("resume stream: %v %v", acks, err)
	}
	equalBytes(t, e.fin.data(f.req.GetSha256()), f.data)
}

func TestInitValidation(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 10)
	tests := []struct {
		name  string
		edit  func(r *uploadv1.InitRequest)
		field string
	}{
		{"bad sha", func(r *uploadv1.InitRequest) { r.Sha256 = "XYZ" }, "sha256"},
		{"too large", func(r *uploadv1.InitRequest) { r.Size = 1<<20 + 1 }, "size"},
		{"zero size", func(r *uploadv1.InitRequest) { r.Size = 0 }, "size"},
		{"bad captured_at", func(r *uploadv1.InitRequest) { r.CapturedAt = "yesterday" }, "captured_at"},
		{"bad content type", func(r *uploadv1.InitRequest) { r.ContentType = "text/plain" }, "content_type"},
		{"bad device", func(r *uploadv1.InitRequest) { r.DeviceId = "X" }, "device_id"},
		{"bad priority", func(r *uploadv1.InitRequest) { r.Priority = 9 }, "priority"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := proto.Clone(f.req).(*uploadv1.InitRequest)
			tt.edit(r)
			_, err := e.client.Init(ctxFor(t, device), r)
			wantCode(t, err, codes.InvalidArgument)
			if msg := status.Convert(err).Message(); !strings.HasPrefix(msg, tt.field+":") {
				t.Fatalf("message %q does not name %s", msg, tt.field)
			}
		})
	}
}

func TestAuth(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 10)

	t.Run("missing token", func(t *testing.T) {
		_, err := e.client.Init(t.Context(), f.req)
		wantCode(t, err, codes.Unauthenticated)
		s, err := e.client.Upload(t.Context())
		if err == nil {
			_, err = s.Recv()
		}
		wantCode(t, err, codes.Unauthenticated)
	})
	t.Run("bad token", func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer nope")
		_, err := e.client.Status(ctx, &uploadv1.StatusRequest{UploadId: uuid.NewString()})
		wantCode(t, err, codes.Unauthenticated)
	})
	t.Run("device mismatch", func(t *testing.T) {
		_, err := e.client.Init(ctxFor(t, other), f.req)
		wantCode(t, err, codes.PermissionDenied)
	})
}

func TestNotFound(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 2*maxChunk)
	res := e.init(f)
	id := res.GetUploadId()

	cases := map[string]struct {
		ctxDev string
		id     string
	}{
		"unknown":      {device, uuid.Must(uuid.NewV7()).String()},
		"not a uuid":   {device, "../../etc/passwd"},
		"other device": {other, id},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := ctxFor(t, c.ctxDev)
			_, err := e.client.Status(ctx, &uploadv1.StatusRequest{UploadId: c.id})
			wantCode(t, err, codes.NotFound)
			_, err = e.client.Cancel(ctx, &uploadv1.CancelRequest{UploadId: c.id})
			wantCode(t, err, codes.NotFound)
			s := e.stream(ctx)
			sendRange(t, s, c.id, f.data, 0, maxChunk)
			_, err = recvAll(s)
			wantCode(t, err, codes.NotFound)
		})
	}
	t.Run("empty id", func(t *testing.T) {
		_, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{})
		wantCode(t, err, codes.InvalidArgument)
		s := e.stream(ctxFor(t, device))
		if err := s.Send(&uploadv1.Chunk{Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		_, err = recvAll(s)
		wantCode(t, err, codes.InvalidArgument)
	})
	t.Run("staging missing", func(t *testing.T) {
		if err := e.store.Delete(id); err != nil {
			t.Fatal(err)
		}
		s := e.stream(ctxFor(t, device))
		sendRange(t, s, id, f.data, 0, maxChunk)
		_, err := recvAll(s)
		wantCode(t, err, codes.NotFound)
		// Init no longer resumes the broken upload but starts a new one.
		if again := e.init(f); again.GetUploadId() == id {
			t.Fatal("Init resumed an upload without staging data")
		}
	})
}

func TestChunkValidation(t *testing.T) {
	e := newEnv(t)
	tests := []struct {
		name  string
		chunk func(id string, data []byte) *uploadv1.Chunk
		code  codes.Code
		msg   string
	}{
		{"offset mismatch", func(id string, d []byte) *uploadv1.Chunk {
			return &uploadv1.Chunk{UploadId: id, Offset: 5, Data: d[5:10]}
		}, codes.FailedPrecondition, "expected offset 0"},
		{"oversize chunk", func(id string, _ []byte) *uploadv1.Chunk {
			return &uploadv1.Chunk{UploadId: id, Data: make([]byte, maxChunk+1)}
		}, codes.InvalidArgument, "max_chunk_bytes"},
		{"empty chunk", func(id string, _ []byte) *uploadv1.Chunk {
			return &uploadv1.Chunk{UploadId: id}
		}, codes.InvalidArgument, "empty"},
		{"beyond size", func(id string, d []byte) *uploadv1.Chunk {
			return &uploadv1.Chunk{UploadId: id, Data: append(d, 'x')}
		}, codes.InvalidArgument, "beyond the upload size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFile(t, 100)
			id := e.init(f).GetUploadId()
			s := e.stream(ctxFor(t, device))
			if err := s.Send(tt.chunk(id, f.data)); err != nil {
				t.Fatal(err)
			}
			_, err := recvAll(s)
			wantCode(t, err, tt.code)
			if !strings.Contains(status.Convert(err).Message(), tt.msg) {
				t.Fatalf("message %q lacks %q", status.Convert(err).Message(), tt.msg)
			}
			if u := e.repo.row(t, id); u.Committed != 0 {
				t.Fatalf("committed = %d after a rejected chunk", u.Committed)
			}
		})
	}
	if got := testutil.ToFloat64(e.metrics.GRPCStreamResets.WithLabelValues(Protocol)); got != float64(len(tests)) {
		t.Fatalf("resets = %v", got)
	}
}

func TestMixedUploadIDs(t *testing.T) {
	e := newEnv(t)
	a, b := newFile(t, 2*maxChunk), newFile(t, 2*maxChunk)
	idA, idB := e.init(a).GetUploadId(), e.init(b).GetUploadId()
	s := e.stream(ctxFor(t, device))
	sendRange(t, s, idA, a.data, 0, maxChunk)
	sendRange(t, s, idB, b.data, 0, maxChunk)
	_, err := recvAll(s)
	wantCode(t, err, codes.InvalidArgument)
	if !strings.Contains(status.Convert(err).Message(), "upload_id") {
		t.Fatalf("message %q", status.Convert(err).Message())
	}
}

func TestSecondStreamAborted(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 3*maxChunk)
	id := e.init(f).GetUploadId()

	s1 := e.stream(ctxFor(t, device))
	sendRange(t, s1, id, f.data, 0, maxChunk)
	if a, err := s1.Recv(); err != nil || a.GetCommittedOffset() != maxChunk {
		t.Fatalf("first ack = %v, %v", a, err)
	}
	if !e.svc.Active(id) {
		t.Fatal("Active = false during stream")
	}

	s2 := e.stream(ctxFor(t, device))
	sendRange(t, s2, id, f.data, maxChunk, 2*maxChunk)
	_, err := recvAll(s2)
	wantCode(t, err, codes.Aborted)

	_, err = e.client.Cancel(ctxFor(t, device), &uploadv1.CancelRequest{UploadId: id})
	wantCode(t, err, codes.Aborted)
	if _, ok := e.svc.Acquire(id); ok {
		t.Fatal("Acquire succeeded during stream")
	}

	// The first stream is unaffected and completes.
	sendRange(t, s1, id, f.data, maxChunk, len(f.data))
	if err := s1.CloseSend(); err != nil {
		t.Fatal(err)
	}
	acks, err := recvAll(s1)
	if err != nil || !acks[len(acks)-1].GetComplete() {
		t.Fatalf("first stream: %v %v", acks, err)
	}
	if e.svc.Active(id) {
		t.Fatal("Active = true after stream end")
	}
}

func TestChecksumMismatch(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 2*maxChunk)
	f.req.Sha256 = strings.Repeat("0", 64)
	id := e.init(f).GetUploadId()
	_, err := e.upload(f, id, 0)
	wantCode(t, err, codes.DataLoss)
	if u := e.repo.row(t, id); u.Status != StatusFailedChecksum {
		t.Fatalf("status = %s", u.Status)
	}
	if e.stagingExists(id) {
		t.Fatal("staging not deleted")
	}
	_, err = e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
	wantCode(t, err, codes.NotFound)
	// A new Init starts over.
	if again := e.init(f); again.GetUploadId() == id || again.GetCommittedOffset() != 0 {
		t.Fatalf("Init after DATA_LOSS = %v", again)
	}
}

func TestAlreadyExists(t *testing.T) {
	t.Run("init after completion", func(t *testing.T) {
		e := newEnv(t)
		f := newFile(t, 100)
		id := e.init(f).GetUploadId()
		if _, err := e.upload(f, id, 0); err != nil {
			t.Fatal(err)
		}
		_, err := e.client.Init(ctxFor(t, device), f.req)
		wantCode(t, err, codes.AlreadyExists)
		// A stream on the completed upload is ALREADY_EXISTS too.
		_, err = e.upload(f, id, 0)
		wantCode(t, err, codes.AlreadyExists)
	})
	t.Run("finalize race", func(t *testing.T) {
		e := newEnv(t)
		e.fin.dup = true
		f := newFile(t, 100)
		id := e.init(f).GetUploadId()
		_, err := e.upload(f, id, 0)
		wantCode(t, err, codes.AlreadyExists)
	})
}

func TestCancel(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 3*maxChunk)
	id := e.init(f).GetUploadId()
	s := e.stream(ctxFor(t, device))
	sendRange(t, s, id, f.data, 0, maxChunk)
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := recvAll(s); err != nil {
		t.Fatal(err)
	}

	if _, err := e.client.Cancel(ctxFor(t, device), &uploadv1.CancelRequest{UploadId: id}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if u := e.repo.row(t, id); u.Status != StatusCancelled {
		t.Fatalf("status = %s", u.Status)
	}
	if e.stagingExists(id) {
		t.Fatal("staging not deleted")
	}
	if _, err := os.Stat(filepath.Join(e.dir, id+".bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".bin still present: %v", err)
	}
	_, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
	wantCode(t, err, codes.NotFound)
	_, err = e.client.Cancel(ctxFor(t, device), &uploadv1.CancelRequest{UploadId: id})
	wantCode(t, err, codes.NotFound)
	if got := testutil.ToFloat64(e.metrics.Failed.WithLabelValues(Protocol, "cancelled")); got != 1 {
		t.Fatalf("cancelled metric = %v", got)
	}
}

func TestAckEveryN(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.AckEveryN = 3 })

	f := newFile(t, 7*maxChunk)
	id := e.init(f).GetUploadId()
	acks, err := e.upload(f, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int64, len(acks))
	for i, a := range acks {
		got[i] = a.GetCommittedOffset()
	}
	want := []int64{3 * maxChunk, 6 * maxChunk, 7 * maxChunk}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || !acks[2].GetComplete() {
		t.Fatalf("acks = %v, want %v (last complete)", got, want)
	}

	// Half-closing mid-upload acks the unacked progress.
	g := newFile(t, 10*maxChunk)
	id = e.init(g).GetUploadId()
	s := e.stream(ctxFor(t, device))
	sendRange(t, s, id, g.data, 0, 4*maxChunk)
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	acks, err = recvAll(s)
	if err != nil || len(acks) != 2 || acks[0].GetCommittedOffset() != 3*maxChunk || acks[1].GetCommittedOffset() != 4*maxChunk {
		t.Fatalf("acks = %v, %v", acks, err)
	}
}

func TestRestartResumesFromDBOffset(t *testing.T) {
	repo, dir := newFakeRepo(), t.TempDir()
	e1 := startEnv(t, repo, dir)
	f := newFile(t, 5*maxChunk+7)
	id := e1.init(f).GetUploadId()

	s := e1.stream(ctxFor(t, device))
	sendRange(t, s, id, f.data, 0, 2*maxChunk)
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := recvAll(s); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after a write+fsync but before the DB update: the
	// .bin holds unacked garbage past the committed offset.
	bin, err := os.OpenFile(filepath.Join(dir, id+".bin"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bin.Write([]byte("garbage-not-acked")); err != nil {
		t.Fatal(err)
	}
	if err := bin.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := startEnv(t, repo, dir) // new Service over the same state
	res := e2.init(f)
	if res.GetUploadId() != id || res.GetCommittedOffset() != 2*maxChunk {
		t.Fatalf("Init after restart = %v", res)
	}
	s = e2.stream(ctxFor(t, device))
	sendRange(t, s, id, f.data, 2*maxChunk, 3*maxChunk)
	if a, err := s.Recv(); err != nil || a.GetCommittedOffset() != 3*maxChunk {
		t.Fatalf("ack after restart = %v, %v", a, err)
	}
	// The garbage was truncated away before the new chunk was written.
	equalBytes(t, e2.stagedBytes(id), f.data[:3*maxChunk])
	sendRange(t, s, id, f.data, 3*maxChunk, len(f.data))
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	acks, err := recvAll(s)
	if err != nil || !acks[len(acks)-1].GetComplete() {
		t.Fatalf("stream after restart: %v %v", acks, err)
	}
	equalBytes(t, e2.fin.data(f.req.GetSha256()), f.data)
}

// fullyCommitted leaves an upload with every byte committed but not
// finalized, as after a crash between the last commit and finalize.
func fullyCommitted(t *testing.T, e *env) (file, string) {
	t.Helper()
	f := newFile(t, 2*maxChunk)
	id := e.init(f).GetUploadId()
	e.fin.fail = errors.New("object store down")
	_, err := e.upload(f, id, 0)
	wantCode(t, err, codes.Internal)
	e.fin.fail = nil
	if u := e.repo.row(t, id); u.Status != StatusUploading || u.Committed != int64(len(f.data)) {
		t.Fatalf("row = %+v", u)
	}
	return f, id
}

func TestFinalizeRetry(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		e := newEnv(t)
		f, id := fullyCommitted(t, e)
		_, err := e.client.Init(ctxFor(t, device), f.req)
		wantCode(t, err, codes.AlreadyExists)
		if u := e.repo.row(t, id); u.Status != StatusCompleted {
			t.Fatalf("status = %s", u.Status)
		}
	})
	t.Run("status", func(t *testing.T) {
		e := newEnv(t)
		f, id := fullyCommitted(t, e)
		st, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
		if err != nil || !st.GetComplete() || st.GetCommittedOffset() != int64(len(f.data)) {
			t.Fatalf("Status = %v, %v", st, err)
		}
	})
	t.Run("status error", func(t *testing.T) {
		e := newEnv(t)
		_, id := fullyCommitted(t, e)
		e.fin.dup = true
		_, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
		wantCode(t, err, codes.AlreadyExists)
	})
	t.Run("status inactive", func(t *testing.T) {
		e := newEnv(t)
		_, id := fullyCommitted(t, e)
		e.fin.fail = fmt.Errorf("expired meanwhile: %w", ErrNotFound)
		_, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
		wantCode(t, err, codes.NotFound)
	})
	t.Run("init error", func(t *testing.T) {
		e := newEnv(t)
		f, _ := fullyCommitted(t, e)
		e.fin.fail = errors.New("still down")
		_, err := e.client.Init(ctxFor(t, device), f.req)
		wantCode(t, err, codes.Internal)
	})
	t.Run("stream", func(t *testing.T) {
		e := newEnv(t)
		f, id := fullyCommitted(t, e)
		s := e.stream(ctxFor(t, device))
		if err := s.Send(&uploadv1.Chunk{UploadId: id, Offset: int64(len(f.data))}); err != nil {
			t.Fatal(err)
		}
		acks, err := recvAll(s)
		if err != nil || len(acks) != 1 || !acks[0].GetComplete() {
			t.Fatalf("acks = %v, %v", acks, err)
		}
	})
	t.Run("busy", func(t *testing.T) {
		e := newEnv(t)
		f, id := fullyCommitted(t, e)
		release, ok := e.svc.Acquire(id)
		if !ok {
			t.Fatal("Acquire failed")
		}
		defer release()
		res, err := e.client.Init(ctxFor(t, device), f.req)
		if err != nil || res.GetCommittedOffset() != int64(len(f.data)) {
			t.Fatalf("Init = %v, %v", res, err)
		}
		st, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
		if err != nil || st.GetComplete() {
			t.Fatalf("Status = %v, %v", st, err)
		}
		if n := e.fin.callCount(); n != 1 {
			t.Fatalf("finalizer calls = %d, want 1", n)
		}
	})
}

func TestIdleTimeout(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.IdleTimeout = 100 * time.Millisecond })
	f := newFile(t, 2*maxChunk)
	id := e.init(f).GetUploadId()
	s := e.stream(ctxFor(t, device))
	sendRange(t, s, id, f.data, 0, maxChunk)
	acks, err := recvAll(s)
	wantCode(t, err, codes.DeadlineExceeded)
	if len(acks) != 1 {
		t.Fatalf("acks = %v", acks)
	}
	if e.svc.Active(id) {
		t.Fatal("upload still active after idle timeout")
	}
}

func TestShutdown(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 2*maxChunk)
	id := e.init(f).GetUploadId()
	s := e.stream(ctxFor(t, device))
	sendRange(t, s, id, f.data, 0, maxChunk)
	if _, err := s.Recv(); err != nil {
		t.Fatal(err)
	}
	e.svc.Shutdown()
	e.svc.Shutdown() // idempotent
	_, err := recvAll(s)
	wantCode(t, err, codes.Unavailable)
	_, err = e.client.Init(ctxFor(t, device), f.req)
	wantCode(t, err, codes.Unavailable)
	// The committed chunk survived.
	if u := e.repo.row(t, id); u.Committed != maxChunk {
		t.Fatalf("committed = %d", u.Committed)
	}
}

func TestDrain(t *testing.T) {
	e := newEnv(t)
	release, ok := e.svc.Acquire("held")
	if !ok {
		t.Fatal("acquire failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := e.svc.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain while held = %v, want deadline exceeded", err)
	}
	go func() { time.Sleep(20 * time.Millisecond); release() }()
	if err := e.svc.Drain(context.Background()); err != nil {
		t.Fatalf("Drain after release = %v", err)
	}
}

func TestClientCancelsStream(t *testing.T) {
	e := newEnv(t)
	f := newFile(t, 2*maxChunk)
	id := e.init(f).GetUploadId()
	ctx, cancel := context.WithCancel(ctxFor(t, device))
	s := e.stream(ctx)
	sendRange(t, s, id, f.data, 0, maxChunk)
	if _, err := s.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for e.svc.Active(id) {
		if time.Now().After(deadline) {
			t.Fatal("stream still active after client cancel")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := testutil.ToFloat64(e.metrics.GRPCStreamResets.WithLabelValues(Protocol)); got != 1 {
		t.Fatalf("resets = %v", got)
	}
}

func TestInternalErrors(t *testing.T) {
	boom := errors.New("db down")
	f := newFile(t, 2*maxChunk)
	for _, method := range []string{"CompletedExists", "FindInProgress", "Insert"} {
		t.Run(method, func(t *testing.T) {
			e := newEnv(t)
			e.repo.fail[method] = boom
			_, err := e.client.Init(ctxFor(t, device), f.req)
			wantCode(t, err, codes.Internal)
			if msg := status.Convert(err).Message(); strings.Contains(msg, "db down") {
				t.Fatalf("internal detail leaked: %q", msg)
			}
		})
	}
	t.Run("Get", func(t *testing.T) {
		e := newEnv(t)
		id := e.init(f).GetUploadId()
		e.repo.fail["Get"] = boom
		_, err := e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
		wantCode(t, err, codes.Internal)
	})
	t.Run("UpdateCommitted", func(t *testing.T) {
		e := newEnv(t)
		id := e.init(f).GetUploadId()
		e.repo.fail["UpdateCommitted"] = boom
		_, err := e.upload(f, id, 0)
		wantCode(t, err, codes.Internal)
	})
	t.Run("IncrementResumes", func(t *testing.T) {
		e := newEnv(t)
		id := e.init(f).GetUploadId()
		s := e.stream(ctxFor(t, device))
		sendRange(t, s, id, f.data, 0, maxChunk)
		_ = s.CloseSend()
		if _, err := recvAll(s); err != nil {
			t.Fatal(err)
		}
		e.repo.fail["IncrementResumes"] = boom
		_, err := e.client.Init(ctxFor(t, device), f.req)
		wantCode(t, err, codes.Internal)
		_, err = e.client.Status(ctxFor(t, device), &uploadv1.StatusRequest{UploadId: id})
		wantCode(t, err, codes.Internal)
	})
	t.Run("MarkCancelled", func(t *testing.T) {
		e := newEnv(t)
		id := e.init(f).GetUploadId()
		e.repo.fail["MarkCancelled"] = boom
		_, err := e.client.Cancel(ctxFor(t, device), &uploadv1.CancelRequest{UploadId: id})
		wantCode(t, err, codes.Internal)
		// Retrying after the failure succeeds.
		delete(e.repo.fail, "MarkCancelled")
		if _, err := e.client.Cancel(ctxFor(t, device), &uploadv1.CancelRequest{UploadId: id}); err != nil {
			t.Fatalf("retry Cancel: %v", err)
		}
	})
	t.Run("short staged file", func(t *testing.T) {
		e := newEnv(t)
		id := e.init(f).GetUploadId()
		if err := e.repo.UpdateCommitted(t.Context(), id, maxChunk); err != nil {
			t.Fatal(err)
		}
		s := e.stream(ctxFor(t, device))
		sendRange(t, s, id, f.data, maxChunk, 2*maxChunk)
		_, err := recvAll(s)
		wantCode(t, err, codes.Internal)
	})
}

func TestNewServiceValidation(t *testing.T) {
	e := newEnv(t)
	d := Deps{Repo: e.repo, Finalizer: e.fin, Store: e.store, Verifier: e.svc.verifier, Metrics: e.metrics, Logger: e.svc.log}
	for name, tc := range map[string]struct {
		cfg Config
		d   Deps
	}{
		"chunk":   {Config{AckEveryN: 1}, d},
		"ack":     {Config{MaxChunkBytes: 1}, d},
		"no repo": {Config{MaxChunkBytes: 1, AckEveryN: 1}, Deps{}},
	} {
		if _, err := NewService(tc.cfg, tc.d); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	svc, err := NewService(Config{MaxChunkBytes: 1, AckEveryN: 1}, d)
	if err != nil || svc.cfg.IdleTimeout != DefaultIdleTimeout {
		t.Fatalf("defaults: %v %v", svc, err)
	}
}

func TestStreamDeadlines(t *testing.T) {
	t.Run("finalize", func(t *testing.T) {
		e := newEnv(t)
		f := newFile(t, 2*maxChunk)
		id := e.init(f).GetUploadId()
		ctx, cancel := context.WithCancel(ctxFor(t, device))
		defer cancel()
		type seen struct {
			deadline time.Time
			ok       bool
			err      error
		}
		got := make(chan seen, 1)
		e.fin.onRun = func(fctx context.Context) {
			// The client goes away while the finalizer runs.
			cancel()
			select {
			case <-fctx.Done():
			case <-time.After(200 * time.Millisecond):
			}
			d, ok := fctx.Deadline()
			got <- seen{d, ok, fctx.Err()}
		}
		s := e.stream(ctx)
		sendRange(t, s, id, f.data, 0, len(f.data))
		_, _ = recvAll(s)
		r := <-got
		if r.err != nil {
			t.Fatalf("finalizer context ended with the client: %v", r.err)
		}
		if left := time.Until(r.deadline); !r.ok || left <= finalizeTimeout-time.Minute || left > finalizeTimeout {
			t.Fatalf("finalizer deadline = %v (ok=%v), want ~%v from now", r.deadline, r.ok, finalizeTimeout)
		}
		deadline := time.Now().Add(5 * time.Second)
		for e.repo.row(t, id).Status != StatusCompleted {
			if time.Now().After(deadline) {
				t.Fatalf("status = %s, want completed", e.repo.row(t, id).Status)
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	t.Run("commit", func(t *testing.T) {
		e := newEnv(t)
		f := newFile(t, 2*maxChunk)
		id := e.init(f).GetUploadId()
		var mu sync.Mutex
		var lefts []time.Duration
		e.repo.onCommit = func(ctx context.Context) {
			left := time.Duration(-1)
			if d, ok := ctx.Deadline(); ok {
				left = time.Until(d)
			}
			mu.Lock()
			lefts = append(lefts, left)
			mu.Unlock()
		}
		if _, err := e.upload(f, id, 0); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(lefts) != 2 {
			t.Fatalf("commits = %d, want 2", len(lefts))
		}
		for _, left := range lefts {
			if left <= 0 || left > commitTimeout {
				t.Fatalf("UpdateCommitted deadline in %v, want within %v", left, commitTimeout)
			}
		}
	})
}

func TestCancelRace(t *testing.T) {
	t.Run("completed meanwhile", func(t *testing.T) {
		e := newEnv(t)
		f := newFile(t, 2*maxChunk)
		id := e.init(f).GetUploadId()
		// A stream finishes the upload between Cancel's lookup and its
		// acquire of the upload.
		var once sync.Once
		e.repo.afterGet = func(u Upload) {
			once.Do(func() { _ = e.repo.update(u.ID, func(u *Upload) { u.Status = StatusCompleted }) })
		}
		_, err := e.client.Cancel(ctxFor(t, device), &uploadv1.CancelRequest{UploadId: id})
		wantCode(t, err, codes.NotFound)
		if u := e.repo.row(t, id); u.Status != StatusCompleted {
			t.Fatalf("status = %s, want completed", u.Status)
		}
		if !e.stagingExists(id) {
			t.Fatal("Cancel deleted the staging of an upload it did not cancel")
		}
	})
	t.Run("not uploading at MarkCancelled", func(t *testing.T) {
		e := newEnv(t)
		f := newFile(t, 2*maxChunk)
		id := e.init(f).GetUploadId()
		e.repo.fail["MarkCancelled"] = fmt.Errorf("expired meanwhile: %w", ErrNotFound)
		_, err := e.client.Cancel(ctxFor(t, device), &uploadv1.CancelRequest{UploadId: id})
		wantCode(t, err, codes.NotFound)
	})
}
