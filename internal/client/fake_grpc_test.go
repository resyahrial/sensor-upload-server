package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

// fakeGRPC is an in-memory UploadService registered only under the /grpc
// prefix, so a client without the prefix interceptor would get Unimplemented.
type fakeGRPC struct {
	uploadv1.UnimplementedUploadServiceServer

	maxChunk int32

	mu        sync.Mutex
	nextID    int
	uploads   map[string]*fakeGUpload // upload_id → upload
	bySHA     map[string]string       // in-progress sha → upload_id
	completed map[string]bool
	inits     int
	chunks    int
	received  int64 // payload bytes read from streams, including rejected chunks

	// onChunk runs before a chunk is applied (n = 1-based chunk count);
	// a non-nil error ends the stream with it.
	onChunk func(n int, u *fakeGUpload, c *uploadv1.Chunk) error
	// onInit can fail Init.
	onInit func(n int) error
	// corruptOnce fails the next completion with DATA_LOSS.
	corruptOnce bool
	// skipDedup makes Init ignore completed uploads (finalize race).
	skipDedup bool
	// ackEvery acks every n-th chunk of a stream and the final one, like
	// the server's GRPC_ACK_EVERY_N (0 = 1).
	ackEvery int
	// ackDelay is slept before each ack, so a client that does not wait
	// for acks visibly runs ahead.
	ackDelay time.Duration
	// maxUnacked is the largest number of bytes received on a stream past
	// the last ack sent on it (or past the stream's first offset).
	maxUnacked int64
}

type fakeGUpload struct {
	id   string
	sha  string
	size int64
	data []byte
}

func newFakeGRPC() *fakeGRPC {
	return &fakeGRPC{maxChunk: 1 << 20, uploads: map[string]*fakeGUpload{}, bySHA: map[string]string{}, completed: map[string]bool{}}
}

func checkAuth(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get("authorization"); len(v) != 1 || v[0] != "Bearer tok" {
		return status.Error(codes.Unauthenticated, "bad token")
	}
	return nil
}

func (f *fakeGRPC) Init(ctx context.Context, req *uploadv1.InitRequest) (*uploadv1.InitResponse, error) {
	if err := checkAuth(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inits++
	if f.onInit != nil {
		if err := f.onInit(f.inits); err != nil {
			return nil, err
		}
	}
	if req.GetDeviceId() != "dev-1" || len(req.GetSha256()) != 64 || req.GetPriority() < 1 {
		return nil, status.Error(codes.InvalidArgument, "bad metadata")
	}
	if f.completed[req.GetSha256()] && !f.skipDedup {
		return nil, status.Error(codes.AlreadyExists, "already exists")
	}
	if id, ok := f.bySHA[req.GetSha256()]; ok {
		u := f.uploads[id]
		return &uploadv1.InitResponse{UploadId: id, CommittedOffset: int64(len(u.data)), MaxChunkBytes: f.maxChunk}, nil
	}
	f.nextID++
	id := "u" + strconv.Itoa(f.nextID)
	f.uploads[id] = &fakeGUpload{id: id, sha: req.GetSha256(), size: req.GetSize()}
	f.bySHA[req.GetSha256()] = id
	return &uploadv1.InitResponse{UploadId: id, MaxChunkBytes: f.maxChunk}, nil
}

func (f *fakeGRPC) Status(ctx context.Context, req *uploadv1.StatusRequest) (*uploadv1.StatusResponse, error) {
	if err := checkAuth(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.uploads[req.GetUploadId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown")
	}
	return &uploadv1.StatusResponse{CommittedOffset: int64(len(u.data)), Size: u.size, Complete: f.completed[u.sha]}, nil
}

func (f *fakeGRPC) Upload(stream grpc.BidiStreamingServer[uploadv1.Chunk, uploadv1.Ack]) error {
	if err := checkAuth(stream.Context()); err != nil {
		return err
	}
	// A reader goroutine receives chunks as soon as the client sends them,
	// so maxUnacked measures what the client put in flight, independent of
	// how fast chunks are applied and acked. lastAck is guarded by f.mu.
	type recv struct {
		c   *uploadv1.Chunk
		err error
	}
	ctx := stream.Context()
	in := make(chan recv)
	lastAck := int64(-1)
	go func() {
		for {
			c, err := stream.Recv()
			if err == nil {
				f.mu.Lock()
				if lastAck < 0 {
					lastAck = c.GetOffset()
				}
				f.maxUnacked = max(f.maxUnacked, c.GetOffset()+int64(len(c.GetData()))-lastAck)
				f.mu.Unlock()
			}
			select {
			case in <- recv{c, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	every := max(f.ackEvery, 1)
	var pending *uploadv1.Ack // applied but not yet acked
	send := func(ack *uploadv1.Ack) error {
		time.Sleep(f.ackDelay)
		f.mu.Lock()
		lastAck = ack.GetCommittedOffset() // before Send: the client may answer at once
		f.mu.Unlock()
		pending = nil
		return stream.Send(ack)
	}
	for n := 1; ; n++ {
		r := <-in
		if errors.Is(r.err, io.EOF) {
			if pending != nil { // half-close: ack unacked progress, like the server
				return send(pending)
			}
			return nil
		}
		if r.err != nil {
			return r.err
		}
		ack, err := f.apply(r.c)
		if err != nil {
			return err
		}
		pending = ack
		if !ack.GetComplete() && n%every != 0 {
			continue
		}
		if err := send(ack); err != nil {
			return err
		}
		if ack.GetComplete() {
			return nil
		}
	}
}

func (f *fakeGRPC) apply(c *uploadv1.Chunk) (*uploadv1.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.uploads[c.GetUploadId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown")
	}
	f.chunks++
	f.received += int64(len(c.GetData()))
	if f.onChunk != nil {
		if err := f.onChunk(f.chunks, u, c); err != nil {
			return nil, err
		}
	}
	if c.GetOffset() != int64(len(u.data)) {
		return nil, status.Errorf(codes.FailedPrecondition, "expected offset %d", len(u.data))
	}
	if len(c.GetData()) > int(f.maxChunk) {
		return nil, status.Error(codes.InvalidArgument, "chunk too big")
	}
	u.data = append(u.data, c.GetData()...)
	if int64(len(u.data)) < u.size {
		return &uploadv1.Ack{CommittedOffset: int64(len(u.data))}, nil
	}
	sum := sha256.Sum256(u.data)
	if f.corruptOnce || hex.EncodeToString(sum[:]) != u.sha {
		f.corruptOnce = false
		delete(f.uploads, u.id)
		delete(f.bySHA, u.sha)
		return nil, status.Error(codes.DataLoss, "sha-256 mismatch")
	}
	f.completed[u.sha] = true
	delete(f.bySHA, u.sha)
	return &uploadv1.Ack{CommittedOffset: int64(len(u.data)), Complete: true}, nil
}

func (f *fakeGRPC) stored(sha string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.uploads {
		if u.sha == sha && f.completed[sha] {
			return append([]byte(nil), u.data...), true
		}
	}
	return nil, false
}

// startFakeGRPC serves f over bufconn and returns dial options for the client.
func startFakeGRPC(t *testing.T, f *fakeGRPC) []grpc.DialOption {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	if err := grpcprefix.RegisterWithPrefix(s, &uploadv1.UploadService_ServiceDesc, f, "/grpc"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}
