package grpcapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/auth/authtest"
	"github.com/resyahrial/sensor-upload-server/internal/config"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
	"github.com/resyahrial/sensor-upload-server/internal/staging"
)

const (
	device   = "dev-0001"
	other    = "dev-0002"
	maxChunk = 4096
)

// testKeys is the signing key shared by every test env (a restarted env
// keeps accepting earlier tokens); token mints one token per device and
// keeps it.
var (
	testKeysOnce sync.Once
	testKeys     *authtest.Keys
	tokensMu     sync.Mutex
	tokens       = map[string]string{}
)

func keys(t testing.TB) *authtest.Keys {
	testKeysOnce.Do(func() { testKeys = authtest.New(t) })
	return testKeys
}

// fakeRepo is an in-memory Repo.
type fakeRepo struct {
	mu      sync.Mutex
	rows    map[string]Upload
	resumes map[string]int
	fail    map[string]error // method name → error to return
	// onCommit, if set, runs at the start of every UpdateCommitted with its
	// context (e.g. to stall the service's commit loop).
	onCommit func(ctx context.Context)
	// afterGet, if set, runs after Get has read a row and before it returns
	// it, so a test can change the row behind the caller's back.
	afterGet func(u Upload)
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{rows: map[string]Upload{}, resumes: map[string]int{}, fail: map[string]error{}}
}

func (r *fakeRepo) err(method string) error { return r.fail[method] }

func (r *fakeRepo) Insert(_ context.Context, u Upload) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.err("Insert"); err != nil {
		return err
	}
	u.Status, u.Committed = StatusUploading, 0
	r.rows[u.ID] = u
	return nil
}

func (r *fakeRepo) Get(_ context.Context, id string) (Upload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.err("Get"); err != nil {
		return Upload{}, err
	}
	u, ok := r.rows[id]
	if !ok {
		return Upload{}, fmt.Errorf("get %s: %w", id, ErrNotFound)
	}
	if r.afterGet != nil {
		r.mu.Unlock()
		r.afterGet(u)
		r.mu.Lock()
	}
	return u, nil
}

func (r *fakeRepo) CompletedExists(_ context.Context, sha string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.err("CompletedExists"); err != nil {
		return false, err
	}
	for _, u := range r.rows {
		if u.Meta.SHA256 == sha && u.Status == StatusCompleted {
			return true, nil
		}
	}
	return false, nil
}

func (r *fakeRepo) FindInProgress(_ context.Context, sha, dev string) (Upload, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.err("FindInProgress"); err != nil {
		return Upload{}, err
	}
	var best Upload
	found := false
	for _, u := range r.rows {
		if u.Meta.SHA256 == sha && u.Meta.DeviceID == dev && u.Status == StatusUploading &&
			(!found || u.ID > best.ID) { // UUIDv7 sorts by creation time
			best, found = u, true
		}
	}
	if !found {
		return Upload{}, ErrNotFound
	}
	return best, nil
}

func (r *fakeRepo) update(id string, f func(*Upload)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	u, ok := r.rows[id]
	if !ok {
		return ErrNotFound
	}
	f(&u)
	r.rows[id] = u
	return nil
}

func (r *fakeRepo) UpdateCommitted(ctx context.Context, id string, n int64) error {
	if r.onCommit != nil {
		r.onCommit(ctx)
	}
	if err := r.err("UpdateCommitted"); err != nil {
		return err
	}
	return r.update(id, func(u *Upload) { u.Committed = n })
}

func (r *fakeRepo) IncrementResumes(_ context.Context, id string) error {
	if err := r.err("IncrementResumes"); err != nil {
		return err
	}
	r.mu.Lock()
	r.resumes[id]++
	r.mu.Unlock()
	return nil
}

func (r *fakeRepo) MarkCancelled(_ context.Context, id string) error {
	if err := r.err("MarkCancelled"); err != nil {
		return err
	}
	// Like grpcadapt: a row that is no longer 'uploading' is ErrNotFound.
	r.mu.Lock()
	defer r.mu.Unlock()
	u, ok := r.rows[id]
	if !ok || u.Status != StatusUploading {
		return fmt.Errorf("mark cancelled %s: %w", id, ErrNotFound)
	}
	u.Status = StatusCancelled
	r.rows[id] = u
	return nil
}

func (r *fakeRepo) row(t *testing.T, id string) Upload {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	u, ok := r.rows[id]
	if !ok {
		t.Fatalf("no row %s", id)
	}
	return u
}

func (r *fakeRepo) resumeCount(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.resumes[id]
}

// fakeFinalizer verifies the SHA-256 and records the result in the repo,
// following the Finalizer contract.
type fakeFinalizer struct {
	repo *fakeRepo

	mu     sync.Mutex
	calls  int
	stored map[string][]byte // sha → data
	dup    bool              // report ErrDuplicate
	fail   error             // transient error
	// onRun, if set, runs at the start of every Run with its context.
	onRun func(ctx context.Context)
}

func (f *fakeFinalizer) Run(ctx context.Context, in FinalizeInput) error {
	if f.onRun != nil {
		f.onRun(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if in.Protocol != Protocol {
		return fmt.Errorf("protocol %q", in.Protocol)
	}
	if f.fail != nil {
		return f.fail
	}
	rc, err := in.Open()
	if err != nil {
		return err
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != in.Meta.SHA256 {
		_ = f.repo.update(in.Ref, func(u *Upload) { u.Status = StatusFailedChecksum })
		if err := in.Delete(); err != nil {
			return err
		}
		return fmt.Errorf("verify: %w", ErrChecksumMismatch)
	}
	if f.dup {
		return fmt.Errorf("record: %w", ErrDuplicate)
	}
	if f.stored == nil {
		f.stored = map[string][]byte{}
	}
	f.stored[in.Meta.SHA256] = data
	_ = f.repo.update(in.Ref, func(u *Upload) { u.Status = StatusCompleted })
	return in.Delete()
}

func (f *fakeFinalizer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeFinalizer) data(sha string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stored[sha]
}

// env is one running service over bufconn.
type env struct {
	t       *testing.T
	svc     *Service
	repo    *fakeRepo
	fin     *fakeFinalizer
	store   *staging.Store
	dir     string
	metrics *obs.Metrics
	lis     *bufconn.Listener
	client  uploadv1.UploadServiceClient // /grpc prefix
}

type envOpt func(*Config)

func newEnv(t *testing.T, opts ...envOpt) *env {
	return startEnv(t, newFakeRepo(), t.TempDir(), opts...)
}

// startEnv starts a service over an existing repo and staging dir, which
// simulates a server restart when called again with the same state.
func startEnv(t *testing.T, repo *fakeRepo, dir string, opts ...envOpt) *env {
	t.Helper()
	store, err := staging.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	verifier := keys(t).Verifier
	metrics, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		MaxChunkBytes: maxChunk,
		AckEveryN:     1,
		Rules:         meta.Rules{MaxFileBytes: 1 << 20, ContentTypes: config.DefaultContentTypes},
	}
	for _, o := range opts {
		o(&cfg)
	}
	fin := &fakeFinalizer{repo: repo}
	svc, err := NewService(cfg, Deps{
		Repo: repo, Finalizer: fin, Store: store, Verifier: verifier, Metrics: metrics,
		Logger: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	gs, err := NewServer(svc)
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	e := &env{t: t, svc: svc, repo: repo, fin: fin, store: store, dir: dir, metrics: metrics, lis: lis}
	e.client = uploadv1.NewUploadServiceClient(e.dial(true))
	return e
}

func (e *env) dial(prefix bool) *grpc.ClientConn {
	e.t.Helper()
	opts := []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return e.lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if prefix {
		opts = append(opts, grpcprefix.DialOptions("/grpc")...)
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func token(t *testing.T, dev string) string {
	t.Helper()
	tokensMu.Lock()
	defer tokensMu.Unlock()
	tok, ok := tokens[dev]
	if !ok {
		tok = keys(t).Token(t, dev)
		tokens[dev] = tok
	}
	return tok
}

// ctxFor returns a context authenticated as dev.
func ctxFor(t *testing.T, dev string) context.Context {
	return metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+token(t, dev))
}

// file is a test payload with its InitRequest.
type file struct {
	data []byte
	req  *uploadv1.InitRequest
}

func newFile(t *testing.T, size int) file {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return file{data: data, req: &uploadv1.InitRequest{
		DeviceId:    device,
		Sha256:      hex.EncodeToString(sum[:]),
		Size:        int64(size),
		Filename:    "a.flac",
		ContentType: "audio/flac",
		CapturedAt:  "2026-10-21T05:30:00+07:00",
		Priority:    2,
	}}
}

func (e *env) init(f file) *uploadv1.InitResponse {
	e.t.Helper()
	res, err := e.client.Init(ctxFor(e.t, device), f.req)
	if err != nil {
		e.t.Fatalf("Init: %v", err)
	}
	return res
}

func (e *env) stream(ctx context.Context) grpc.BidiStreamingClient[uploadv1.Chunk, uploadv1.Ack] {
	e.t.Helper()
	s, err := e.client.Upload(ctx)
	if err != nil {
		e.t.Fatalf("Upload: %v", err)
	}
	return s
}

// sendRange sends data[from:to] in maxChunk chunks without waiting for acks.
func sendRange(t *testing.T, s grpc.BidiStreamingClient[uploadv1.Chunk, uploadv1.Ack], id string, data []byte, from, to int) {
	t.Helper()
	for off := from; off < to; off += maxChunk {
		end := min(off+maxChunk, to)
		if err := s.Send(&uploadv1.Chunk{UploadId: id, Offset: int64(off), Data: data[off:end]}); err != nil {
			t.Fatalf("Send at %d: %v", off, err)
		}
	}
}

// recvAll reads acks until the stream ends and returns them with the final
// error (nil on a clean end).
func recvAll(s grpc.BidiStreamingClient[uploadv1.Chunk, uploadv1.Ack]) ([]*uploadv1.Ack, error) {
	var acks []*uploadv1.Ack
	for {
		a, err := s.Recv()
		if err == io.EOF {
			return acks, nil
		}
		if err != nil {
			return acks, err
		}
		acks = append(acks, a)
	}
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v (%v), want %v", got, err, want)
	}
}

// upload sends the whole file on one stream and returns the acks and error.
func (e *env) upload(f file, id string, from int) ([]*uploadv1.Ack, error) {
	e.t.Helper()
	s := e.stream(ctxFor(e.t, device))
	sendRange(e.t, s, id, f.data, from, len(f.data))
	if err := s.CloseSend(); err != nil {
		e.t.Fatal(err)
	}
	return recvAll(s)
}

func (e *env) stagedBytes(id string) []byte {
	e.t.Helper()
	r, err := e.store.Reader(id)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	if err != nil {
		e.t.Fatal(err)
	}
	return b
}

func (e *env) stagingExists(id string) bool {
	_, err := e.store.LoadInfo(id)
	return err == nil
}

func equalBytes(t *testing.T, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes differ: got %d bytes, want %d", len(got), len(want))
	}
}
