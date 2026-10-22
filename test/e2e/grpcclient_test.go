//go:build integration

package e2e

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

// grpcMode is one way of reaching the gRPC service.
type grpcMode struct {
	name   string
	addr   func(*Server) string
	prefix bool // call /grpc/upload.v1.UploadService/… (contract client)
}

var grpcModes = []grpcMode{
	{"tls-listener-prefix", func(s *Server) string { return "127.0.0.1:" + s.port }, true},
	{"native-prefix", (*Server).NativeAddr, true},
	{"native-bare", (*Server).NativeAddr, false},
}

// GRPC is a raw gRPC client for one device.
type GRPC struct {
	t     *testing.T
	C     uploadv1.UploadServiceClient
	Token string
}

// DialGRPC connects to s in the given mode with a token for device.
func DialGRPC(t *testing.T, s *Server, m grpcMode, device string) *GRPC {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(TLSConfig()))}
	if m.prefix {
		opts = append(opts, grpcprefix.DialOptions("/grpc")...)
	}
	conn, err := grpc.NewClient(m.addr(s), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &GRPC{t: t, C: uploadv1.NewUploadServiceClient(conn), Token: Token(t, device)}
}

// Ctx returns a call context carrying the token (none if empty).
func (g *GRPC) Ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	g.t.Cleanup(cancel)
	if g.Token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+g.Token)
}

// InitReq returns the InitRequest for f; sha overrides the digest when
// non-empty.
func InitReq(f File, sha string) *uploadv1.InitRequest {
	if sha == "" {
		sha = f.SHA256
	}
	return &uploadv1.InitRequest{
		DeviceId: f.Device, Sha256: sha, Size: int64(len(f.Data)), Filename: "e2e.flac",
		ContentType: "audio/flac", CapturedAt: "2026-10-21T05:30:00+07:00", Priority: 2,
	}
}

// MustInit calls Init and fails the test on an error.
func (g *GRPC) MustInit(f File) *uploadv1.InitResponse {
	g.t.Helper()
	res, err := g.C.Init(g.Ctx(), InitReq(f, ""))
	if err != nil {
		g.t.Fatalf("init: %v", err)
	}
	return res
}

// Stream opens an Upload stream with ctx (use Ctx() for the token).
func (g *GRPC) Stream(ctx context.Context) grpc.BidiStreamingClient[uploadv1.Chunk, uploadv1.Ack] {
	g.t.Helper()
	s, err := g.C.Upload(ctx)
	if err != nil {
		g.t.Fatalf("open stream: %v", err)
	}
	return s
}

// SendRange sends data[from:to] in chunks of n bytes.
func SendRange(t *testing.T, s grpc.BidiStreamingClient[uploadv1.Chunk, uploadv1.Ack], id string, data []byte, from, to, n int) {
	t.Helper()
	for off := from; off < to; off += n {
		end := min(off+n, to)
		if err := s.Send(&uploadv1.Chunk{UploadId: id, Offset: int64(off), Data: data[off:end]}); err != nil {
			t.Fatalf("send at %d: %v", off, err)
		}
	}
}

// RecvAll reads acks until the stream ends; err is nil after a complete
// ack or a clean end.
func RecvAll(s grpc.BidiStreamingClient[uploadv1.Chunk, uploadv1.Ack]) ([]*uploadv1.Ack, error) {
	var acks []*uploadv1.Ack
	for {
		a, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return acks, nil
		}
		if err != nil {
			return acks, err
		}
		acks = append(acks, a)
		if a.GetComplete() {
			return acks, nil
		}
	}
}

// UploadFrom streams f from off to the end and returns the stream's
// result. It retries the stream open while the previous stream of the
// upload still holds it (ABORTED).
func (g *GRPC) UploadFrom(id string, f File, off int) ([]*uploadv1.Ack, error) {
	g.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := g.Stream(g.Ctx())
		end := min(off+chunk, len(f.Data))
		if err := s.Send(&uploadv1.Chunk{UploadId: id, Offset: int64(off), Data: f.Data[off:end]}); err != nil {
			g.t.Fatalf("send at %d: %v", off, err)
		}
		first, err := s.Recv()
		if status.Code(err) == codes.Aborted && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err != nil {
			return nil, err
		}
		acks := []*uploadv1.Ack{first}
		if first.GetComplete() {
			return acks, nil
		}
		SendRange(g.t, s, id, f.Data, end, len(f.Data), chunk)
		if err := s.CloseSend(); err != nil {
			g.t.Fatal(err)
		}
		rest, err := RecvAll(s)
		return append(acks, rest...), err
	}
}

// MustUpload streams f from off and requires Ack{complete}.
func (g *GRPC) MustUpload(id string, f File, off int) {
	g.t.Helper()
	acks, err := g.UploadFrom(id, f, off)
	if err != nil {
		g.t.Fatalf("upload from %d: %v", off, err)
	}
	if last := acks[len(acks)-1]; !last.GetComplete() || last.GetCommittedOffset() != int64(len(f.Data)) {
		g.t.Fatalf("last ack = %v", last)
	}
}

// WantCode fails unless err has gRPC status code want.
func WantCode(t *testing.T, what string, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Errorf("%s: code %s (%v), want %s", what, got, err, want)
	}
}

// AssertGRPCStored checks that the gRPC upload id of f is completed in the
// DB and in the object store, with its staging files gone.
func AssertGRPCStored(t *testing.T, s *Server, id string, f File) {
	t.Helper()
	u := Row(t, s, "grpc", id)
	if u.Status != db.StatusCompleted || u.CommittedBytes != int64(len(f.Data)) {
		t.Fatalf("row status=%s committed=%d", u.Status, u.CommittedBytes)
	}
	if u.StorageKey != f.FinalKey(t) {
		t.Errorf("storage_key = %q, want %q", u.StorageKey, f.FinalKey(t))
	}
	got, err := os.ReadFile(filepath.Join(s.env["OBJSTORE_LOCAL_DIR"], filepath.FromSlash(u.StorageKey)))
	if err != nil || !bytes.Equal(got, f.Data) {
		t.Errorf("stored object differs (err %v, %d bytes)", err, len(got))
	}
	AssertGRPCNotStaged(t, s, id)
}

// AssertGRPCNotStaged checks that the gRPC staging files of id are gone.
func AssertGRPCNotStaged(t *testing.T, s *Server, id string) {
	t.Helper()
	for _, ext := range []string{".bin", ".json"} {
		if _, err := os.Stat(filepath.Join(s.GRPCDir(), id+ext)); !os.IsNotExist(err) {
			t.Errorf("staging file %s%s still present (%v)", id, ext, err)
		}
	}
}

// Objects counts the stored objects of device.
func Objects(t *testing.T, s *Server, device string) int {
	t.Helper()
	var n int
	root := filepath.Join(s.env["OBJSTORE_LOCAL_DIR"], device)
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			n++
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return n
}

// CompletedRows counts the completed rows (any protocol) of sha.
func CompletedRows(t *testing.T, s *Server, sha string) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM uploads WHERE sha256 = $1 AND status = 'completed'`, sha).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
