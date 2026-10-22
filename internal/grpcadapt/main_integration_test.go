//go:build integration

package grpcadapt

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/config"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/finalize"
	"github.com/resyahrial/sensor-upload-server/internal/grpcapi"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/objstore"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
	"github.com/resyahrial/sensor-upload-server/internal/staging"
)

const (
	postgresImage = "docker.io/library/postgres:16-alpine"
	chunkSize     = 4096
)

var (
	testDSN string
	keys    = auth.NewMemoryStore()
)

func TestMain(m *testing.M) { os.Exit(runWithPostgres(m)) }

func runWithPostgres(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("uploads"), postgres.WithUsername("test"), postgres.WithPassword("test"),
		postgres.BasicWaitStrategies())
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintln(os.Stderr, "terminate postgres:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	if testDSN, err = ctr.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintln(os.Stderr, "postgres dsn:", err)
		return 1
	}
	return m.Run()
}

// state is what survives a server restart: the database, the staging
// directory and the object store.
type state struct {
	pool    *pgxpool.Pool
	repo    *db.Repo
	objects *objstore.Local
	objDir  string
	stage   string
}

func newState(t *testing.T) *state {
	t.Helper()
	ctx := context.Background()
	pool, err := db.Open(ctx, testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	objDir := t.TempDir()
	objects, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}
	return &state{pool: pool, repo: db.NewRepo(pool), objects: objects, objDir: objDir, stage: t.TempDir()}
}

// server is one running grpcapi.Service over bufconn, built from the real
// adapters, db.Repo, finalize and staging.
type server struct {
	t      *testing.T
	st     *state
	svc    *grpcapi.Service
	gs     *grpc.Server
	store  *staging.Store
	client uploadv1.UploadServiceClient
}

func startServer(t *testing.T, st *state) *server {
	t.Helper()
	store, err := staging.New(st.stage)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewVerifier(keys, 0)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, err := grpcapi.NewService(grpcapi.Config{
		MaxChunkBytes: chunkSize,
		AckEveryN:     1,
		Rules:         meta.Rules{MaxFileBytes: 1 << 20, ContentTypes: config.DefaultContentTypes},
	}, grpcapi.Deps{
		Repo:      NewRepo(st.repo),
		Finalizer: NewFinalizer(finalize.New(st.repo, st.objects, metrics, log, nil)),
		Store:     store, Verifier: verifier, Metrics: metrics, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	gs, err := grpcapi.NewServer(svc)
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	opts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, grpcprefix.DialOptions("/grpc")...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &server{t: t, st: st, svc: svc, gs: gs, store: store, client: uploadv1.NewUploadServiceClient(conn)}
}

func ctxFor(t *testing.T, device string) context.Context {
	t.Helper()
	tok, err := keys.Token(device)
	if err != nil {
		t.Fatal(err)
	}
	return metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+tok)
}

// file is a random payload and its InitRequest.
type file struct {
	data []byte
	req  *uploadv1.InitRequest
}

func newFile(t *testing.T, device string, size int) file {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return fileOf(device, data)
}

func fileOf(device string, data []byte) file {
	sum := sha256.Sum256(data)
	return file{data: data, req: &uploadv1.InitRequest{
		DeviceId:    device,
		Sha256:      hex.EncodeToString(sum[:]),
		Size:        int64(len(data)),
		Filename:    "a.flac",
		ContentType: "audio/flac",
		CapturedAt:  "2026-10-21T05:30:00+07:00",
		Priority:    1,
	}}
}

func (f file) meta(t *testing.T) meta.Metadata {
	t.Helper()
	at, err := meta.ParseCapturedAt(f.req.GetCapturedAt())
	if err != nil {
		t.Fatal(err)
	}
	return meta.Metadata{
		DeviceID: f.req.GetDeviceId(), SHA256: f.req.GetSha256(), Filename: f.req.GetFilename(),
		ContentType: f.req.GetContentType(), CapturedAt: at, Priority: int(f.req.GetPriority()), Size: f.req.GetSize(),
	}
}

func (s *server) init(f file) *uploadv1.InitResponse {
	s.t.Helper()
	res, err := s.client.Init(ctxFor(s.t, f.req.GetDeviceId()), f.req)
	if err != nil {
		s.t.Fatalf("Init: %v", err)
	}
	return res
}

type stream = grpc.BidiStreamingClient[uploadv1.Chunk, uploadv1.Ack]

// openStream opens an Upload stream and sends the chunk at from, retrying
// while a previous stream on the server still holds the upload (ABORTED).
// It returns the stream and the ack of that chunk.
func (s *server) openStream(ctx context.Context, f file, id string, from int) (stream, *uploadv1.Ack, error) {
	s.t.Helper()
	end := min(from+chunkSize, len(f.data))
	for range 50 {
		st, err := s.client.Upload(ctx)
		if err != nil {
			return nil, nil, err
		}
		if err := st.Send(&uploadv1.Chunk{UploadId: id, Offset: int64(from), Data: f.data[from:end]}); err != nil {
			return nil, nil, err
		}
		a, err := st.Recv()
		if status.Code(err) == codes.Aborted {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		return st, a, err
	}
	return nil, nil, errors.New("upload still busy")
}

// sendTo sends f.data[from:to] on a fresh stream and waits for the ack of
// offset to. It returns that ack and the (still open) stream.
func (s *server) sendTo(ctx context.Context, f file, id string, from, to int) (*uploadv1.Ack, stream, error) {
	s.t.Helper()
	st, a, err := s.openStream(ctx, f, id, from)
	if err != nil {
		return a, st, err
	}
	for off := from + chunkSize; off < to; off += chunkSize {
		end := min(off+chunkSize, to)
		if err := st.Send(&uploadv1.Chunk{UploadId: id, Offset: int64(off), Data: f.data[off:end]}); err != nil {
			return nil, st, err
		}
	}
	for a.GetCommittedOffset() < int64(to) {
		if a, err = st.Recv(); err != nil {
			return a, st, err
		}
	}
	return a, st, nil
}

// uploadRest sends f.data[from:] and expects the complete ack (the final
// chunk is acked once, with complete set).
func (s *server) uploadRest(f file, id string, from int) error {
	s.t.Helper()
	a, st, err := s.sendTo(ctxFor(s.t, f.req.GetDeviceId()), f, id, from, len(f.data))
	if err != nil {
		return err
	}
	if !a.GetComplete() {
		return fmt.Errorf("last ack %v not complete", a)
	}
	return st.CloseSend()
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v (%v), want %v", got, err, want)
	}
}

func (st *state) row(t *testing.T, id string) db.Upload {
	t.Helper()
	u, err := st.repo.Get(context.Background(), grpcapi.Protocol, id)
	if err != nil {
		t.Fatalf("get row %s: %v", id, err)
	}
	return u
}

// rowsBySHA counts rows with sha256 sha, by status.
func (st *state) rowsBySHA(t *testing.T, sha string) map[db.Status]int {
	t.Helper()
	rows, err := st.pool.Query(context.Background(), `SELECT status FROM uploads WHERE sha256 = $1`, sha)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[db.Status]int{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out[db.Status(s)]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// objects lists the stored object keys (temp files excluded).
func (st *state) objectKeys(t *testing.T) []string {
	t.Helper()
	var keys []string
	err := filepath.WalkDir(st.objDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasPrefix(d.Name(), ".put-") {
			return err
		}
		rel, err := filepath.Rel(st.objDir, p)
		keys = append(keys, filepath.ToSlash(rel))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func (st *state) staged(t *testing.T, id string) bool {
	t.Helper()
	for _, ext := range []string{".bin", ".json"} {
		if _, err := os.Stat(filepath.Join(st.stage, id+ext)); err == nil {
			return true
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	return false
}
