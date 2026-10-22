//go:build integration

package cleanup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	"github.com/resyahrial/sensor-upload-server/internal/grpcadapt"
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
	itDevice      = "dev-cleanup-1"
)

var (
	testDSN string
	secret  = []byte("test-secret")
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

func authCtx(t *testing.T) context.Context {
	t.Helper()
	tok, err := auth.Sign(auth.HS256, secret, itDevice, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+tok)
}

func initReq(t *testing.T, size int) ([]byte, *uploadv1.InitRequest) {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return data, &uploadv1.InitRequest{
		DeviceId: itDevice, Sha256: hex.EncodeToString(sum[:]), Size: int64(size),
		Filename: "a.wav", ContentType: "audio/x-wav", CapturedAt: "2026-10-21T05:30:00Z", Priority: 1,
	}
}

// openWithChunk opens a stream, sends the first chunk of data and waits
// for its ack, so the server holds the upload while the stream is open.
func openWithChunk(t *testing.T, ctx context.Context, c uploadv1.UploadServiceClient, id string, data []byte) grpc.BidiStreamingClient[uploadv1.Chunk, uploadv1.Ack] {
	t.Helper()
	s, err := c.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(&uploadv1.Chunk{UploadId: id, Data: data[:chunkSize]}); err != nil {
		t.Fatal(err)
	}
	if a, err := s.Recv(); err != nil || a.GetCommittedOffset() != chunkSize {
		t.Fatalf("first ack = %v, %v", a, err)
	}
	return s
}

// itEnv is a gRPC service and a cleaner over Postgres, real staging, the
// real finalizer and a local object store.
type itEnv struct {
	pool    *pgxpool.Pool
	repo    *db.Repo
	objects *objstore.Local
	// objectsDir is the root of objects.
	objectsDir string
	store      *staging.Store
	stageDir   string
	svc        *grpcapi.Service
	client     uploadv1.UploadServiceClient
	metrics    *obs.Metrics
	cleaner    *Cleaner
}

func newITEnv(t *testing.T) *itEnv {
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
	repo := db.NewRepo(pool)
	objectsDir := t.TempDir()
	objects, err := objstore.NewLocal(objectsDir)
	if err != nil {
		t.Fatal(err)
	}
	stageDir := t.TempDir()
	store, err := staging.New(stageDir)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewVerifier(auth.Config{Algs: []string{auth.HS256}, HMACSecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fin := finalize.New(repo, objects, metrics, log, nil)
	svc, err := grpcapi.NewService(grpcapi.Config{
		MaxChunkBytes: chunkSize, AckEveryN: 1,
		Rules: meta.Rules{MaxFileBytes: 1 << 20, ContentTypes: config.DefaultContentTypes},
	}, grpcapi.Deps{
		Repo:      grpcadapt.NewRepo(repo),
		Finalizer: grpcadapt.NewFinalizer(fin),
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
	conn, err := grpc.NewClient("passthrough:///bufnet", append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, grpcprefix.DialOptions("/grpc")...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := uploadv1.NewUploadServiceClient(conn)

	cleaner, err := New(Config{TTL: time.Hour, Interval: time.Hour}, Deps{
		Repo: repo, GRPC: svc, GRPCStaging: store, Finalizer: fin, Metrics: metrics, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &itEnv{pool: pool, repo: repo, objects: objects, objectsDir: objectsDir, store: store, stageDir: stageDir,
		svc: svc, client: client, metrics: metrics, cleaner: cleaner}
}

func (e *itEnv) age(t *testing.T, id string) {
	t.Helper()
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE uploads SET updated_at = now() - interval '2 hours' WHERE upload_ref = $1`, id); err != nil {
		t.Fatal(err)
	}
}

func TestExpireGRPC(t *testing.T) {
	ctx := context.Background()
	e := newITEnv(t)
	repo, stageDir, svc, client, metrics, cleaner := e.repo, e.stageDir, e.svc, e.client, e.metrics, e.cleaner

	newUpload := func() (string, []byte, *uploadv1.InitRequest) {
		data, req := initReq(t, 3*chunkSize)
		res, err := client.Init(authCtx(t), req)
		if err != nil {
			t.Fatal(err)
		}
		return res.GetUploadId(), data, req
	}
	age := func(id string) { e.age(t, id) }

	// stale: one chunk committed, stream closed, then aged.
	staleID, staleData, staleReq := newUpload()
	s := openWithChunk(t, authCtx(t), client, staleID, staleData)
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream end = %v", err)
	}
	waitReleased(t, svc, staleID)
	age(staleID)

	// fresh: untouched.
	freshID, _, _ := newUpload()

	// active: aged, but a stream holds it.
	activeID, activeData, _ := newUpload()
	activeCtx, cancelActive := context.WithCancel(authCtx(t))
	defer cancelActive()
	openWithChunk(t, activeCtx, client, activeID, activeData)
	age(activeID)

	n, err := cleaner.RunOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v; want 1", n, err)
	}

	if u, err := repo.Get(ctx, "grpc", staleID); err != nil || u.Status != db.StatusExpired {
		t.Fatalf("stale row = %+v, %v", u, err)
	}
	if staged(t, stageDir, staleID) {
		t.Fatal("stale staging files left behind")
	}
	_, err = client.Status(authCtx(t), &uploadv1.StatusRequest{UploadId: staleID})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Status of expired upload: %v, want NOT_FOUND", err)
	}
	if res, err := client.Init(authCtx(t), staleReq); err != nil || res.GetUploadId() == staleID || res.GetCommittedOffset() != 0 {
		t.Fatalf("Init after expiry = %v, %v; want a new upload", res, err)
	}
	if got := testutil.ToFloat64(metrics.Failed.WithLabelValues("grpc", obs.ReasonExpired)); got != 1 {
		t.Fatalf("expired metric = %v", got)
	}

	for _, id := range []string{freshID, activeID} {
		if u, err := repo.Get(ctx, "grpc", id); err != nil || u.Status != db.StatusUploading {
			t.Fatalf("row %s = %+v, %v; want uploading", id, u, err)
		}
		if !staged(t, stageDir, id) {
			t.Fatalf("staging of %s deleted", id)
		}
	}

	// Once the stream is gone, the still-stale active upload is expired.
	cancelActive()
	waitReleased(t, svc, activeID)
	if n, err := cleaner.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("second RunOnce = %d, %v; want 1", n, err)
	}
}

func waitReleased(t *testing.T, svc *grpcapi.Service, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for svc.Active(id) {
		if time.Now().After(deadline) {
			t.Fatalf("upload %s still held", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func staged(t *testing.T, dir, id string) bool {
	t.Helper()
	n := 0
	for _, ext := range []string{".bin", ".json"} {
		if _, err := os.Stat(filepath.Join(dir, id+ext)); err == nil {
			n++
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	return n > 0
}

// fullyCommitted creates a gRPC upload whose bytes are all staged and
// committed but which was never finalized (the finalize failed or the
// process stopped after the last commit).
func (e *itEnv) fullyCommitted(t *testing.T) (string, []byte) {
	t.Helper()
	data, req := initReq(t, 3*chunkSize)
	return e.fullyCommittedWith(t, data, req), data
}

// fullyCommittedWith is fullyCommitted for a given file.
func (e *itEnv) fullyCommittedWith(t *testing.T, data []byte, req *uploadv1.InitRequest) string {
	t.Helper()
	res, err := e.client.Init(authCtx(t), req)
	if err != nil {
		t.Fatal(err)
	}
	id := res.GetUploadId()
	f, err := e.store.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := e.store.LoadInfo(id)
	if err != nil {
		t.Fatal(err)
	}
	info.CommittedOffset = int64(len(data))
	if err := e.store.SaveInfo(id, info); err != nil {
		t.Fatal(err)
	}
	if err := e.repo.UpdateCommitted(context.Background(), GRPCProtocol, id, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	return id
}

// TestFinalizeFullyCommittedGRPC: a fully committed upload is finalized by
// the cleaner, not expired, whether it is stale or not.
func TestFinalizeFullyCommittedGRPC(t *testing.T) {
	ctx := context.Background()
	e := newITEnv(t)
	staleID, staleData := e.fullyCommitted(t)
	e.age(t, staleID)
	freshID, freshData := e.fullyCommitted(t)

	if n, err := e.cleaner.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("RunOnce = %d, %v; want 0 expired", n, err)
	}
	for id, data := range map[string][]byte{staleID: staleData, freshID: freshData} {
		u, err := e.repo.Get(ctx, GRPCProtocol, id)
		if err != nil || u.Status != db.StatusCompleted {
			t.Fatalf("row %s = %+v, %v; want completed", id, u, err)
		}
		if u.StorageKey != meta.FinalKey(u.Metadata()) {
			t.Fatalf("storage key = %q", u.StorageKey)
		}
		if size, ok, err := e.objects.Stat(ctx, u.StorageKey); err != nil || !ok || size != int64(len(data)) {
			t.Fatalf("object %s: size %d, exists %v, %v", u.StorageKey, size, ok, err)
		}
		if staged(t, e.stageDir, id) {
			t.Fatalf("staging of %s left behind", id)
		}
	}
	if got := testutil.ToFloat64(e.metrics.Failed.WithLabelValues(GRPCProtocol, obs.ReasonExpired)); got != 0 {
		t.Fatalf("expired metric = %v", got)
	}
	if got := testutil.ToFloat64(e.metrics.Completed.WithLabelValues(GRPCProtocol)); got != 2 {
		t.Fatalf("completed metric = %v", got)
	}
}

// TestSweepOrphansGRPC: staged files left behind a completed row (crash
// between Complete and the staging delete) are removed once old enough;
// recent ones and those of a live upload stay.
func TestSweepOrphansGRPC(t *testing.T) {
	ctx := context.Background()
	e := newITEnv(t)
	doneID, _ := e.fullyCommitted(t)
	recentID, _ := e.fullyCommitted(t)
	if _, err := e.cleaner.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	liveID, _ := e.fullyCommitted(t)
	if err := e.repo.UpdateCommitted(ctx, GRPCProtocol, liveID, 1); err != nil {
		t.Fatal(err)
	}

	// Put back the staging of the completed rows, as a crash would leave it.
	old := time.Now().Add(-2 * time.Hour)
	for _, id := range []string{doneID, recentID} {
		if u, err := e.repo.Get(ctx, GRPCProtocol, id); err != nil || u.Status != db.StatusCompleted {
			t.Fatalf("row %s = %+v, %v", id, u, err)
		}
		for _, ext := range []string{".bin", ".json"} {
			if err := os.WriteFile(filepath.Join(e.stageDir, id+ext), []byte("left"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, name := range []string{doneID + ".bin", doneID + ".json", liveID + ".bin", liveID + ".json"} {
		if err := os.Chtimes(filepath.Join(e.stageDir, name), old, old); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := e.cleaner.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if staged(t, e.stageDir, doneID) {
		t.Fatal("orphaned staging of a completed upload left behind")
	}
	if !staged(t, e.stageDir, recentID) || !staged(t, e.stageDir, liveID) {
		t.Fatal("recent or live staging deleted")
	}
	if got := testutil.ToFloat64(e.metrics.StagingOrphans.WithLabelValues("grpc")); got != 1 {
		t.Fatalf("orphan metric = %v", got)
	}
}
