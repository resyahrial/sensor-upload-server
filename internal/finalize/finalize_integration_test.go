//go:build integration

package finalize

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/objstore"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

const (
	postgresImage = "docker.io/library/postgres:16-alpine"
	minioImage    = "docker.io/pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
)

var (
	testDSN string
	s3Cfg   objstore.S3Config
)

func TestMain(m *testing.M) { os.Exit(runWithContainers(m)) }

func runWithContainers(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pg, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("uploads"), postgres.WithUsername("test"), postgres.WithPassword("test"),
		postgres.BasicWaitStrategies())
	defer terminate("postgres", pg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	if testDSN, err = pg.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintln(os.Stderr, "postgres dsn:", err)
		return 1
	}

	mc, err := minio.Run(ctx, minioImage)
	defer terminate("minio", mc)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start minio:", err)
		return 1
	}
	host, err := mc.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "minio endpoint:", err)
		return 1
	}
	s3Cfg = objstore.S3Config{
		Endpoint: "http://" + host, Region: "us-east-1", Bucket: "uploads",
		AccessKey: mc.Username, SecretKey: mc.Password, ForcePathStyle: true,
	}
	if _, err := rawS3().CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(s3Cfg.Bucket)}); err != nil {
		fmt.Fprintln(os.Stderr, "create bucket:", err)
		return 1
	}
	return m.Run()
}

func terminate(name string, c testcontainers.Container) {
	if err := testcontainers.TerminateContainer(c); err != nil {
		fmt.Fprintf(os.Stderr, "terminate %s: %v\n", name, err)
	}
}

// rawS3 is a plain client for assertions (listing objects).
func rawS3() *s3.Client {
	return s3.New(s3.Options{
		Region:       s3Cfg.Region,
		BaseEndpoint: aws.String(s3Cfg.Endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(s3Cfg.AccessKey, s3Cfg.SecretKey, ""),
	})
}

type itEnv struct {
	repo  *db.Repo
	store *objstore.S3
	f     *Finalizer
	dir   string
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
	store, err := objstore.NewS3(ctx, s3Cfg)
	if err != nil {
		t.Fatal(err)
	}
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	repo := db.NewRepo(pool)
	return &itEnv{
		repo:  repo,
		store: store,
		f:     New(repo, store, m, slog.New(slog.DiscardHandler), nil),
		dir:   t.TempDir(),
	}
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// stage writes data to a staging file, inserts an uploading row and
// returns the finalizer Input plus the staging path.
func (e *itEnv) stage(t *testing.T, protocol, device string, data []byte, sha string) (Input, string, meta.Metadata) {
	t.Helper()
	ref := uuid.Must(uuid.NewV7()).String()
	path := filepath.Join(e.dir, ref+".bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	m := meta.Metadata{
		DeviceID: device, SHA256: sha, Filename: "f.wav", ContentType: "audio/wav",
		CapturedAt: time.Date(2026, 10, 21, 23, 0, 0, 0, time.FixedZone("", -5*3600)), Priority: 1,
		Size: int64(len(data)),
	}
	if _, err := e.repo.InsertUploading(context.Background(), protocol, ref, m); err != nil {
		t.Fatal(err)
	}
	in := Input{
		Protocol: protocol,
		Ref:      ref,
		Open:     func(context.Context) (io.ReadCloser, error) { return os.Open(path) },
		Delete: func(context.Context) error {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			return nil
		},
	}
	return in, path, m
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (e *itEnv) objectExists(t *testing.T, key string) bool {
	t.Helper()
	_, ok, err := e.store.Stat(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func countObjects(t *testing.T, prefix string) int {
	t.Helper()
	out, err := rawS3().ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Cfg.Bucket), Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatal(err)
	}
	return int(aws.ToInt32(out.KeyCount))
}

func TestFinalizeIntegrationHappyPath(t *testing.T) {
	e := newITEnv(t)
	ctx := context.Background()
	data := randBytes(t, 2*objstore.DefaultS3PartSize+777) // multipart
	in, path, m := e.stage(t, "rest", "bioac-happy", data, shaHex(data))

	key, err := e.f.Run(ctx, in)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 23:00-05:00 is the next day in UTC.
	if want := "bioac-happy/2026/10/22/" + m.SHA256 + ".wav"; key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	size, ok, err := e.store.Stat(ctx, key)
	if err != nil || !ok || size != int64(len(data)) {
		t.Fatalf("Stat = %d, %v, %v", size, ok, err)
	}
	row, err := e.repo.Get(ctx, "rest", in.Ref)
	if err != nil || row.Status != db.StatusCompleted || row.StorageKey != key || row.CompletedAt == nil {
		t.Fatalf("row = %+v, %v", row, err)
	}
	if exists(path) {
		t.Fatal("staging file not deleted")
	}
}

func TestFinalizeIntegrationChecksumMismatch(t *testing.T) {
	e := newITEnv(t)
	ctx := context.Background()
	data := randBytes(t, 4096)
	in, path, m := e.stage(t, "grpc", "bioac-badsum", data, shaHex([]byte("something else")))

	if _, err := e.f.Run(ctx, in); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Run = %v, want ErrChecksum", err)
	}
	row, err := e.repo.Get(ctx, "grpc", in.Ref)
	if err != nil || row.Status != db.StatusFailedChecksum {
		t.Fatalf("row = %+v, %v", row, err)
	}
	if exists(path) {
		t.Fatal("staging file not deleted")
	}
	if e.objectExists(t, meta.FinalKey(m)) || countObjects(t, "bioac-badsum/") != 0 {
		t.Fatal("object stored for a corrupt upload")
	}
}

func TestFinalizeIntegrationDuplicateRace(t *testing.T) {
	tests := []struct{ name, devA, devB string }{
		{"different devices", "bioac-race-a", "bioac-race-b"},
		{"same device", "bioac-race-s", "bioac-race-s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newITEnv(t)
			ctx := context.Background()
			for i := range 5 {
				data := randBytes(t, 64<<10)
				sha := shaHex(data)
				inA, pathA, mA := e.stage(t, "rest", tt.devA, data, sha)
				inB, pathB, mB := e.stage(t, "grpc", tt.devB, data, sha)

				var wg sync.WaitGroup
				errs := make([]error, 2)
				for j, in := range []Input{inA, inB} {
					wg.Go(func() { _, errs[j] = e.f.Run(ctx, in) })
				}
				wg.Wait()

				var ok, dup int
				for _, err := range errs {
					switch {
					case err == nil:
						ok++
					case errors.Is(err, ErrDuplicate):
						dup++
					default:
						t.Fatalf("iteration %d: unexpected error %v", i, err)
					}
				}
				if ok != 1 || dup != 1 {
					t.Fatalf("iteration %d: ok=%d dup=%d, want 1/1 (errs %v)", i, ok, dup, errs)
				}
				winner, err := e.repo.FindCompletedBySHA(ctx, sha)
				if err != nil {
					t.Fatal(err)
				}
				var objects int
				for _, k := range uniq(meta.FinalKey(mA), meta.FinalKey(mB)) {
					if e.objectExists(t, k) {
						objects++
						if k != winner.StorageKey {
							t.Fatalf("iteration %d: orphan object %s", i, k)
						}
					}
				}
				if objects != 1 {
					t.Fatalf("iteration %d: %d objects, want 1", i, objects)
				}
				if exists(pathA) || exists(pathB) {
					t.Fatalf("iteration %d: staging left behind", i)
				}
				loser := inA
				if winner.Ref == inA.Ref {
					loser = inB
				}
				if row, _ := e.repo.Get(ctx, loser.Protocol, loser.Ref); row.Status != db.StatusCancelled {
					t.Fatalf("iteration %d: loser status %s", i, row.Status)
				}
			}
		})
	}
}

func uniq(a, b string) []string {
	if a == b {
		return []string{a}
	}
	return []string{a, b}
}

// crashRepo makes Complete fail once, either before (crash between steps
// 3 and 4) or after (between 4 and 5) the real update.
type crashRepo struct {
	*db.Repo
	mu    sync.Mutex
	after bool
	fired bool
}

var errCrash = errors.New("simulated crash")

func (c *crashRepo) Complete(ctx context.Context, protocol, ref, key string) (db.Upload, error) {
	c.mu.Lock()
	fire := !c.fired
	c.fired = true
	c.mu.Unlock()
	if !fire {
		return c.Repo.Complete(ctx, protocol, ref, key)
	}
	if c.after {
		if _, err := c.Repo.Complete(ctx, protocol, ref, key); err != nil {
			return db.Upload{}, err
		}
	}
	return db.Upload{}, errCrash
}

func TestFinalizeIntegrationRetryAfterCrash(t *testing.T) {
	for name, after := range map[string]bool{"between put and complete": false, "between complete and delete": true} {
		t.Run(name, func(t *testing.T) {
			e := newITEnv(t)
			ctx := context.Background()
			crashing := New(&crashRepo{Repo: e.repo, after: after}, e.store, e.f.metrics, e.f.log, nil)
			data := randBytes(t, 10_000)
			in, path, m := e.stage(t, "grpc", "bioac-crash", data, shaHex(data))

			if _, err := crashing.Run(ctx, in); !errors.Is(err, errCrash) {
				t.Fatalf("first Run = %v, want simulated crash", err)
			}
			if !exists(path) || !e.objectExists(t, meta.FinalKey(m)) {
				t.Fatal("after crash: want staging and object present")
			}
			row, _ := e.repo.Get(ctx, "grpc", in.Ref)
			if wantStatus := map[bool]db.Status{false: db.StatusUploading, true: db.StatusCompleted}[after]; row.Status != wantStatus {
				t.Fatalf("status after crash = %s, want %s", row.Status, wantStatus)
			}

			// Restarted process: a fresh finalizer on the real repo.
			key, err := e.f.Run(ctx, in)
			if err != nil || key != meta.FinalKey(m) {
				t.Fatalf("retry = %q, %v", key, err)
			}
			row, _ = e.repo.Get(ctx, "grpc", in.Ref)
			if row.Status != db.StatusCompleted || exists(path) {
				t.Fatalf("after retry: status %s, staging exists %v", row.Status, exists(path))
			}
		})
	}
}
