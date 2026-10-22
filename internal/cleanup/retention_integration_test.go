//go:build integration

package cleanup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/objstore"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

const minioImage = "docker.io/pgsty/minio:RELEASE.2026-08-04T00-00-00Z"

func newITRetention(t *testing.T, repo *db.Repo, store ObjectDeleter, metrics *obs.Metrics, cfg RetentionConfig, stat StatFunc) *Retention {
	t.Helper()
	if cfg.Interval == 0 {
		cfg.Interval = time.Hour
	}
	if cfg.DiskInterval == 0 {
		cfg.DiskInterval = time.Minute
	}
	if cfg.HighWatermark == 0 {
		cfg.HighWatermark, cfg.LowWatermark = 100, 99
	}
	if cfg.DiskPath == "" {
		cfg.DiskPath = t.TempDir()
	}
	r, err := NewRetention(cfg, RetentionDeps{Repo: repo, Store: store, Stat: stat, Metrics: metrics,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (e *itEnv) setCompletedAt(t *testing.T, ref string, at time.Time) {
	t.Helper()
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE uploads SET completed_at = $2 WHERE upload_ref = $1`, ref, at); err != nil {
		t.Fatal(err)
	}
}

// TestRetentionAgeReupload: an upload completed more than MaxAge ago loses
// its object and row, a recent one and an in-progress one stay, and the
// same file can be uploaded (and completed) again afterwards.
func TestRetentionAgeReupload(t *testing.T) {
	ctx := context.Background()
	e := newITEnv(t)
	data, req := initReq(t, 3*chunkSize)
	oldID := e.fullyCommittedWith(t, data, req)
	recentID, _ := e.fullyCommitted(t)
	if _, err := e.cleaner.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	old, err := e.repo.Get(ctx, GRPCProtocol, oldID)
	if err != nil || old.Status != db.StatusCompleted {
		t.Fatalf("old row = %+v, %v", old, err)
	}
	e.setCompletedAt(t, oldID, time.Now().Add(-8*24*time.Hour))
	// An in-progress upload, idle for longer than MaxAge (expiry's job).
	_, idle := initReq(t, 3*chunkSize)
	res, err := e.client.Init(authCtx(t), idle)
	if err != nil {
		t.Fatal(err)
	}
	idleID := res.GetUploadId()
	if _, err := e.pool.Exec(ctx, `UPDATE uploads SET created_at = now() - interval '30 days' WHERE upload_ref = $1`, idleID); err != nil {
		t.Fatal(err)
	}
	// Before retention, the same file is a duplicate.
	if _, err := e.client.Init(authCtx(t), req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("Init of a completed file = %v, want ALREADY_EXISTS", err)
	}

	r := newITRetention(t, e.repo, e.objects, e.metrics, RetentionConfig{MaxAge: 7 * 24 * time.Hour}, nil)
	n, err := r.PurgeAged(ctx)
	if err != nil || n != 1 {
		t.Fatalf("PurgeAged = %d, %v; want 1", n, err)
	}
	if _, err := e.repo.Get(ctx, GRPCProtocol, oldID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("old row after retention: %v, want ErrNotFound", err)
	}
	if _, ok, err := e.objects.Stat(ctx, old.StorageKey); err != nil || ok {
		t.Fatalf("old object exists=%v, %v", ok, err)
	}
	if u, err := e.repo.Get(ctx, GRPCProtocol, recentID); err != nil || u.Status != db.StatusCompleted {
		t.Fatalf("recent row = %+v, %v", u, err)
	}
	if u, err := e.repo.Get(ctx, GRPCProtocol, idleID); err != nil || u.Status != db.StatusUploading {
		t.Fatalf("in-progress row = %+v, %v", u, err)
	}
	if got := testutil.ToFloat64(e.metrics.RetentionDeleted.WithLabelValues(obs.RetentionAge)); got != 1 {
		t.Fatalf("retention_deleted{age} = %v", got)
	}
	// Idempotent: nothing left to do.
	if n, err := r.PurgeAged(ctx); err != nil || n != 0 {
		t.Fatalf("second PurgeAged = %d, %v", n, err)
	}

	// The same file again: a new upload that completes to the same key.
	newID := e.fullyCommittedWith(t, data, req)
	if newID == oldID {
		t.Fatal("Init after retention returned the deleted upload")
	}
	if _, err := e.cleaner.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	u, err := e.repo.Get(ctx, GRPCProtocol, newID)
	if err != nil || u.Status != db.StatusCompleted || u.StorageKey != old.StorageKey {
		t.Fatalf("re-uploaded row = %+v, %v", u, err)
	}
	if size, ok, err := e.objects.Stat(ctx, u.StorageKey); err != nil || !ok || size != int64(len(data)) {
		t.Fatalf("re-uploaded object: %d, %v, %v", size, ok, err)
	}
}

// TestRetentionDiskPressure: with usage measured from the object
// directory, the oldest completed uploads are deleted until usage is
// below the low watermark.
func TestRetentionDiskPressure(t *testing.T) {
	ctx := context.Background()
	e := newITEnv(t)
	const size = 3 * chunkSize
	var refs []string
	for range 4 {
		id, _ := e.fullyCommitted(t)
		refs = append(refs, id)
	}
	if _, err := e.cleaner.RunOnce(ctx); err != nil { // completes them
		t.Fatal(err)
	}
	t.Cleanup(func() { // the 1990 rows left over would be every later test's oldest
		if _, err := e.pool.Exec(context.Background(), `DELETE FROM uploads WHERE upload_ref = ANY($1)`, refs); err != nil {
			t.Error(err)
		}
	})
	for i, id := range refs {
		// Oldest of the whole database, so other tests' rows come later.
		e.setCompletedAt(t, id, time.Date(1990, 1, 1+i, 0, 0, 0, 0, time.UTC))
	}
	root := e.objectsDir
	// 4 objects in a filesystem of 5 objects: 80%.
	stat := func(string) (DiskUsage, error) {
		var used uint64
		err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			fi, err := d.Info()
			if err == nil {
				used += uint64(fi.Size())
			}
			return err
		})
		return DiskUsage{Total: 5 * size, Used: used}, err
	}
	r := newITRetention(t, e.repo, e.objects, e.metrics,
		RetentionConfig{HighWatermark: 75, LowWatermark: 50}, stat)

	n, err := r.RelieveDisk(ctx)
	// 80% → 60% → 40%: the two oldest go.
	if err != nil || n != 2 {
		t.Fatalf("RelieveDisk = %d, %v; want 2", n, err)
	}
	for i, id := range refs {
		_, err := e.repo.Get(ctx, GRPCProtocol, id)
		if gone := errors.Is(err, db.ErrNotFound); gone != (i < 2) {
			t.Fatalf("row %d gone=%v (%v)", i, gone, err)
		}
	}
	if got := testutil.ToFloat64(e.metrics.DiskUsedRatio); got != 0.4 {
		t.Fatalf("disk_used_ratio = %v, want 0.4", got)
	}
	if got := testutil.ToFloat64(e.metrics.RetentionDeleted.WithLabelValues(obs.RetentionDisk)); got != 2 {
		t.Fatalf("retention_deleted{disk} = %v", got)
	}
}

// TestRetentionS3: retention deletes the object from S3 (MinIO) and the
// row; an object that is already gone does not block the row delete.
func TestRetentionS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	mc, err := minio.Run(ctx, minioImage)
	testcontainers.CleanupContainer(t, mc)
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	host, err := mc.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := objstore.S3Config{Endpoint: "http://" + host, Region: "us-east-1", Bucket: "uploads",
		AccessKey: mc.Username, SecretKey: mc.Password, ForcePathStyle: true}
	client, err := objstore.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		t.Fatal(err)
	}
	store, err := objstore.NewS3(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := newITEnv(t) // Postgres; its local store is unused here

	completed := func(withObject bool) db.Upload {
		t.Helper()
		data, req := initReq(t, 100)
		res, err := e.client.Init(authCtx(t), req)
		if err != nil {
			t.Fatal(err)
		}
		key := "dev-cleanup-1/2026/10/21/" + req.GetSha256() + ".wav"
		if withObject {
			if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatal(err)
			}
		}
		u, err := e.repo.Complete(ctx, GRPCProtocol, res.GetUploadId(), key)
		if err != nil {
			t.Fatal(err)
		}
		e.setCompletedAt(t, u.Ref, time.Now().Add(-8*24*time.Hour))
		return u
	}
	withObj, noObj := completed(true), completed(false)

	r := newITRetention(t, e.repo, store, e.metrics, RetentionConfig{MaxAge: 7 * 24 * time.Hour}, nil)
	n, err := r.PurgeAged(ctx)
	if err != nil || n != 2 {
		t.Fatalf("PurgeAged = %d, %v; want 2", n, err)
	}
	if _, ok, err := store.Stat(ctx, withObj.StorageKey); err != nil || ok {
		t.Fatalf("S3 object exists=%v, %v after retention", ok, err)
	}
	for _, u := range []db.Upload{withObj, noObj} {
		if _, err := e.repo.Get(ctx, GRPCProtocol, u.Ref); !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("row %s after retention: %v", u.Ref, err)
		}
	}
}
