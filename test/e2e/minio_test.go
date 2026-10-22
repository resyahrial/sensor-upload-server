//go:build integration

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/resyahrial/sensor-upload-server/internal/objstore"
)

const minioImage = "docker.io/pgsty/minio:RELEASE.2026-08-04T00-00-00Z"

// TestMinIO: OBJSTORE=s3 against MinIO, with file staging (default) and
// with TUS_STAGING=s3 (smoke).
func TestMinIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctr, err := minio.Run(ctx, minioImage)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	host, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	scfg := objstore.S3Config{
		Endpoint: "http://" + host, Region: "us-east-1", Bucket: "uploads",
		AccessKey: ctr.Username, SecretKey: ctr.Password, ForcePathStyle: true,
	}
	cl, err := objstore.NewS3Client(ctx, scfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(scfg.Bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	store, err := objstore.NewS3(ctx, scfg)
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"OBJSTORE": "s3", "S3_ENDPOINT": scfg.Endpoint, "S3_REGION": scfg.Region, "S3_BUCKET": scfg.Bucket,
		"S3_ACCESS_KEY": scfg.AccessKey, "S3_SECRET_KEY": scfg.SecretKey, "S3_FORCE_PATH_STYLE": "true",
	}
	for _, staging := range []string{"file", "s3"} {
		t.Run("tus-staging-"+staging, func(t *testing.T) {
			env["TUS_STAGING"] = staging
			s := StartServer(t, env)
			dev := device()
			c := Endpoint(t, s, H2, "/rest/files/", dev)
			size := 3*chunk + 5
			f := NewFile(t, dev, size)
			loc := c.MustCreate(f)
			// With s3 staging, chunks < 5 MiB become .part objects that
			// s3store re-downloads on the next PATCH (works, but slow).
			c.SendFrom(loc, f, 0, chunk)
			u := Row(t, s, "rest", loc)
			if u.Status != "completed" {
				t.Fatalf("status = %s", u.Status)
			}
			got, ok, err := store.Stat(ctx, u.StorageKey)
			if err != nil || !ok || got != int64(size) {
				t.Errorf("object %s: size %d exists %v err %v", u.StorageKey, got, ok, err)
			}
			if off := c.Head(loc); off != size {
				t.Errorf("HEAD after completion = %d", off)
			}
		})
	}
}
