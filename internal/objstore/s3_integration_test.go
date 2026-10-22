//go:build integration

package objstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

// minioImage is the community MinIO build (upstream images are not
// anonymously pullable; see docs/decisions.md).
const minioImage = "docker.io/pgsty/minio:RELEASE.2026-08-04T00-00-00Z"

func newMinIO(t *testing.T) (*S3, S3Config) {
	t.Helper()
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
	cfg := S3Config{
		Endpoint:       "http://" + host,
		Region:         "us-east-1",
		Bucket:         "uploads",
		AccessKey:      ctr.Username,
		SecretKey:      ctr.Password,
		ForcePathStyle: true,
	}
	s, err := NewS3(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ping(ctx); err == nil {
		t.Fatal("Ping before bucket exists: want error")
	}
	if _, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return s, cfg
}

func TestS3(t *testing.T) {
	s, _ := newMinIO(t)

	t.Run("contract", func(t *testing.T) { storeContract(t, s) })

	t.Run("multipart", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		const size = 3*DefaultS3PartSize + 12345 // 4 parts
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		const key = "dev-1/2026/10/21/big.bin"
		// io.MultiReader hides Seek, as the finalizer's staging readers may.
		if err := s.Put(ctx, key, io.MultiReader(bytes.NewReader(data)), size); err != nil {
			t.Fatalf("Put: %v", err)
		}
		got, ok, err := s.Stat(ctx, key)
		if err != nil || !ok || got != size {
			t.Fatalf("Stat = %d, %v, %v", got, ok, err)
		}
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		defer out.Body.Close()
		h := sha256.New()
		if _, err := io.Copy(h, out.Body); err != nil {
			t.Fatal(err)
		}
		if want := sha256.Sum256(data); !bytes.Equal(h.Sum(nil), want[:]) {
			t.Fatal("downloaded content differs")
		}
	})

	t.Run("multipart short source leaves no object", func(t *testing.T) {
		ctx := context.Background()
		data := make([]byte, 2*DefaultS3PartSize)
		if err := s.Put(ctx, "dev-1/short-big", bytes.NewReader(data), int64(len(data))+1); err == nil {
			t.Fatal("want error")
		}
		if _, ok, err := s.Stat(ctx, "dev-1/short-big"); err != nil || ok {
			t.Fatalf("Stat = ok %v, err %v", ok, err)
		}
	})
}
