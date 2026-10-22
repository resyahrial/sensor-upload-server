package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Multipart defaults. The transfer manager buffers up to
// (Concurrency+1) parts per upload, so memory per Put is bounded by
// (DefaultS3Concurrency+1)*DefaultS3PartSize (15 MiB), independent of the
// object size (NFR-2).
const (
	DefaultS3PartSize    = 5 << 20 // S3 minimum part size
	DefaultS3Concurrency = 2
)

// S3Config configures the S3-compatible store.
type S3Config struct {
	Endpoint       string // empty = AWS default endpoint resolution
	Region         string
	Bucket         string
	AccessKey      string // empty = AWS default credential chain
	SecretKey      string
	ForcePathStyle bool  // required by MinIO
	PartSize       int64 // 0 = DefaultS3PartSize; also the multipart threshold
	Concurrency    int   // 0 = DefaultS3Concurrency
}

// S3 stores objects in an S3-compatible bucket.
type S3 struct {
	client *s3.Client
	tm     *transfermanager.Client
	bucket string
}

var _ Store = (*S3)(nil)

// NewS3 creates an S3 store. It does not contact the service; use Ping.
func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	client, err := NewS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	partSize := cfg.PartSize
	if partSize <= 0 {
		partSize = DefaultS3PartSize
	}
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultS3Concurrency
	}
	tm := transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = partSize
		// The first read buffers up to the threshold, so keep it at one part.
		o.MultipartUploadThreshold = partSize
		o.Concurrency = concurrency
	})
	return &S3{client: client, tm: tm, bucket: cfg.Bucket}, nil
}

// NewS3Client builds the S3 API client for cfg (endpoint, region,
// credentials, path style). It is shared with the tus s3store staging.
func NewS3Client(ctx context.Context, cfg S3Config) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3: load AWS config: %w", err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	}), nil
}

// Put uploads r (multipart above the part size) unless key already exists
// with the same size. S3 makes an object visible only when the upload
// completes, and a failed multipart upload is aborted by the transfer manager.
func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if got, ok, err := s.Stat(ctx, key); err != nil {
		return err
	} else if ok && got == size {
		return nil
	}
	_, err := s.tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          newExactReader(r, size),
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("s3 put %q: %w", key, err)
	}
	return nil
}

// Stat reports the size of key and whether it exists (HeadObject).
func (s *S3) Stat(ctx context.Context, key string) (int64, bool, error) {
	if err := ValidateKey(key); err != nil {
		return 0, false, err
	}
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if isNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("s3 head %q: %w", key, err)
	}
	return aws.ToInt64(out.ContentLength), true, nil
}

// Delete removes key; S3 treats deleting a missing key as success.
func (s *S3) Delete(ctx context.Context, key string) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 delete %q: %w", key, err)
	}
	return nil
}

// Ping checks that the bucket exists and is accessible (HeadBucket).
func (s *S3) Ping(ctx context.Context) error {
	if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)}); err != nil {
		return fmt.Errorf("s3 head bucket: %w", err)
	}
	return nil
}

// isNotFound reports a 404 response. HeadObject has no body, so the error
// carries only the status code, not a typed NoSuchKey.
func isNotFound(err error) bool {
	var re interface{ HTTPStatusCode() int }
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound
}
