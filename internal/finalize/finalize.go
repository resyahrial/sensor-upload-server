// Package finalize is the completion pipeline shared by every protocol
// (FR-8): verify SHA-256, store the object, record completion, delete the
// staged data, then log, count and notify. Every step is idempotent, so Run
// may be repeated after a crash at any point. Run is repeated only while
// the row is 'uploading' (by the protocol layers and the expiry cleaner);
// staged data left behind a row that already has its outcome (a crash
// after the outcome was recorded) is deleted by the cleaner's orphan sweep.
package finalize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// Outcome errors returned by Run (wrapped; test with errors.Is).
var (
	// ErrChecksum: the staged bytes do not match the declared sha256; the
	// row is failed_checksum and the staged data is deleted (460 / DATA_LOSS).
	ErrChecksum = errors.New("sha256 mismatch")
	// ErrDuplicate: another upload with the same sha256 is completed; this
	// row is cancelled and its staged data deleted (461 / ALREADY_EXISTS).
	ErrDuplicate = errors.New("already exists")
	// ErrInactive: the upload was cancelled or expired before it could be
	// finalized.
	ErrInactive = errors.New("upload is cancelled or expired")
)

// Repository is the subset of *db.Repo the finalizer uses.
type Repository interface {
	Get(ctx context.Context, protocol, ref string) (db.Upload, error)
	FindCompletedBySHA(ctx context.Context, sha string) (db.Upload, error)
	Complete(ctx context.Context, protocol, ref, storageKey string) (db.Upload, error)
	SetStatus(ctx context.Context, protocol, ref string, s db.Status) error
}

// ObjectStore is the subset of objstore.Store the finalizer uses.
type ObjectStore interface {
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Delete(ctx context.Context, key string) error
}

// Notifier is told about every newly completed upload (future: publish an
// event). Errors are logged and do not fail the upload.
type Notifier interface {
	Completed(ctx context.Context, u db.Upload) error
}

// NopNotifier is a Notifier that does nothing.
type NopNotifier struct{}

// Completed does nothing.
func (NopNotifier) Completed(context.Context, db.Upload) error { return nil }

// Input identifies the upload to finalize and gives access to its staged
// bytes. Metadata (sha256, size, device, captured_at, content_type) is read
// from the uploads row, which is authoritative.
type Input struct {
	Protocol string // db protocol: "rest", "rest-http3" or "grpc"
	Ref      string // tus upload ID or gRPC upload_id
	// Open returns a reader over the complete staged file. It may be
	// called more than once (hash, then store).
	Open func(ctx context.Context) (io.ReadCloser, error)
	// Delete removes the staged data. It must return nil if the data is
	// already gone.
	Delete func(ctx context.Context) error
}

// Finalizer runs FR-8. It is safe for concurrent use; callers serialise
// Run per upload (tus locker, gRPC stream registry), but concurrent runs on
// the same ref are still correct.
type Finalizer struct {
	repo     Repository
	store    ObjectStore
	metrics  *obs.Metrics
	log      *slog.Logger
	notifier Notifier
}

// New returns a Finalizer. A nil notifier means NopNotifier.
func New(repo Repository, store ObjectStore, metrics *obs.Metrics, log *slog.Logger, notifier Notifier) *Finalizer {
	if notifier == nil {
		notifier = NopNotifier{}
	}
	return &Finalizer{repo: repo, store: store, metrics: metrics, log: log, notifier: notifier}
}

// Run finalizes the upload in and returns its object storage key.
//
// It returns ErrChecksum, ErrDuplicate, ErrInactive, an error wrapping
// db.ErrNotFound for an unknown ref, or another (internal) error after
// which Run may be retried. If the upload is already completed (a retry
// after a crash), Run deletes any remaining staged data and returns the
// stored key; for failed_checksum it returns ErrChecksum again, so a
// protocol layer can call Run to learn a finished upload's outcome.
func (f *Finalizer) Run(ctx context.Context, in Input) (string, error) {
	start := time.Now()
	if in.Open == nil || in.Delete == nil {
		return "", errors.New("finalize: Input.Open and Input.Delete are required")
	}
	key, row, err := f.run(ctx, in)

	attrs := []any{
		"protocol", in.Protocol, "upload_ref", in.Ref, "device_id", row.DeviceID,
		"method", "finalize", "bytes", row.Size,
		"duration_ms", time.Since(start).Milliseconds(),
	}
	switch {
	case err == nil:
		f.log.Info("upload finalized", append(attrs, "status", "completed", "storage_key", key)...)
	case errors.Is(err, ErrChecksum):
		f.log.Warn("upload rejected", append(attrs, "status", "failed_checksum", "error", err.Error())...)
	case errors.Is(err, ErrDuplicate):
		f.log.Info("upload rejected", append(attrs, "status", "duplicate", "error", err.Error())...)
	case errors.Is(err, ErrInactive), errors.Is(err, db.ErrNotFound):
		f.log.Warn("upload not finalizable", append(attrs, "status", "inactive", "error", err.Error())...)
	default:
		f.metrics.Failed.WithLabelValues(in.Protocol, obs.ReasonInternal).Inc()
		f.log.Error("finalize failed", append(attrs, "status", "error", "error", err.Error())...)
	}
	return key, err
}

// run returns the storage key, the row as last loaded (for logging) and
// the outcome.
func (f *Finalizer) run(ctx context.Context, in Input) (string, db.Upload, error) {
	start := time.Now()
	row, err := f.repo.Get(ctx, in.Protocol, in.Ref)
	if err != nil {
		return "", db.Upload{}, fmt.Errorf("load upload: %w", err)
	}

	switch row.Status {
	case db.StatusUploading:
	case db.StatusCompleted:
		// A concurrent run won, or a caller asks for the outcome again (if
		// nobody asks after a crash between steps 4 and 5, the cleaner's
		// orphan sweep deletes the staging).
		f.deleteStaging(ctx, in)
		return row.StorageKey, row, nil
	case db.StatusFailedChecksum:
		f.deleteStaging(ctx, in)
		return "", row, ErrChecksum
	default: // cancelled, expired
		f.deleteStaging(ctx, in)
		if _, err := f.repo.FindCompletedBySHA(ctx, row.SHA256); err == nil {
			return "", row, ErrDuplicate
		} else if !errors.Is(err, db.ErrNotFound) {
			return "", row, fmt.Errorf("look up completed sha256: %w", err)
		}
		return "", row, fmt.Errorf("upload is %s: %w", row.Status, ErrInactive)
	}

	// Step 1: verify the checksum, streaming.
	sum, n, err := hashStaged(ctx, in.Open)
	if err != nil {
		return "", row, err
	}
	if n != row.Size {
		return "", row, fmt.Errorf("staged data has %d bytes, want %d", n, row.Size)
	}
	if sum != row.SHA256 {
		return "", row, f.rejectChecksum(ctx, in, row)
	}

	// Step 2: final key.
	key := meta.FinalKey(row.Metadata())

	// Dedup before writing, so a known duplicate never creates an object.
	if winner, err := f.repo.FindCompletedBySHA(ctx, row.SHA256); err == nil {
		return "", row, f.rejectDuplicate(ctx, in, key, winner)
	} else if !errors.Is(err, db.ErrNotFound) {
		return "", row, fmt.Errorf("look up completed sha256: %w", err)
	}

	// Step 3: store (idempotent).
	if err := f.put(ctx, in, key, row.Size); err != nil {
		return "", row, err
	}

	// Step 4: record completion.
	done, err := f.repo.Complete(ctx, in.Protocol, in.Ref, key)
	switch {
	case errors.Is(err, db.ErrDuplicate):
		// Lost a race with another protocol/upload of the same file.
		winner, werr := f.repo.FindCompletedBySHA(ctx, row.SHA256)
		if werr != nil {
			return "", row, fmt.Errorf("look up duplicate winner: %w", werr)
		}
		return "", row, f.rejectDuplicate(ctx, in, key, winner)
	case errors.Is(err, db.ErrNotUploading):
		// The row changed under us (concurrent run, cancel or expiry).
		// Re-evaluate; the row is terminal now, so this does not loop. If it
		// was cancelled/expired the object may be orphaned: it is not
		// deleted, because another upload of the same device and file may
		// be about to complete with the same key.
		return f.run(ctx, in)
	case err != nil:
		return "", row, fmt.Errorf("record completion: %w", err)
	}

	// Step 5: delete staged data.
	f.deleteStaging(ctx, in)

	// Step 6: metrics and notification (Run logs).
	f.metrics.Completed.WithLabelValues(in.Protocol).Inc()
	f.metrics.FinalizeDuration.WithLabelValues(in.Protocol).Observe(time.Since(start).Seconds())
	if done.CompletedAt != nil {
		f.metrics.Duration.WithLabelValues(in.Protocol).Observe(done.CompletedAt.Sub(done.CreatedAt).Seconds())
	}
	if err := f.notifier.Completed(ctx, done); err != nil {
		f.log.Warn("notify completed upload", "protocol", in.Protocol, "upload_ref", in.Ref, "error", err.Error())
	}
	return key, done, nil
}

// rejectChecksum marks the row failed_checksum, then deletes the staged
// data. After a crash in between, staging is deleted by a later Run for
// the ref (a protocol layer asking for the outcome) or by the cleaner's
// orphan sweep.
func (f *Finalizer) rejectChecksum(ctx context.Context, in Input, row db.Upload) error {
	err := f.repo.SetStatus(ctx, in.Protocol, in.Ref, db.StatusFailedChecksum)
	switch {
	case errors.Is(err, db.ErrNotUploading):
		_, _, err := f.run(ctx, in) // changed concurrently; report its state
		return err
	case err != nil:
		return fmt.Errorf("mark failed_checksum: %w", err)
	}
	f.metrics.Failed.WithLabelValues(in.Protocol, obs.ReasonChecksum).Inc()
	f.deleteStaging(ctx, in)
	return fmt.Errorf("upload %s: %w", row.Ref, ErrChecksum)
}

// rejectDuplicate handles a sha256 that winner already completed. key is
// this upload's object key: if it differs from the winner's (another
// device, or another capture date), any object this upload wrote is an
// orphan and is deleted. The same key is the winner's object and is kept.
// No other upload can complete with key while winner holds the sha256, so
// the delete is safe. Then the row is cancelled and staging deleted.
func (f *Finalizer) rejectDuplicate(ctx context.Context, in Input, key string, winner db.Upload) error {
	if winner.StorageKey != key {
		if err := f.store.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete orphan object: %w", err)
		}
	}
	err := f.repo.SetStatus(ctx, in.Protocol, in.Ref, db.StatusCancelled)
	if err != nil && !errors.Is(err, db.ErrNotUploading) {
		return fmt.Errorf("cancel duplicate upload: %w", err)
	}
	if err == nil {
		f.metrics.Failed.WithLabelValues(in.Protocol, obs.ReasonDuplicate).Inc()
	}
	f.deleteStaging(ctx, in)
	return fmt.Errorf("sha256 completed by %s/%s: %w", winner.Protocol, winner.Ref, ErrDuplicate)
}

func (f *Finalizer) put(ctx context.Context, in Input, key string, size int64) error {
	rc, err := in.Open(ctx)
	if err != nil {
		return fmt.Errorf("open staged data: %w", err)
	}
	defer rc.Close() // read-only; a close error cannot lose data
	if err := f.store.Put(ctx, key, rc, size); err != nil {
		return fmt.Errorf("store object: %w", err)
	}
	return nil
}

// deleteStaging removes staged data. A failure is logged, not returned:
// the outcome is already recorded in the database, and the cleaner's
// orphan sweep deletes what is left.
func (f *Finalizer) deleteStaging(ctx context.Context, in Input) {
	if err := in.Delete(ctx); err != nil {
		f.log.Warn("delete staged data", "protocol", in.Protocol, "upload_ref", in.Ref, "error", err.Error())
	}
}

// hashStaged streams the staged file through SHA-256 and returns the hex
// digest and the byte count.
func hashStaged(ctx context.Context, open func(context.Context) (io.ReadCloser, error)) (string, int64, error) {
	rc, err := open(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("open staged data: %w", err)
	}
	defer rc.Close() // read-only; a close error cannot lose data
	h := sha256.New()
	n, err := io.Copy(h, ctxReader{ctx, rc})
	if err != nil {
		return "", n, fmt.Errorf("hash staged data: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// ctxReader stops a long read once ctx is done.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
