// Package grpcapi implements the gRPC UploadService (FR-6): Init, the bidi
// Upload stream, Status and Cancel, plus the auth interceptors, the
// grpc.Server construction and the /grpc prefix routing for the TLS listener.
//
// Storage and completion are reached through the small consumer-side
// interfaces Repo and Finalizer, so the wiring in cmd/server adapts the real
// db and finalize packages with thin adapters.
package grpcapi

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

// Protocol is the value of uploads.protocol and of the protocol metric label
// for every upload handled by this package.
const Protocol = "grpc"

// Upload statuses (uploads.status, requirements §6).
const (
	StatusUploading      = "uploading"
	StatusCompleted      = "completed"
	StatusFailedChecksum = "failed_checksum"
	StatusCancelled      = "cancelled"
	StatusExpired        = "expired"
)

var (
	// ErrNotFound is returned (possibly wrapped) by Repo lookups that match
	// no row.
	ErrNotFound = errors.New("grpcapi: upload not found")
	// ErrChecksumMismatch must be wrapped by Finalizer.Run when the staged
	// bytes do not match the metadata sha256 (→ DATA_LOSS).
	ErrChecksumMismatch = errors.New("grpcapi: sha-256 mismatch")
	// ErrDuplicate must be wrapped by Finalizer.Run when another upload with
	// the same sha256 completed first (→ ALREADY_EXISTS).
	ErrDuplicate = errors.New("grpcapi: duplicate upload")
)

// Upload is the part of an uploads row this package works with. The row id
// and upload_ref are the same UUIDv7 (ID).
type Upload struct {
	ID        string
	Meta      meta.Metadata
	Committed int64
	Status    string
	CreatedAt time.Time
}

// Repo is the uploads table, restricted to protocol = 'grpc' rows. Every
// method must be safe for concurrent use.
type Repo interface {
	// Insert stores a new row with id = upload_ref = u.ID, protocol 'grpc',
	// status 'uploading' and committed_bytes 0.
	Insert(ctx context.Context, u Upload) error
	// Get returns the row with protocol 'grpc' and upload_ref id, in any
	// status, or an error wrapping ErrNotFound.
	Get(ctx context.Context, id string) (Upload, error)
	// CompletedExists reports whether any row (any protocol) with this
	// sha256 has status 'completed'.
	CompletedExists(ctx context.Context, sha256 string) (bool, error)
	// FindInProgress returns the most recently created 'uploading' row with
	// protocol 'grpc' for this sha256 and device, or an error wrapping
	// ErrNotFound.
	FindInProgress(ctx context.Context, sha256, deviceID string) (Upload, error)
	// UpdateCommitted sets committed_bytes (and updated_at) of the row.
	UpdateCommitted(ctx context.Context, id string, committed int64) error
	// IncrementResumes adds one to resume_count (and sets updated_at).
	IncrementResumes(ctx context.Context, id string) error
	// MarkCancelled sets status 'cancelled' (and updated_at).
	MarkCancelled(ctx context.Context, id string) error
}

// FinalizeInput describes a fully received upload (FR-8 input).
type FinalizeInput struct {
	// Protocol is always "grpc".
	Protocol string
	// Ref is the upload_ref (= upload_id).
	Ref string
	// Meta is the validated metadata, including Size.
	Meta meta.Metadata
	// CreatedAt is when the upload was created (for upload_duration_seconds).
	CreatedAt time.Time
	// Open opens the staged bytes for reading from the start.
	Open func() (io.ReadCloser, error)
	// Delete removes the staging data; missing data is not an error.
	Delete func() error
}

// Finalizer is the shared completion pipeline (FR-8).
type Finalizer interface {
	// Run verifies the SHA-256, stores the object and marks the row
	// completed, then deletes the staging data. On a checksum mismatch it
	// marks the row failed_checksum, deletes the staging data and returns an
	// error wrapping ErrChecksumMismatch. On a unique violation (another
	// upload of the same sha256 completed first) it returns an error
	// wrapping ErrDuplicate. If the row is no longer 'uploading' (cancelled
	// or expired meanwhile) or does not exist, it returns an error wrapping
	// ErrNotFound (→ NOT_FOUND). Any other error is treated as transient: the
	// row stays 'uploading' with every byte committed, and Run is retried on
	// the next Init or Status. Run must be idempotent, and it emits the
	// completed/failed/finalize/duration metrics itself.
	Run(ctx context.Context, in FinalizeInput) error
}
