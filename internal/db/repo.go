package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

// Repository errors.
var (
	// ErrNotFound means no upload matches.
	ErrNotFound = errors.New("upload not found")
	// ErrNotUploading means the upload exists but is no longer 'uploading',
	// so a state change that requires it was not applied.
	ErrNotUploading = errors.New("upload is not in uploading state")
	// ErrDuplicate means another upload with the same sha256 is already
	// completed (unique index uploads_sha256_completed).
	ErrDuplicate = errors.New("sha256 already completed")
)

const (
	pgUniqueViolation = "23505"
	completedSHAIndex = "uploads_sha256_completed"
	uploadColumns     = `id, protocol, upload_ref, device_id, sha256, size_bytes, committed_bytes,
		filename, content_type, captured_at, priority, status, coalesce(storage_key, '') AS storage_key,
		resume_count, created_at, updated_at, completed_at`
	whereUploadingByRef = `WHERE protocol = $1 AND upload_ref = $2 AND status = 'uploading'`
)

// Status is the lifecycle state of an upload row.
type Status string

// Upload states (requirements §6).
const (
	StatusUploading      Status = "uploading"
	StatusCompleted      Status = "completed"
	StatusFailedChecksum Status = "failed_checksum"
	StatusCancelled      Status = "cancelled"
	StatusExpired        Status = "expired"
)

// Upload is one row of the uploads table.
type Upload struct {
	ID             uuid.UUID  `db:"id"`
	Protocol       string     `db:"protocol"`
	Ref            string     `db:"upload_ref"`
	DeviceID       string     `db:"device_id"`
	SHA256         string     `db:"sha256"`
	Size           int64      `db:"size_bytes"`
	CommittedBytes int64      `db:"committed_bytes"`
	Filename       string     `db:"filename"`
	ContentType    string     `db:"content_type"`
	CapturedAt     time.Time  `db:"captured_at"`
	Priority       int        `db:"priority"`
	Status         Status     `db:"status"`
	StorageKey     string     `db:"storage_key"` // "" until completed
	ResumeCount    int        `db:"resume_count"`
	CreatedAt      time.Time  `db:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"`
	CompletedAt    *time.Time `db:"completed_at"`
}

// Metadata returns the FR-1 metadata stored with the upload.
func (u Upload) Metadata() meta.Metadata {
	return meta.Metadata{
		DeviceID:    u.DeviceID,
		SHA256:      u.SHA256,
		Filename:    u.Filename,
		ContentType: u.ContentType,
		CapturedAt:  u.CapturedAt,
		Priority:    u.Priority,
		Size:        u.Size,
	}
}

// Repo runs the uploads table queries.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo returns a Repo using pool.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// InsertUploading inserts a new 'uploading' row with a UUIDv7 id.
func (r *Repo) InsertUploading(ctx context.Context, protocol, ref string, m meta.Metadata) (Upload, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Upload{}, fmt.Errorf("generate upload id: %w", err)
	}
	return r.one(ctx, "insert upload", `
		INSERT INTO uploads (id, protocol, upload_ref, device_id, sha256, size_bytes,
		                     filename, content_type, captured_at, priority, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'uploading')
		RETURNING `+uploadColumns,
		id, protocol, ref, m.DeviceID, m.SHA256, m.Size, m.Filename, m.ContentType, m.CapturedAt, m.Priority)
}

// Get returns the upload identified by (protocol, ref), in any state.
func (r *Repo) Get(ctx context.Context, protocol, ref string) (Upload, error) {
	return r.one(ctx, "get upload",
		`SELECT `+uploadColumns+` FROM uploads WHERE protocol = $1 AND upload_ref = $2`, protocol, ref)
}

// FindCompletedBySHA returns the completed upload with sha256 sha.
func (r *Repo) FindCompletedBySHA(ctx context.Context, sha string) (Upload, error) {
	return r.one(ctx, "find completed upload",
		`SELECT `+uploadColumns+` FROM uploads WHERE sha256 = $1 AND status = 'completed'`, sha)
}

// FindInProgress returns the most recent 'uploading' row for
// (sha, deviceID, protocol), for idempotent gRPC Init (FR-3).
func (r *Repo) FindInProgress(ctx context.Context, sha, deviceID, protocol string) (Upload, error) {
	return r.one(ctx, "find in-progress upload", `
		SELECT `+uploadColumns+` FROM uploads
		WHERE sha256 = $1 AND device_id = $2 AND protocol = $3 AND status = 'uploading'
		ORDER BY created_at DESC LIMIT 1`, sha, deviceID, protocol)
}

// UpdateCommitted sets committed_bytes (and updated_at) of an 'uploading' row.
func (r *Repo) UpdateCommitted(ctx context.Context, protocol, ref string, committed int64) error {
	return r.updateUploading(ctx, "update committed bytes", protocol, ref,
		`UPDATE uploads SET committed_bytes = $3, updated_at = now() `+whereUploadingByRef, committed)
}

// IncrResume increments resume_count (and updated_at) of an 'uploading' row.
func (r *Repo) IncrResume(ctx context.Context, protocol, ref string) error {
	return r.updateUploading(ctx, "increment resume count", protocol, ref,
		`UPDATE uploads SET resume_count = resume_count + 1, updated_at = now() `+whereUploadingByRef)
}

// Touch sets updated_at of an 'uploading' row to now (activity keeps it
// from expiring).
func (r *Repo) Touch(ctx context.Context, protocol, ref string) error {
	return r.updateUploading(ctx, "touch upload", protocol, ref,
		`UPDATE uploads SET updated_at = now() `+whereUploadingByRef)
}

// Complete marks an 'uploading' row completed with storageKey, setting
// committed_bytes = size_bytes and completed_at, in one statement. It
// returns ErrDuplicate if another row with the same sha256 is completed,
// ErrNotUploading if the row is in another state and ErrNotFound if it
// does not exist.
func (r *Repo) Complete(ctx context.Context, protocol, ref, storageKey string) (Upload, error) {
	u, err := r.one(ctx, "complete upload", `
		UPDATE uploads
		SET status = 'completed', storage_key = $3, committed_bytes = size_bytes,
		    completed_at = now(), updated_at = now() `+whereUploadingByRef+`
		RETURNING `+uploadColumns, protocol, ref, storageKey)
	if errors.Is(err, ErrNotFound) {
		return Upload{}, r.notUploadingOrNotFound(ctx, protocol, ref)
	}
	return u, err
}

// SetStatus moves an 'uploading' row to a terminal failure state
// (failed_checksum, cancelled or expired). Errors as for Complete.
func (r *Repo) SetStatus(ctx context.Context, protocol, ref string, s Status) error {
	switch s {
	case StatusFailedChecksum, StatusCancelled, StatusExpired:
	default:
		return fmt.Errorf("set status: %q is not a failure state", s)
	}
	return r.updateUploading(ctx, "set status", protocol, ref,
		`UPDATE uploads SET status = $3, updated_at = now() `+whereUploadingByRef, string(s))
}

// ListStale returns 'uploading' rows whose updated_at is older than ttl
// (by the database clock), oldest first.
func (r *Repo) ListStale(ctx context.Context, ttl time.Duration) ([]Upload, error) {
	return r.many(ctx, "list stale uploads", `
		SELECT `+uploadColumns+` FROM uploads
		WHERE status = 'uploading' AND updated_at < now() - $1::bigint * interval '1 microsecond'
		ORDER BY updated_at`, ttl.Microseconds())
}

// ListFullyCommitted returns 'uploading' rows whose committed_bytes equals
// size_bytes: all bytes were received but finalization did not finish
// (crash recovery).
func (r *Repo) ListFullyCommitted(ctx context.Context) ([]Upload, error) {
	return r.many(ctx, "list fully committed uploads", `
		SELECT `+uploadColumns+` FROM uploads
		WHERE status = 'uploading' AND committed_bytes = size_bytes
		ORDER BY created_at`)
}

func (r *Repo) one(ctx context.Context, op, sql string, args ...any) (Upload, error) {
	rows, _ := r.pool.Query(ctx, sql, args...) // the error is reported by CollectOneRow
	u, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[Upload])
	if err != nil {
		return Upload{}, mapErr(op, err)
	}
	return u, nil
}

func (r *Repo) many(ctx context.Context, op, sql string, args ...any) ([]Upload, error) {
	rows, _ := r.pool.Query(ctx, sql, args...) // the error is reported by CollectRows
	us, err := pgx.CollectRows(rows, pgx.RowToStructByName[Upload])
	if err != nil {
		return nil, mapErr(op, err)
	}
	return us, nil
}

// updateUploading runs an UPDATE restricted to the 'uploading' row
// (protocol=$1, upload_ref=$2), mapping zero affected rows to
// ErrNotUploading or ErrNotFound.
func (r *Repo) updateUploading(ctx context.Context, op, protocol, ref, sql string, extra ...any) error {
	tag, err := r.pool.Exec(ctx, sql, append([]any{protocol, ref}, extra...)...)
	if err != nil {
		return mapErr(op, err)
	}
	if tag.RowsAffected() == 0 {
		return r.notUploadingOrNotFound(ctx, protocol, ref)
	}
	return nil
}

func (r *Repo) notUploadingOrNotFound(ctx context.Context, protocol, ref string) error {
	u, err := r.Get(ctx, protocol, ref)
	if err != nil {
		return err
	}
	return fmt.Errorf("upload %s/%s is %s: %w", protocol, ref, u.Status, ErrNotUploading)
}

func mapErr(op string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == completedSHAIndex {
		return fmt.Errorf("%s: %w", op, ErrDuplicate)
	}
	return fmt.Errorf("%s: %w", op, err)
}
