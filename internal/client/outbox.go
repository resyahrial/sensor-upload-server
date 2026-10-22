package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver
)

// Outbox row statuses (contract §5.1).
const (
	StatusQueued    = "queued"
	StatusUploading = "uploading"
	StatusDone      = "done"
	StatusFailed    = "failed"
)

// schema holds the contract §5.1 columns first, then the file metadata fixed
// at queue time and the counters the report needs across restarts.
const schema = `
CREATE TABLE IF NOT EXISTS outbox (
	sha256        TEXT PRIMARY KEY,
	path          TEXT NOT NULL,
	priority      INTEGER NOT NULL,
	protocol      TEXT NOT NULL,
	upload_ref    TEXT NOT NULL DEFAULT '',
	status        TEXT NOT NULL CHECK (status IN ('queued','uploading','done','failed')),
	attempts      INTEGER NOT NULL DEFAULT 0,
	next_retry_at INTEGER NOT NULL DEFAULT 0,
	size          INTEGER NOT NULL,
	filename      TEXT NOT NULL,
	content_type  TEXT NOT NULL,
	captured_at   TEXT NOT NULL,
	resumes       INTEGER NOT NULL DEFAULT 0,
	bytes_sent    INTEGER NOT NULL DEFAULT 0,
	bytes_resent  INTEGER NOT NULL DEFAULT 0,
	sent_hwm      INTEGER NOT NULL DEFAULT 0,
	result        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS outbox_path ON outbox (path);
`

const columns = `sha256, path, priority, protocol, upload_ref, status, attempts, next_retry_at,
	size, filename, content_type, captured_at, resumes, bytes_sent, bytes_resent, sent_hwm, result`

// Row is one file in the outbox.
type Row struct {
	SHA256      string
	Path        string
	Priority    int
	Protocol    string
	UploadRef   string // tus upload URL or gRPC upload_id
	Status      string
	Attempts    int
	NextRetryAt time.Time

	Size        int64
	Filename    string
	ContentType string
	CapturedAt  string // RFC 3339

	Resumes     int
	BytesSent   int64
	BytesResent int64
	// SentHWM is the highest file offset ever handed to the network. It is
	// written before each chunk is sent, so re-sends after a kill are counted.
	SentHWM int64
	Result  string
}

// Outbox is the SQLite-backed upload queue.
type Outbox struct {
	db *sql.DB
}

// OpenOutbox opens (creating if needed) the outbox database at path.
func OpenOutbox(ctx context.Context, path string) (*Outbox, error) {
	dsn := "file:" + (&url.URL{Path: path}).EscapedPath() +
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open outbox: %w", err)
	}
	// One connection serializes writers from concurrent workers and avoids
	// SQLITE_BUSY; the writes are tiny.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, errors.Join(fmt.Errorf("init outbox: %w", err), db.Close())
	}
	return &Outbox{db: db}, nil
}

// Close closes the database.
func (o *Outbox) Close() error { return o.db.Close() }

type scanner interface{ Scan(dest ...any) error }

func scanRow(s scanner) (Row, error) {
	var r Row
	var next int64
	err := s.Scan(&r.SHA256, &r.Path, &r.Priority, &r.Protocol, &r.UploadRef, &r.Status, &r.Attempts, &next,
		&r.Size, &r.Filename, &r.ContentType, &r.CapturedAt, &r.Resumes, &r.BytesSent, &r.BytesResent, &r.SentHWM, &r.Result)
	if next != 0 {
		r.NextRetryAt = time.UnixMilli(next)
	}
	return r, err
}

func unixMilli(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// ByPath returns the most relevant row recorded for path, if any.
func (o *Outbox) ByPath(ctx context.Context, path string) (Row, bool, error) {
	r, err := scanRow(o.db.QueryRowContext(ctx,
		`SELECT `+columns+` FROM outbox WHERE path = ? ORDER BY status = 'done' DESC LIMIT 1`, path))
	if errors.Is(err, sql.ErrNoRows) {
		return Row{}, false, nil
	}
	if err != nil {
		return Row{}, false, fmt.Errorf("outbox by path: %w", err)
	}
	return r, true, nil
}

// Get returns the row for sha.
func (o *Outbox) Get(ctx context.Context, sha string) (Row, error) {
	r, err := scanRow(o.db.QueryRowContext(ctx, `SELECT `+columns+` FROM outbox WHERE sha256 = ?`, sha))
	if err != nil {
		return Row{}, fmt.Errorf("outbox get %s: %w", sha, err)
	}
	return r, nil
}

// Insert adds r unless a row with the same sha256 exists, and returns the
// stored row.
func (o *Outbox) Insert(ctx context.Context, r Row) (Row, error) {
	_, err := o.db.ExecContext(ctx, `INSERT INTO outbox (`+columns+`)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (sha256) DO NOTHING`,
		r.SHA256, r.Path, r.Priority, r.Protocol, r.UploadRef, r.Status, r.Attempts, unixMilli(r.NextRetryAt),
		r.Size, r.Filename, r.ContentType, r.CapturedAt, r.Resumes, r.BytesSent, r.BytesResent, r.SentHWM, r.Result)
	if err != nil {
		return Row{}, fmt.Errorf("outbox insert: %w", err)
	}
	return o.Get(ctx, r.SHA256)
}

// Save writes every mutable field of r.
func (o *Outbox) Save(ctx context.Context, r Row) error {
	_, err := o.db.ExecContext(ctx, `UPDATE outbox SET path = ?, priority = ?, protocol = ?, upload_ref = ?,
		status = ?, attempts = ?, next_retry_at = ?, size = ?, filename = ?, content_type = ?, captured_at = ?,
		resumes = ?, bytes_sent = ?, bytes_resent = ?, sent_hwm = MAX(sent_hwm, ?), result = ? WHERE sha256 = ?`,
		r.Path, r.Priority, r.Protocol, r.UploadRef, r.Status, r.Attempts, unixMilli(r.NextRetryAt),
		r.Size, r.Filename, r.ContentType, r.CapturedAt, r.Resumes, r.BytesSent, r.BytesResent, r.SentHWM, r.Result,
		r.SHA256)
	if err != nil {
		return fmt.Errorf("outbox save %s: %w", r.SHA256, err)
	}
	return nil
}

// SaveHWM records the sent high-water mark ahead of sending a chunk.
func (o *Outbox) SaveHWM(ctx context.Context, sha string, hwm int64) error {
	_, err := o.db.ExecContext(ctx, `UPDATE outbox SET sent_hwm = MAX(sent_hwm, ?) WHERE sha256 = ?`, hwm, sha)
	if err != nil {
		return fmt.Errorf("outbox save hwm %s: %w", sha, err)
	}
	return nil
}

// Rekey replaces the row identified by oldSHA with r (used when a re-read
// file has a different SHA-256).
func (o *Outbox) Rekey(ctx context.Context, oldSHA string, r Row) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("outbox rekey: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit
	if _, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE sha256 = ? OR sha256 = ?`, oldSHA, r.SHA256); err != nil {
		return fmt.Errorf("outbox rekey: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox (`+columns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.SHA256, r.Path, r.Priority, r.Protocol, r.UploadRef, r.Status, r.Attempts, unixMilli(r.NextRetryAt),
		r.Size, r.Filename, r.ContentType, r.CapturedAt, r.Resumes, r.BytesSent, r.BytesResent, r.SentHWM, r.Result); err != nil {
		return fmt.Errorf("outbox rekey: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("outbox rekey: %w", err)
	}
	return nil
}
