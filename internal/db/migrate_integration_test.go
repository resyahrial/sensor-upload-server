//go:build integration

package db

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func insertUpload(t *testing.T, pool *pgxpool.Pool, protocol, ref, sha, status string) error {
	t.Helper()
	_, err := pool.Exec(testCtx(t), `
		INSERT INTO uploads (id, protocol, upload_ref, device_id, sha256, size_bytes,
		                     filename, content_type, captured_at, priority, status)
		VALUES ($1, $2, $3, 'bioac-0142', $4, 10, 'f.flac', 'audio/flac', now(), 2, $5)`,
		uuid.Must(uuid.NewV7()), protocol, ref, sha, status)
	return err
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestMigrateCreatesSchemaIdempotently(t *testing.T) {
	ctx := testCtx(t)
	pool, err := Open(ctx, testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	for range 2 {
		if err := Migrate(ctx, pool); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	sha := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	if err := insertUpload(t, pool, "rest", "a", sha, "completed"); err != nil {
		t.Fatalf("insert completed: %v", err)
	}
	// An in-progress upload of an already completed sha is allowed (FR-3 tus).
	if err := insertUpload(t, pool, "grpc", "b", sha, "uploading"); err != nil {
		t.Fatalf("insert uploading duplicate: %v", err)
	}

	tests := []struct {
		name                  string
		protocol, ref, status string
		wantCode              string
	}{
		{"second completed sha", "grpc", "c", "completed", "23505"},
		{"duplicate protocol+ref", "rest", "a", "uploading", "23505"},
		{"bad protocol", "ftp", "d", "uploading", "23514"},
		{"bad status", "rest", "e", "done", "23514"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := insertUpload(t, pool, tt.protocol, tt.ref, sha, tt.status)
			if got := pgCode(err); got != tt.wantCode {
				t.Fatalf("insert error = %v (code %q), want code %s", err, got, tt.wantCode)
			}
		})
	}
}
