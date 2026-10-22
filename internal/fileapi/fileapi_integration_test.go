//go:build integration

package fileapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

var testDSN string

func TestMain(m *testing.M) {
	os.Exit(runWithPostgres(m))
}

func runWithPostgres(m *testing.M) int {
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "docker.io/library/postgres:16-alpine",
		postgres.WithDatabase("uploads"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintln(os.Stderr, "terminate postgres:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	if testDSN, err = ctr.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintln(os.Stderr, "postgres dsn:", err)
		return 1
	}
	return m.Run()
}

func randSHA(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func get(t *testing.T, h http.Handler, sha string) (int, Response) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, Prefix+sha, nil))
	var res Response
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("body %q: %v", rec.Body, err)
	}
	return rec.Code, res
}

// TestHandlerRealDB checks the lookup against real rows: only a completed
// upload is reported; in-progress and failed uploads are not_found.
func TestHandlerRealDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := db.Open(ctx, testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	repo := db.NewRepo(pool)
	h := NewHandler(Config{Repo: repo})

	insert := func(sha string) string {
		ref := uuid.Must(uuid.NewV7()).String()
		_, err := repo.InsertUploading(ctx, "rest", ref, meta.Metadata{
			DeviceID: "dev-1", SHA256: sha, Filename: "a.flac", ContentType: "audio/flac",
			CapturedAt: time.Now(), Priority: 3, Size: 4096,
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}

	completed, uploading, failed := randSHA(t), randSHA(t), randSHA(t)
	ref := insert(completed)
	before := time.Now().Add(-time.Minute)
	if _, err := repo.Complete(ctx, "rest", ref, "dev-1/key"); err != nil {
		t.Fatal(err)
	}
	insert(uploading)
	if err := repo.SetStatus(ctx, "rest", insert(failed), db.StatusFailedChecksum); err != nil {
		t.Fatal(err)
	}

	code, res := get(t, h, completed)
	if code != http.StatusOK || res.Status != StatusCompleted || res.Size != 4096 || res.SHA256 != completed {
		t.Fatalf("completed: %d %+v", code, res)
	}
	at, err := time.Parse(time.RFC3339, res.CompletedAt)
	if err != nil || at.Before(before.Truncate(time.Second)) {
		t.Errorf("completed_at = %q (%v)", res.CompletedAt, err)
	}
	for name, sha := range map[string]string{"uploading": uploading, "failed": failed, "unknown": randSHA(t)} {
		if code, res := get(t, h, sha); code != http.StatusNotFound || res.Status != StatusNotFound || res.Size != 0 {
			t.Errorf("%s: %d %+v", name, code, res)
		}
	}
}
