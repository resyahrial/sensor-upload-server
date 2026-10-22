package app

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/google/uuid"
	"github.com/tus/tusd/v2/pkg/filestore"
	"github.com/tus/tusd/v2/pkg/handler"
)

func TestTusFiles(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	ref := uuid.Must(uuid.NewV7()).String()
	up, err := filestore.New(dir).NewUpload(ctx, handler.FileInfo{ID: ref, Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := up.WriteChunk(ctx, 0, bytes.NewReader([]byte("abc"))); err != nil {
		t.Fatal(err)
	}

	tf := newTusFiles(dir)
	if n, err := tf.Offset(ctx, ref); err != nil || n != 3 {
		t.Fatalf("Offset = %d, %v; want 3", n, err)
	}
	rc, err := tf.Open(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(b) != "abc" {
		t.Fatalf("Open read %q, %v", b, err)
	}

	for _, bad := range []string{uuid.Must(uuid.NewV7()).String(), "../etc/passwd", "ABC"} {
		if _, err := tf.Offset(ctx, bad); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Offset(%q) = %v, want fs.ErrNotExist", bad, err)
		}
		if _, err := tf.Open(ctx, bad); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Open(%q) = %v, want fs.ErrNotExist", bad, err)
		}
	}
}
