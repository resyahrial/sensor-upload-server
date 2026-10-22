package staging_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/resyahrial/sensor-upload-server/internal/staging"
)

const id = "0192b7e2-6c1d-7a3e-9f00-123456789abc"

func newStore(t *testing.T) *staging.Store {
	t.Helper()
	s, err := staging.New(filepath.Join(t.TempDir(), "grpc"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestWriteReopenTruncateDelete(t *testing.T) {
	s := newStore(t)
	info := staging.Info{DeviceID: "bioac-0142", SHA256: strings.Repeat("a", 64), Size: 10, Priority: 2}
	f, err := s.Create(id, info)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if n, err := f.WriteAt([]byte("hello"), 0); err != nil || n != 5 {
		t.Fatalf("WriteAt = %d, %v", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := s.LoadInfo(id)
	if err != nil {
		t.Fatalf("LoadInfo: %v", err)
	}
	info.UploadID = id
	if got != info {
		t.Fatalf("LoadInfo = %+v, want %+v", got, info)
	}

	// Reopen, simulate a partially written chunk beyond the committed offset.
	f, err = s.Open(id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := f.WriteAt([]byte("XYZ"), 5); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Truncate(5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if size, err := f.Size(); err != nil || size != 5 {
		t.Fatalf("Size = %d, %v", size, err)
	}
	if err := f.Truncate(6); !errors.Is(err, staging.ErrShortFile) {
		t.Fatalf("Truncate beyond size = %v, want ErrShortFile", err)
	}
	if _, err := f.WriteAt([]byte("world"), 5); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info.CommittedOffset = 10
	if err := s.SaveInfo(id, info); err != nil {
		t.Fatalf("SaveInfo: %v", err)
	}
	if got, _ := s.LoadInfo(id); got.CommittedOffset != 10 {
		t.Fatalf("CommittedOffset = %d", got.CommittedOffset)
	}

	r, err := s.Reader(id)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	b, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || string(b) != "helloworld" {
		t.Fatalf("ReadAll = %q, %v", b, err)
	}

	entries, _ := os.ReadDir(s.Dir())
	if len(entries) != 2 {
		t.Fatalf("staging dir has %d entries, want 2 (no temp files)", len(entries))
	}

	if err := s.Delete(id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(id); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if _, err := s.Open(id); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open after delete = %v, want ErrNotExist", err)
	}
	if _, err := s.LoadInfo(id); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadInfo after delete = %v, want ErrNotExist", err)
	}
	if _, err := s.Reader(id); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Reader after delete = %v, want ErrNotExist", err)
	}
}

func TestCreateTwiceFails(t *testing.T) {
	s := newStore(t)
	f, err := s.Create(id, staging.Info{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_ = f.Close()
	if _, err := s.Create(id, staging.Info{}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second Create = %v, want ErrExist", err)
	}
}

func TestInvalidIDRejected(t *testing.T) {
	s := newStore(t)
	bad := []string{
		"",
		"../../etc/passwd",
		"not-a-uuid",
		strings.ToUpper(id),
		"{" + id + "}",
		"urn:uuid:" + id,
		strings.ReplaceAll(id, "-", ""),
	}
	for _, b := range bad {
		calls := map[string]error{}
		_, calls["Create"] = s.Create(b, staging.Info{})
		_, calls["Open"] = s.Open(b)
		_, calls["Reader"] = s.Reader(b)
		_, calls["LoadInfo"] = s.LoadInfo(b)
		calls["SaveInfo"] = s.SaveInfo(b, staging.Info{})
		calls["Delete"] = s.Delete(b)
		for name, err := range calls {
			if !errors.Is(err, staging.ErrInvalidID) {
				t.Errorf("%s(%q) = %v, want ErrInvalidID", name, b, err)
			}
		}
	}
	if entries, _ := os.ReadDir(s.Dir()); len(entries) != 0 {
		t.Fatalf("invalid ids created %d files", len(entries))
	}
}

func TestLoadInfoCorrupt(t *testing.T) {
	s := newStore(t)
	if err := os.WriteFile(filepath.Join(s.Dir(), id+".json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadInfo(id); err == nil {
		t.Fatal("LoadInfo of corrupt json: want error")
	}
}

func TestNewFailsOnFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := staging.New(filepath.Join(p, "sub")); err == nil {
		t.Fatal("New under a regular file: want error")
	}
}

func TestSaveInfoFailsWhenDirGone(t *testing.T) {
	s := newStore(t)
	if err := os.RemoveAll(s.Dir()); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveInfo(id, staging.Info{}); err == nil {
		t.Fatal("SaveInfo without dir: want error")
	}
	if _, err := s.Create(id, staging.Info{}); err == nil {
		t.Fatal("Create without dir: want error")
	}
}

func TestClosedFileErrors(t *testing.T) {
	s := newStore(t)
	f, err := s.Create(id, staging.Info{})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("x"), 0); err == nil {
		t.Error("WriteAt on closed file: want error")
	}
	if err := f.Truncate(0); err == nil {
		t.Error("Truncate on closed file: want error")
	}
	if err := f.Close(); err == nil {
		t.Error("double Close: want error")
	}
}
