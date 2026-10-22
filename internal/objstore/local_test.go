package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateKey(t *testing.T) {
	tests := []struct {
		key string
		ok  bool
	}{
		{"bioac-0142/2026/10/21/abc.flac", true},
		{"a", true},
		{"", false},
		{"/etc/passwd", false},
		{"a/../b", false},
		{"..", false},
		{"a/./b", false},
		{"a//b", false},
		{"a/", false},
		{`a\b`, false},
		{"a\x00b", false},
	}
	for _, tt := range tests {
		err := ValidateKey(tt.key)
		if (err == nil) != tt.ok {
			t.Errorf("ValidateKey(%q) = %v, want ok=%v", tt.key, err, tt.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalidKey) {
			t.Errorf("ValidateKey(%q) error %v does not wrap ErrInvalidKey", tt.key, err)
		}
	}
}

func newLocal(t *testing.T) *Local {
	t.Helper()
	s, err := NewLocal(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// storeContract runs behaviour shared by every Store implementation.
func storeContract(t *testing.T, s Store) {
	ctx := context.Background()
	const key = "dev-1/2026/10/21/abc.bin"
	data := []byte("hello object store")

	if _, ok, err := s.Stat(ctx, key); err != nil || ok {
		t.Fatalf("Stat before Put = ok %v, err %v", ok, err)
	}
	if err := s.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	size, ok, err := s.Stat(ctx, key)
	if err != nil || !ok || size != int64(len(data)) {
		t.Fatalf("Stat = %d, %v, %v", size, ok, err)
	}

	// Idempotent: same size → success without reading the source.
	if err := s.Put(ctx, key, failingReader{}, int64(len(data))); err != nil {
		t.Fatalf("idempotent Put: %v", err)
	}

	// Short and long sources are rejected and leave no object.
	if err := s.Put(ctx, "dev-1/short", strings.NewReader("abc"), 10); err == nil {
		t.Fatal("short source: want error")
	}
	if err := s.Put(ctx, "dev-1/long", strings.NewReader("abcdef"), 3); err == nil {
		t.Fatal("long source: want error")
	}
	for _, k := range []string{"dev-1/short", "dev-1/long"} {
		if _, ok, err := s.Stat(ctx, k); err != nil || ok {
			t.Fatalf("Stat(%s) after failed Put = ok %v, err %v", k, ok, err)
		}
	}

	if err := s.Put(ctx, "../escape", strings.NewReader("x"), 1); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Put traversal = %v, want ErrInvalidKey", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
	if _, ok, _ := s.Stat(ctx, key); ok {
		t.Fatal("object still exists after Delete")
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("must not be read") }

func TestLocalContract(t *testing.T) {
	storeContract(t, newLocal(t))
}

func TestLocalPutLeavesNoTempFiles(t *testing.T) {
	s := newLocal(t)
	ctx := context.Background()
	_ = s.Put(ctx, "d/x", strings.NewReader("ab"), 5) // fails: short source
	if err := s.Put(ctx, "d/y", strings.NewReader("abcde"), 5); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "d"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "y" {
		t.Fatalf("dir entries = %v, want only y", entries)
	}
}

func TestLocalPutReplacesDifferentSize(t *testing.T) {
	s := newLocal(t)
	ctx := context.Background()
	if err := s.Put(ctx, "k", strings.NewReader("abc"), 3); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "k", strings.NewReader("abcdef"), 6); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(s.root, "k"))
	if err != nil || string(got) != "abcdef" {
		t.Fatalf("content = %q, %v", got, err)
	}
}

func TestLocalPutHonoursContext(t *testing.T) {
	s := newLocal(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Put(ctx, "k", strings.NewReader("abc"), 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put = %v, want context.Canceled", err)
	}
}

func TestLocalStatNotRegular(t *testing.T) {
	s := newLocal(t)
	if err := os.MkdirAll(filepath.Join(s.root, "dir"), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Stat(context.Background(), "dir"); err == nil {
		t.Fatal("Stat on directory: want error")
	}
}

func TestLocalPingFailsWhenRootMissing(t *testing.T) {
	s := newLocal(t)
	if err := os.RemoveAll(s.root); err != nil {
		t.Fatal(err)
	}
	if err := s.Ping(context.Background()); err == nil {
		t.Fatal("Ping: want error")
	}
}

func TestExactReader(t *testing.T) {
	got, err := io.ReadAll(newExactReader(strings.NewReader("abc"), 3))
	if err != nil || string(got) != "abc" {
		t.Fatalf("exact = %q, %v", got, err)
	}
	if _, err := io.ReadAll(newExactReader(strings.NewReader("ab"), 3)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short = %v", err)
	}
	if _, err := io.ReadAll(newExactReader(strings.NewReader("abcd"), 3)); err == nil {
		t.Fatal("long: want error")
	}
}
