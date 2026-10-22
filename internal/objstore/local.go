package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Local stores objects as files below a root directory.
type Local struct {
	root string
}

var _ Store = (*Local)(nil)

// NewLocal returns a Local store rooted at dir, creating dir if needed.
func NewLocal(dir string) (*Local, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("object dir: %w", err)
	}
	return &Local{root: abs}, nil
}

func (l *Local) path(key string) (string, error) {
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	return filepath.Join(l.root, filepath.FromSlash(key)), nil
}

// Put writes r to a temp file next to the target, fsyncs it, renames it
// into place and fsyncs the directories up to the root, so a crash leaves
// either no object or the complete object.
func (l *Local) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	dst, err := l.path(key)
	if err != nil {
		return err
	}
	if got, ok, err := l.Stat(ctx, key); err != nil {
		return err
	} else if ok && got == size {
		return nil
	}
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create object dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".put-*")
	if err != nil {
		return fmt.Errorf("create temp object: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()        // already failed; the close error adds nothing
			_ = os.Remove(tmpName) // best effort cleanup of the partial temp file
		}
	}()

	if _, err := io.Copy(tmp, newExactReader(ctxReader{ctx, r}, size)); err != nil {
		return fmt.Errorf("write object: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("fsync object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close object: %w", err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("rename object: %w", err)
	}
	committed = true
	return l.syncDirs(dir)
}

// syncDirs fsyncs dir and each parent up to the root, so both the rename
// and any directories MkdirAll created survive a crash.
func (l *Local) syncDirs(dir string) error {
	for {
		if err := syncDir(dir); err != nil {
			return err
		}
		if dir == l.root || !strings.HasPrefix(dir, l.root) {
			return nil
		}
		dir = filepath.Dir(dir)
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close() // the fsync error is the one that matters
		return fmt.Errorf("fsync dir: %w", err)
	}
	return d.Close()
}

// Stat reports the size of key and whether it exists.
func (l *Local) Stat(_ context.Context, key string) (int64, bool, error) {
	p, err := l.path(key)
	if err != nil {
		return 0, false, err
	}
	fi, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("stat object: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return 0, false, fmt.Errorf("object %q is not a regular file", key)
	}
	return fi.Size(), true, nil
}

// Delete removes key; a missing key is not an error.
func (l *Local) Delete(_ context.Context, key string) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

// Ping checks that a file can be created in the root directory.
func (l *Local) Ping(context.Context) error {
	f, err := os.CreateTemp(l.root, ".ping-*")
	if err != nil {
		return fmt.Errorf("object dir not writable: %w", err)
	}
	name := f.Name()
	cerr := f.Close()
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("object dir cleanup: %w", err)
	}
	return cerr
}

// ctxReader stops a long copy once ctx is done.
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
