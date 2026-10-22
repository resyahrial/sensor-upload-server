package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/google/uuid"
	"github.com/tus/tusd/v2/pkg/filestore"
	"github.com/tus/tusd/v2/pkg/handler"
)

// tusFiles implements cleanup.TusFiles over the tus filestore directory
// (TUS_STAGING=file, where the tus upload ID is the upload_ref). The
// cleaner calls it only while it holds the upload through
// tusapi.CleanupStaging.TryLock, so no tus request writes the files
// meanwhile.
type tusFiles struct{ store filestore.FileStore }

func newTusFiles(dir string) tusFiles { return tusFiles{store: filestore.New(dir)} }

// Offset returns the staged byte count of ref (an error matching
// fs.ErrNotExist if nothing is staged).
func (t tusFiles) Offset(ctx context.Context, ref string) (int64, error) {
	up, err := t.get(ctx, ref)
	if err != nil {
		return 0, err
	}
	info, err := up.GetInfo(ctx)
	if err != nil {
		return 0, fmt.Errorf("tus staged info %s: %w", ref, err)
	}
	return info.Offset, nil
}

// Open returns a reader over the staged bytes of ref.
func (t tusFiles) Open(ctx context.Context, ref string) (io.ReadCloser, error) {
	up, err := t.get(ctx, ref)
	if err != nil {
		return nil, err
	}
	rc, err := up.GetReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("tus staged data %s: %w", ref, err)
	}
	return rc, nil
}

func (t tusFiles) get(ctx context.Context, ref string) (handler.Upload, error) {
	// Refs are canonical UUIDs; anything else never reaches the filesystem.
	if u, err := uuid.Parse(ref); err != nil || u.String() != ref {
		return nil, fmt.Errorf("tus staged upload %q: invalid ref: %w", ref, fs.ErrNotExist)
	}
	up, err := t.store.GetUpload(ctx, ref)
	if errors.Is(err, handler.ErrNotFound) {
		return nil, fmt.Errorf("tus staged upload %s: %w", ref, fs.ErrNotExist)
	}
	if err != nil {
		return nil, fmt.Errorf("tus staged upload %s: %w", ref, err)
	}
	return up, nil
}
