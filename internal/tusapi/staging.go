package tusapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	"github.com/tus/tusd/v2/pkg/handler"
	"github.com/tus/tusd/v2/pkg/s3store"

	"github.com/resyahrial/sensor-upload-server/internal/finalize"
)

// Lock hand-off timing (filelocker defaults are 5 s / 2 s). A resuming
// client's HEAD waits for the previous, possibly dead, PATCH to give up
// the lock, so shorter polls make resume faster at the cost of a stat per
// interval per open request.
const (
	lockHolderPoll   = time.Second
	lockAcquirerPoll = 250 * time.Millisecond
)

// S3StagingPrefix is the key prefix for tus uploads staged in S3, keeping
// them apart from final objects ({device_id}/...).
const S3StagingPrefix = "tus-staging/"

// Staging is the tus staging store (filestore or s3store) and the locker,
// shared by every tus handler (/rest/files/ and /rest-http3/files/).
type Staging struct {
	core   handler.DataStore
	term   handler.TerminaterDataStore
	locker handler.Locker
	// idIsRef is true when the tus upload ID equals the DB upload_ref
	// (filestore). s3store appends "+<multipart id>" to the ID.
	idIsRef bool
	// use tracks tus requests per upload_ref against the expiry cleaner.
	use *useRegistry
}

// NewFileStaging returns a filestore + filelocker staging under dir. It
// removes stale lock files first: the lockfile library treats a lock whose
// PID is alive as held, including our own PID (always 1 in a container),
// so a lock left by a crash could never be acquired again. Only one
// server instance may use dir.
func NewFileStaging(dir string) (*Staging, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create tus staging dir: %w", err)
	}
	if err := clearStaleLocks(dir); err != nil {
		return nil, err
	}
	fs := filestore.New(dir)
	return &Staging{core: fs, term: fs, locker: newLocker(dir), idIsRef: true, use: newUseRegistry()}, nil
}

// NewS3Staging returns an s3store staging in bucket (under S3StagingPrefix)
// with a filelocker in lockDir. s3store keeps chunks smaller than 5 MiB as
// a separate part object that is re-downloaded on the next PATCH, so it
// suits large chunks only.
func NewS3Staging(lockDir, bucket string, client s3store.S3API) (*Staging, error) {
	if err := os.MkdirAll(lockDir, 0o750); err != nil {
		return nil, fmt.Errorf("create tus lock dir: %w", err)
	}
	if err := clearStaleLocks(lockDir); err != nil {
		return nil, err
	}
	s := s3store.New(bucket, client)
	s.ObjectPrefix = S3StagingPrefix
	return &Staging{core: s, term: s, locker: newLocker(lockDir), use: newUseRegistry()}, nil
}

func newLocker(dir string) filelocker.FileLocker {
	l := filelocker.New(dir)
	l.HolderPollInterval = lockHolderPoll
	l.AcquirerPollInterval = lockAcquirerPoll
	return l
}

// clearStaleLocks deletes *.lock and *.stop files in dir. Upload files are
// named {id} and {id}.info, so nothing else matches.
func clearStaleLocks(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read tus staging dir: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !(strings.HasSuffix(name, ".lock") || strings.HasSuffix(name, ".stop")) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale lock: %w", err)
		}
	}
	return nil
}

// composer returns a store composer with core, termination and locking
// only (no concatenation, deferred length or download).
func (s *Staging) composer() *handler.StoreComposer {
	c := handler.NewStoreComposer()
	c.UseCore(s.core)
	c.UseTerminater(s.term)
	c.UseLocker(s.locker)
	return c
}

// info returns the staged upload's info (Offset = bytes staged). It
// returns an error matching handler.ErrNotFound if nothing is staged.
func (s *Staging) info(ctx context.Context, id string) (handler.FileInfo, error) {
	up, err := s.core.GetUpload(ctx, id)
	if err != nil {
		return handler.FileInfo{}, err
	}
	return up.GetInfo(ctx)
}

// lock acquires the upload lock for id, ignoring release requests from
// other requests (the caller's work must finish).
func (s *Staging) lock(ctx context.Context, id string) (handler.Lock, error) {
	l, err := s.locker.NewLock(id)
	if err != nil {
		return nil, fmt.Errorf("new lock: %w", err)
	}
	if err := l.Lock(ctx, func() {}); err != nil {
		return nil, fmt.Errorf("acquire lock: %w", err)
	}
	return l, nil
}

// input builds the finalizer input for the staged tus upload id, recorded
// in the DB as (protocol, ref).
func (s *Staging) input(protocol, ref, id string) finalize.Input {
	return finalize.Input{
		Protocol: protocol,
		Ref:      ref,
		Open: func(ctx context.Context) (io.ReadCloser, error) {
			up, err := s.core.GetUpload(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("get staged upload: %w", err)
			}
			return up.GetReader(ctx)
		},
		Delete: func(ctx context.Context) error {
			up, err := s.core.GetUpload(ctx, id)
			if errors.Is(err, handler.ErrNotFound) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("get staged upload: %w", err)
			}
			if err := s.term.AsTerminatableUpload(up).Terminate(ctx); err != nil {
				return fmt.Errorf("terminate staged upload: %w", err)
			}
			return nil
		},
	}
}
