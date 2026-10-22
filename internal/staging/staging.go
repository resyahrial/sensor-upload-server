// Package staging stores partial gRPC uploads on the local filesystem.
//
// Each upload is two files in the staging directory: {upload_id}.bin holds the
// received bytes and {upload_id}.json holds an Info record. Paths are built
// only from a validated, canonical UUID, so client input never reaches the
// filesystem otherwise. Writes are fsynced before they return, and info files
// are replaced atomically (temp file + fsync + rename + directory fsync).
package staging

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

// ErrInvalidID is returned when an upload ID is not a canonical UUID.
var ErrInvalidID = errors.New("staging: invalid upload id")

// ErrShortFile is returned by File.Truncate when the staged file holds fewer
// bytes than the requested offset, meaning acked data is missing.
var ErrShortFile = errors.New("staging: staged file shorter than offset")

// Info is the metadata stored next to a staged upload. The gRPC service
// writes it once when the upload is created; CommittedOffset is the offset
// at that time (0), not the live one, which lives in the DB (FR-7: the DB
// offset is authoritative and the data file is truncated to it on resume).
type Info struct {
	UploadID        string `json:"upload_id"`
	DeviceID        string `json:"device_id"`
	SHA256          string `json:"sha256"`
	Size            int64  `json:"size"`
	Filename        string `json:"filename"`
	ContentType     string `json:"content_type"`
	CapturedAt      string `json:"captured_at"`
	Priority        int32  `json:"priority"`
	CommittedOffset int64  `json:"committed_offset"`
}

// Store manages staged uploads under one directory. It holds no mutable state
// and is safe for concurrent use; callers serialize access to a single upload.
type Store struct {
	dir string
}

// New returns a Store rooted at dir, creating the directory if needed.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("staging: create dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

// Dir returns the staging directory.
func (s *Store) Dir() string { return s.dir }

// ValidateID reports ErrInvalidID unless id is a canonical (lowercase,
// hyphenated) UUID.
func ValidateID(id string) error {
	u, err := uuid.Parse(id)
	if err != nil || u.String() != id {
		return fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	return nil
}

func (s *Store) binPath(id string) string  { return filepath.Join(s.dir, id+".bin") }
func (s *Store) infoPath(id string) string { return filepath.Join(s.dir, id+".json") }

// Create creates an empty data file and its info record for id. It fails if
// the upload already exists.
func (s *Store) Create(id string, info Info) (*File, error) {
	if err := ValidateID(id); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.binPath(id), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("staging: create %s: %w", id, err)
	}
	if err := f.Sync(); err != nil {
		return nil, closeWith(f, fmt.Errorf("staging: sync %s: %w", id, err))
	}
	info.UploadID = id
	if err := s.SaveInfo(id, info); err != nil {
		return nil, closeWith(f, err)
	}
	return &File{f: f, id: id}, nil
}

// Open opens the data file of an existing upload for writing.
func (s *Store) Open(id string) (*File, error) {
	if err := ValidateID(id); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.binPath(id), os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("staging: open %s: %w", id, err)
	}
	return &File{f: f, id: id}, nil
}

// Reader opens the data file of an upload read-only, e.g. for the finalizer.
func (s *Store) Reader(id string) (*os.File, error) {
	if err := ValidateID(id); err != nil {
		return nil, err
	}
	f, err := os.Open(s.binPath(id))
	if err != nil {
		return nil, fmt.Errorf("staging: read %s: %w", id, err)
	}
	return f, nil
}

// LoadInfo reads the info record of an upload.
func (s *Store) LoadInfo(id string) (Info, error) {
	if err := ValidateID(id); err != nil {
		return Info{}, err
	}
	b, err := os.ReadFile(s.infoPath(id))
	if err != nil {
		return Info{}, fmt.Errorf("staging: load info %s: %w", id, err)
	}
	var info Info
	if err := json.Unmarshal(b, &info); err != nil {
		return Info{}, fmt.Errorf("staging: decode info %s: %w", id, err)
	}
	return info, nil
}

// SaveInfo atomically replaces the info record of an upload.
func (s *Store) SaveInfo(id string, info Info) error {
	if err := ValidateID(id); err != nil {
		return err
	}
	b, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("staging: encode info %s: %w", id, err)
	}
	tmp, err := os.CreateTemp(s.dir, id+".json.tmp-*")
	if err != nil {
		return fmt.Errorf("staging: save info %s: %w", id, err)
	}
	tmpName := tmp.Name()
	if err := writeSyncClose(tmp, b); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("staging: save info %s: %w", id, err)
	}
	if err := os.Rename(tmpName, s.infoPath(id)); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("staging: save info %s: %w", id, err)
	}
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("staging: save info %s: %w", id, err)
	}
	return nil
}

// Delete removes the data and info files of an upload. Missing files are not
// an error.
func (s *Store) Delete(id string) error {
	if err := ValidateID(id); err != nil {
		return err
	}
	var errs []error
	for _, p := range []string{s.binPath(id), s.infoPath(id)} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("staging: delete %s: %w", id, err)
	}
	return nil
}

// File is an open staged data file.
type File struct {
	f  *os.File
	id string
}

// WriteAt writes p at off and fsyncs the file before returning, so the bytes
// are durable once it returns nil.
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.f.WriteAt(p, off)
	if err != nil {
		return n, fmt.Errorf("staging: write %s at %d: %w", f.id, off, err)
	}
	if err := f.f.Sync(); err != nil {
		return n, fmt.Errorf("staging: sync %s: %w", f.id, err)
	}
	return n, nil
}

// Truncate cuts the file to off bytes (the authoritative committed offset on
// resume) and fsyncs it. It returns ErrShortFile if the file is shorter.
func (f *File) Truncate(off int64) error {
	size, err := f.Size()
	if err != nil {
		return err
	}
	if size < off {
		return fmt.Errorf("%w: %s has %d bytes, want %d", ErrShortFile, f.id, size, off)
	}
	if err := f.f.Truncate(off); err != nil {
		return fmt.Errorf("staging: truncate %s: %w", f.id, err)
	}
	if err := f.f.Sync(); err != nil {
		return fmt.Errorf("staging: sync %s: %w", f.id, err)
	}
	return nil
}

// Size returns the current size of the data file.
func (f *File) Size() (int64, error) {
	st, err := f.f.Stat()
	if err != nil {
		return 0, fmt.Errorf("staging: stat %s: %w", f.id, err)
	}
	return st.Size(), nil
}

// Close closes the data file.
func (f *File) Close() error {
	if err := f.f.Close(); err != nil {
		return fmt.Errorf("staging: close %s: %w", f.id, err)
	}
	return nil
}

func writeSyncClose(f *os.File, b []byte) error {
	if _, err := f.Write(b); err != nil {
		return closeWith(f, err)
	}
	if err := f.Sync(); err != nil {
		return closeWith(f, err)
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		return closeWith(d, err)
	}
	return d.Close()
}

// closeWith closes f and returns err joined with any close error.
func closeWith(f *os.File, err error) error {
	return errors.Join(err, f.Close())
}
