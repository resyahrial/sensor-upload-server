// Package objstore stores finalized uploads in an object store: a local
// directory (dev) or an S3-compatible bucket (MinIO, AWS S3).
package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrInvalidKey is returned for keys that are empty, absolute, contain a
// backslash, NUL, or an empty, "." or ".." path segment.
var ErrInvalidKey = errors.New("invalid object key")

// Store is an object store. Keys are slash-separated relative paths such as
// "bioac-0142/2026/10/21/<sha256>.flac". Implementations are safe for
// concurrent use.
type Store interface {
	// Put stores exactly size bytes from r under key. It is idempotent: if
	// key already exists with the same size it returns nil without reading
	// r. The object becomes visible only once it is complete.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	// Stat reports the size of key and whether it exists.
	Stat(ctx context.Context, key string) (size int64, exists bool, err error)
	// Delete removes key. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// Ping reports whether the store is reachable and writable (readiness).
	Ping(ctx context.Context) error
}

// ValidateKey checks that key is a safe relative object key.
func ValidateKey(key string) error {
	switch {
	case key == "":
		return fmt.Errorf("%w: empty", ErrInvalidKey)
	case strings.HasPrefix(key, "/"):
		return fmt.Errorf("%w: absolute path", ErrInvalidKey)
	case strings.ContainsAny(key, "\\\x00"):
		return fmt.Errorf("%w: contains a backslash or NUL", ErrInvalidKey)
	}
	for seg := range strings.SplitSeq(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: segment %q", ErrInvalidKey, seg)
		}
	}
	return nil
}

// exactReader passes through exactly size bytes of r and fails if r ends
// early or has more data, so a short or long source never becomes an object.
type exactReader struct {
	r         io.Reader
	remaining int64
}

func newExactReader(r io.Reader, size int64) *exactReader {
	return &exactReader{r: r, remaining: size}
}

func (e *exactReader) Read(p []byte) (int, error) {
	if e.remaining == 0 {
		// Probe for trailing data.
		var b [1]byte
		n, err := e.r.Read(b[:])
		if n > 0 {
			return 0, errors.New("source has more bytes than the declared size")
		}
		if err == nil {
			// A zero-byte read without error; report EOF only on a real EOF.
			return 0, nil
		}
		return 0, err
	}
	if int64(len(p)) > e.remaining {
		p = p[:e.remaining]
	}
	n, err := e.r.Read(p)
	e.remaining -= int64(n)
	if err == io.EOF && e.remaining > 0 {
		return n, fmt.Errorf("source ended %d bytes before the declared size: %w", e.remaining, io.ErrUnexpectedEOF)
	}
	if err == io.EOF {
		err = nil // EOF is reported by the trailing-data probe
	}
	return n, err
}
