// Package meta defines the common upload metadata (FR-1), its validation,
// tus Upload-Metadata parsing and final object key generation.
package meta

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Metadata keys shared by tus Upload-Metadata and the contract.
const (
	KeyDeviceID    = "device_id"
	KeySHA256      = "sha256"
	KeyFilename    = "filename"
	KeyContentType = "content_type"
	KeyCapturedAt  = "captured_at"
	KeyPriority    = "priority"
	// FieldSize names the size field in errors (Upload-Length / InitRequest.size).
	FieldSize = "size"
)

const (
	maxFilenameLen = 255
	maxFutureSkew  = 24 * time.Hour
)

var (
	// ErrInvalid is wrapped by every validation failure (400 / INVALID_ARGUMENT).
	ErrInvalid = errors.New("invalid metadata")
	// ErrTooLarge is wrapped when the size exceeds the limit (413 / INVALID_ARGUMENT).
	ErrTooLarge = errors.New("file too large")

	// deviceIDRe is the canonical (lowercase) form; NormalizeDeviceID
	// lowercases input first, so callers may send uppercase.
	deviceIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,63}$`)
	sha256Re   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// FieldError reports which field failed validation and why.
type FieldError struct {
	Field  string
	Reason string
	kind   error
}

func (e *FieldError) Error() string { return e.Field + ": " + e.Reason }

// Unwrap returns ErrInvalid or ErrTooLarge.
func (e *FieldError) Unwrap() error { return e.kind }

func invalid(field, format string, args ...any) error {
	return &FieldError{Field: field, Reason: fmt.Sprintf(format, args...), kind: ErrInvalid}
}

// Metadata is the validated description of an upload.
type Metadata struct {
	DeviceID    string
	SHA256      string
	Filename    string
	ContentType string
	CapturedAt  time.Time
	Priority    int
	Size        int64
}

// Rules are the configurable validation limits.
type Rules struct {
	MaxFileBytes int64
	ContentTypes []string
}

// Validate checks every FR-1 rule. now is the server clock used for the
// captured_at future limit. The first failing field is reported.
func (m Metadata) Validate(now time.Time, r Rules) error {
	if !ValidDeviceID(m.DeviceID) {
		return invalid(KeyDeviceID, "must be %s", DeviceIDRule)
	}
	if !sha256Re.MatchString(m.SHA256) {
		return invalid(KeySHA256, "must be 64 lowercase hex characters")
	}
	if err := validateFilename(m.Filename); err != nil {
		return err
	}
	if !slices.Contains(r.ContentTypes, m.ContentType) {
		return invalid(KeyContentType, "%q is not an allowed content type", m.ContentType)
	}
	if m.CapturedAt.IsZero() {
		return invalid(KeyCapturedAt, "is required")
	}
	if m.CapturedAt.After(now.Add(maxFutureSkew)) {
		return invalid(KeyCapturedAt, "is more than 24h in the future")
	}
	if m.Priority < 1 || m.Priority > 4 {
		return invalid(KeyPriority, "must be an integer from 1 to 4")
	}
	if m.Size < 1 {
		return invalid(FieldSize, "must be at least 1 byte")
	}
	if m.Size > r.MaxFileBytes {
		return &FieldError{Field: FieldSize, Reason: fmt.Sprintf("exceeds the maximum of %d bytes", r.MaxFileBytes), kind: ErrTooLarge}
	}
	return nil
}

func validateFilename(name string) error {
	switch {
	case name == "":
		return invalid(KeyFilename, "is required")
	case !utf8.ValidString(name):
		return invalid(KeyFilename, "must be valid UTF-8")
	case utf8.RuneCountInString(name) > maxFilenameLen:
		return invalid(KeyFilename, "must be at most %d characters", maxFilenameLen)
	case strings.ContainsAny(name, `/\`):
		return invalid(KeyFilename, `must not contain "/" or "\"`)
	case strings.ContainsFunc(name, unicode.IsControl):
		return invalid(KeyFilename, "must not contain control characters")
	}
	return nil
}

// DeviceIDRule describes a valid device id (before normalization) for
// error messages.
const DeviceIDRule = "3-64 letters, digits or hyphens, starting with a letter or digit (case-insensitive)"

// ValidDeviceID reports whether id is a canonical device id, i.e. matches
// ^[a-z0-9][a-z0-9-]{2,63}$. Normalize input with NormalizeDeviceID first.
func ValidDeviceID(id string) bool { return deviceIDRe.MatchString(id) }

// NormalizeDeviceID returns id with ASCII A-Z lowercased: "WIH-001" and
// "wih-001" are the same device. Only ASCII is mapped, so no non-ASCII
// input (such as the Kelvin sign) can normalize into a valid id.
func NormalizeDeviceID(id string) string {
	return strings.Map(func(r rune) rune {
		if 'A' <= r && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, id)
}

// ParseCapturedAt parses an RFC 3339 timestamp with a UTC offset ("Z" or ±hh:mm).
func ParseCapturedAt(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, invalid(KeyCapturedAt, "is required")
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, invalid(KeyCapturedAt, "must be RFC 3339 with an offset, e.g. 2026-10-21T05:30:00+07:00")
	}
	return t, nil
}

// ParsePriority parses a decimal priority string (range is checked by Validate).
func ParsePriority(s string) (int, error) {
	p, err := strconv.Atoi(s)
	if err != nil {
		return 0, invalid(KeyPriority, "must be an integer from 1 to 4")
	}
	return p, nil
}

// FromTus builds Metadata from decoded tus metadata pairs and the
// Upload-Length. It reports missing keys and unparsable values; call
// Validate for the remaining rules.
func FromTus(pairs map[string]string, size int64) (Metadata, error) {
	for _, k := range []string{KeyDeviceID, KeySHA256, KeyFilename, KeyContentType, KeyCapturedAt, KeyPriority} {
		if _, ok := pairs[k]; !ok {
			return Metadata{}, invalid(k, "is required")
		}
	}
	capturedAt, err := ParseCapturedAt(pairs[KeyCapturedAt])
	if err != nil {
		return Metadata{}, err
	}
	priority, err := ParsePriority(pairs[KeyPriority])
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{
		DeviceID:    NormalizeDeviceID(pairs[KeyDeviceID]),
		SHA256:      pairs[KeySHA256],
		Filename:    pairs[KeyFilename],
		ContentType: pairs[KeyContentType],
		CapturedAt:  capturedAt,
		Priority:    priority,
		Size:        size,
	}, nil
}
