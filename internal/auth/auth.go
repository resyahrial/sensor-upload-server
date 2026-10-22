// Package auth verifies per-device keys (FR-2, replaced JWTs; see
// docs/decisions.md "Decisions (device keys)").
//
// A device authenticates with "Authorization: Bearer {device_id}_{key}"
// (gRPC: the same value in the "authorization" metadata). The token is split
// at its last "_": device_id before, key after. Keys are 64 lowercase hex
// characters (32 random bytes), so they never contain "_". The server keeps
// only SHA-256(key) per device (table device_keys) and compares hashes in
// constant time.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

// KeyBytes is the number of random bytes in a device key; the key is their
// lowercase hex encoding (KeyHexLen characters).
const (
	KeyBytes  = 32
	KeyHexLen = 2 * KeyBytes
)

// DefaultLookupTimeout bounds one key lookup in the store.
const DefaultLookupTimeout = 2 * time.Second

var (
	// ErrUnauthenticated means the token is missing, malformed, names an
	// unknown device or carries the wrong key (401 / UNAUTHENTICATED). The
	// cases are not distinguished for clients (no device enumeration).
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrPermissionDenied means the token is valid but its device_id does not
	// match the request (403 / PERMISSION_DENIED).
	ErrPermissionDenied = errors.New("permission denied")
	// ErrUnavailable means the key store could not be queried (503 /
	// UNAVAILABLE): the client should retry, not treat its key as bad.
	ErrUnavailable = errors.New("authentication unavailable")
)

// KeyStore looks up the stored SHA-256 hash of a device's key. found is
// false for a device without a key; err is reserved for store failures.
type KeyStore interface {
	KeyHash(ctx context.Context, device string) (hash []byte, found bool, err error)
}

// Verifier checks device tokens against a KeyStore.
type Verifier struct {
	store   KeyStore
	timeout time.Duration
}

// NewVerifier returns a Verifier using store; timeout bounds each lookup
// (0 = DefaultLookupTimeout).
func NewVerifier(store KeyStore, timeout time.Duration) (*Verifier, error) {
	if store == nil {
		return nil, errors.New("auth: key store is required")
	}
	if timeout <= 0 {
		timeout = DefaultLookupTimeout
	}
	return &Verifier{store: store, timeout: timeout}, nil
}

// Verify checks token ("{device_id}_{key}") and returns its device_id.
// Failures wrap ErrUnauthenticated or ErrUnavailable; the detail is for
// logs only and never contains the key.
func (v *Verifier) Verify(ctx context.Context, token string) (string, error) {
	device, key, err := ParseToken(token)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	stored, found, err := v.store.KeyHash(ctx, device)
	if err != nil {
		return "", fmt.Errorf("%w: key lookup for device %q: %w", ErrUnavailable, device, err)
	}
	if !found {
		return "", fmt.Errorf("%w: unknown device %q", ErrUnauthenticated, device)
	}
	if subtle.ConstantTimeCompare(HashKey(key), stored) != 1 {
		return "", fmt.Errorf("%w: wrong key for device %q", ErrUnauthenticated, device)
	}
	return device, nil
}

// VerifyHeader extracts the bearer token from an Authorization header
// value and verifies it.
func (v *Verifier) VerifyHeader(ctx context.Context, header string) (string, error) {
	token, err := BearerToken(header)
	if err != nil {
		return "", err
	}
	return v.Verify(ctx, token)
}

// ParseToken splits token at its last "_" into device_id and key. The
// device_id must satisfy the metadata rules (meta.ValidDeviceID) and the key
// must be KeyHexLen lowercase hex characters.
func ParseToken(token string) (device, key string, err error) {
	i := strings.LastIndexByte(token, '_')
	if i < 0 {
		return "", "", fmt.Errorf("%w: token is not {device_id}_{key}", ErrUnauthenticated)
	}
	device, key = token[:i], token[i+1:]
	if !meta.ValidDeviceID(device) {
		return "", "", fmt.Errorf("%w: token device_id is invalid", ErrUnauthenticated)
	}
	if !ValidKey(key) {
		return "", "", fmt.Errorf("%w: token key is not %d lowercase hex characters", ErrUnauthenticated, KeyHexLen)
	}
	return device, key, nil
}

// ValidKey reports whether key is KeyHexLen lowercase hex characters.
func ValidKey(key string) bool {
	if len(key) != KeyHexLen {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// GenerateKey returns a new random key (KeyBytes from crypto/rand, hex).
func GenerateKey() (string, error) {
	b := make([]byte, KeyBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("auth: generate key: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// HashKey returns SHA-256(key), the value stored in device_keys.key_hash.
// A plain hash suffices: keys are 256-bit random values, so there is
// nothing to brute-force and no need for a slow password hash.
func HashKey(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// FormatToken returns the bearer token "{device_id}_{key}".
func FormatToken(device, key string) string { return device + "_" + key }

// BearerToken returns the token from an "Authorization: Bearer <token>"
// header value. The scheme is case-insensitive.
func BearerToken(header string) (string, error) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", fmt.Errorf("%w: missing bearer token", ErrUnauthenticated)
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("%w: empty bearer token", ErrUnauthenticated)
	}
	return token, nil
}

// CheckDevice returns ErrPermissionDenied unless the token's device ID
// equals the device ID from the upload metadata.
func CheckDevice(tokenDevice, metaDevice string) error {
	if tokenDevice != metaDevice {
		return fmt.Errorf("%w: token device_id does not match metadata", ErrPermissionDenied)
	}
	return nil
}
