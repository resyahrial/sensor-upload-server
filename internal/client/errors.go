package client

import (
	"errors"
	"fmt"
	"net/http"
)

// Outcome errors shared by the tus and gRPC transports. They map the
// contract's error tables (§2, §4) onto the sensor actions of §5.
var (
	// ErrAlreadyExists: 461 / ALREADY_EXISTS. The file is stored; mark done.
	ErrAlreadyExists = errors.New("already exists")
	// ErrExpired: 404 / 410 / NOT_FOUND. Create a new upload.
	ErrExpired = errors.New("upload unknown or expired")
	// ErrOffsetMismatch: 409 / FAILED_PRECONDITION. Ask the server for the offset and resume.
	ErrOffsetMismatch = errors.New("offset mismatch")
	// ErrChecksum: 460 / DATA_LOSS. Re-read the file and upload it again.
	ErrChecksum = errors.New("sha-256 mismatch")
	// ErrPermanent: 400, 401, 403, 413 / INVALID_ARGUMENT, UNAUTHENTICATED,
	// PERMISSION_DENIED. Retrying cannot help (the token is static here).
	ErrPermanent = errors.New("permanent failure")
)

// errConn marks a failure where no response arrived (dial, handshake, reset).
// It drives the HTTP/3 → /rest fallback.
var errConn = errors.New("connection failure")

// statusErr maps an unexpected tus HTTP status to an outcome error.
func statusErr(op string, code int) error {
	switch code {
	case http.StatusNotFound, http.StatusGone:
		return fmt.Errorf("%s: HTTP %d: %w", op, code, ErrExpired)
	case http.StatusConflict:
		return fmt.Errorf("%s: HTTP %d: %w", op, code, ErrOffsetMismatch)
	case 460:
		return fmt.Errorf("%s: HTTP %d: %w", op, code, ErrChecksum)
	case 461:
		return fmt.Errorf("%s: HTTP %d: %w", op, code, ErrAlreadyExists)
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusRequestEntityTooLarge:
		return fmt.Errorf("%s: HTTP %d: %w", op, code, ErrPermanent)
	default:
		// 5xx, 423 and anything unexpected: transient.
		return fmt.Errorf("%s: unexpected HTTP %d", op, code)
	}
}
