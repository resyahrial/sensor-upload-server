package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	// ErrPermanent: 400, 401 invalid_token, 403, 413 / INVALID_ARGUMENT,
	// UNAUTHENTICATED invalid_token, PERMISSION_DENIED, and refresh failures
	// that need a new registration. Retrying cannot help.
	ErrPermanent = errors.New("permanent failure")

	// ErrTokenExpired: 401 token_expired / UNAUTHENTICATED "token_expired".
	// Not permanent by itself: the transports refresh the access token and
	// retry the request once; a static token cannot refresh and fails
	// permanently.
	ErrTokenExpired = errors.New("access token expired")
	// ErrInvalidToken: 401 invalid_token / UNAUTHENTICATED. Always wrapped
	// together with ErrPermanent.
	ErrInvalidToken = errors.New("invalid access token")
	// ErrDeviceNotActive: 403 device_not_active / PERMISSION_DENIED, from an
	// upload or the refresh endpoint. Always wrapped with ErrPermanent.
	ErrDeviceNotActive = errors.New("device not active")
	// ErrInvalidGrant: the refresh endpoint rejected the refresh token
	// (401 invalid_grant); the device must register again. Always wrapped
	// with ErrPermanent.
	ErrInvalidGrant = errors.New("refresh token rejected (invalid_grant)")
)

// Auth error codes in JSON bodies ({"error": "..."}) and gRPC messages.
const (
	codeTokenExpired    = "token_expired"
	codeInvalidToken    = "invalid_token"
	codeDeviceNotActive = "device_not_active"
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

// authErr maps a 401/403 with its error code; other statuses go to statusErr.
func authErr(op string, code int, reason string) error {
	switch {
	case code == http.StatusUnauthorized && reason == codeTokenExpired:
		return fmt.Errorf("%s: HTTP 401 %s: %w", op, reason, ErrTokenExpired)
	case code == http.StatusUnauthorized:
		return fmt.Errorf("%s: HTTP 401 %s: %w: %w", op, reason, ErrPermanent, ErrInvalidToken)
	case code == http.StatusForbidden && reason == codeDeviceNotActive:
		return fmt.Errorf("%s: HTTP 403 %s: %w: %w", op, reason, ErrPermanent, ErrDeviceNotActive)
	default:
		return statusErr(op, code)
	}
}

// responseErr maps an unexpected tus response, reading the auth error code
// of a 401/403 from its JSON body (or WWW-Authenticate). A HEAD response
// cannot carry a body, so a 401 to HEAD without a code is treated as
// token_expired: one refresh and retry tells the cases apart.
func responseErr(op string, res *http.Response) error {
	if res.StatusCode != http.StatusUnauthorized && res.StatusCode != http.StatusForbidden {
		return statusErr(op, res.StatusCode)
	}
	reason := errorCode(res)
	if reason == "" && res.StatusCode == http.StatusUnauthorized && res.Request != nil && res.Request.Method == http.MethodHead {
		reason = codeTokenExpired
	}
	return authErr(op, res.StatusCode, reason)
}

// errorCode returns the "error" member of a JSON error body, or a known
// code named in the WWW-Authenticate header, or "".
func errorCode(res *http.Response) string {
	var body struct {
		Error string `json:"error"`
	}
	if b, err := io.ReadAll(io.LimitReader(res.Body, 4<<10)); err == nil && json.Unmarshal(b, &body) == nil && body.Error != "" {
		return body.Error
	}
	h := res.Header.Get("WWW-Authenticate")
	for _, c := range []string{codeTokenExpired, codeDeviceNotActive, codeInvalidToken} {
		if strings.Contains(h, c) {
			return c
		}
	}
	return ""
}
