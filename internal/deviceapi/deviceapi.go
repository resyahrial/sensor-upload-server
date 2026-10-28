// Package deviceapi serves device registration and refresh token rotation
// (docs/decisions.md "Decisions (JWT + refresh)"):
//
//	POST /v1/devices/register       allow-listed device asks to join; 202 while
//	                                pending, 200 token pair once approved
//	POST /v1/devices/token/refresh  rotate the refresh token, get a new access token
//
// Both are unauthenticated JSON endpoints; errors are {"error":"<code>"}.
// Secrets and tokens are never logged.
package deviceapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// Routes.
const (
	Prefix       = "/v1/devices/"
	PathRegister = Prefix + "register"
	PathRefresh  = Prefix + "token/refresh"
)

// Error codes (the "error" field of error responses).
const (
	CodeInvalidRequest     = "invalid_request"
	CodeDeviceNotAllowed   = "device_not_allowed"
	CodeDeviceNotActive    = "device_not_active"
	CodePollSecretMismatch = "poll_secret_mismatch"
	CodeInvalidGrant       = "invalid_grant"
	CodeRateLimited        = "rate_limited"
	CodeUnavailable        = "temporarily_unavailable"
	CodeMethodNotAllowed   = "method_not_allowed"
	CodeNotFound           = "not_found"
)

// Response values.
const (
	// StatusPending is the "status" of a 202 register response.
	StatusPending = "pending"
	// TokenTypeBearer is the "token_type" of a token pair.
	TokenTypeBearer = "Bearer"
)

const (
	maxBodyBytes          = 4 << 10
	maxFirmwareLen        = 128
	unavailableRetryAfter = 5 * time.Second
	// requestTimeout bounds the database work of one request.
	requestTimeout = 5 * time.Second
	// pruneInterval spaces opportunistic PruneRetired calls.
	pruneInterval   = time.Hour
	refreshAttempts = 3
	// Default register rate limit per client IP: 10 per minute, burst 10.
	defaultRegisterPerMin   = 10
	defaultRegisterBurst    = 10
	rateLimiterSweepEvery   = time.Minute
	rateLimiterMaxRetryWait = time.Hour
)

// Store is the device persistence the handler needs (*db.Devices
// implements it).
type Store interface {
	// Update locks the device's state, runs fn and persists its changes
	// in one transaction; an error from fn is returned unchanged.
	Update(ctx context.Context, id string, fn func(s *db.DeviceState) error) error
	// FindRefreshHash finds the device of a live or retired refresh token
	// hash, or returns db.ErrTokenNotFound.
	FindRefreshHash(ctx context.Context, hash []byte) (db.TokenMatch, error)
	// Get returns the device row or db.ErrDeviceNotFound.
	Get(ctx context.Context, id string) (db.Device, error)
	// PruneRetired deletes retired hashes retired before before.
	PruneRetired(ctx context.Context, before time.Time) (int64, error)
}

// Config configures the handler.
type Config struct {
	Store  Store
	Issuer *auth.Issuer
	// IdleTTL (REFRESH_IDLE_TTL) and MaxAge (REFRESH_MAX_AGE) bound a
	// refresh token family.
	IdleTTL time.Duration
	MaxAge  time.Duration
	// RetryAfter (REGISTER_RETRY_AFTER) is the Retry-After of a 202.
	RetryAfter time.Duration
	// RegisterPerMinute and RegisterBurst limit register requests per
	// client IP; 0 = 10 and 10.
	RegisterPerMinute int
	RegisterBurst     int
	// BehindProxy keys the register rate limit on the client address the
	// reverse proxy appended to X-Forwarded-For, not the proxy's own.
	BehindProxy bool
	// Now overrides the clock (tests); defaults to time.Now. The Issuer
	// has its own clock.
	Now     func() time.Time
	Metrics *obs.Metrics // nil = no metrics
	Log     *slog.Logger // nil = discard
}

// TokenResponse is the 200 body of register and refresh (and the output
// of devicectl issue).
type TokenResponse struct {
	DeviceID         string `json:"device_id"`
	AccessToken      string `json:"access_token"`
	TokenType        string `json:"token_type"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshToken     string `json:"refresh_token"`
	RefreshExpiresAt string `json:"refresh_expires_at"`
}

type errorBody struct {
	Error string `json:"error"`
}

type pendingBody struct {
	Status string `json:"status"`
}

// Outcomes of the state machine callbacks (never leave the package).
var (
	errNotAllowed   = errors.New("device not in batch")
	errNotActive    = errors.New("device not active")
	errMismatch     = errors.New("poll secret mismatch")
	errExpired      = errors.New("refresh family expired")
	errRaced        = errors.New("refresh token changed concurrently")
	errInvalidInput = errors.New("invalid request")
)

// NewHandler returns the handler for the routes under Prefix. Other paths
// get 404, methods other than POST 405.
func NewHandler(cfg Config) (http.Handler, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("deviceapi: store is required")
	case cfg.Issuer == nil:
		return nil, errors.New("deviceapi: issuer is required")
	case cfg.IdleTTL <= 0 || cfg.MaxAge <= 0:
		return nil, errors.New("deviceapi: refresh idle TTL and max age must be positive")
	case cfg.RetryAfter <= 0:
		return nil, errors.New("deviceapi: register retry-after must be positive")
	case cfg.RegisterPerMinute < 0 || cfg.RegisterBurst < 0:
		return nil, errors.New("deviceapi: register rate limit must not be negative")
	}
	if cfg.RegisterPerMinute == 0 {
		cfg.RegisterPerMinute = defaultRegisterPerMin
	}
	if cfg.RegisterBurst == 0 {
		cfg.RegisterBurst = defaultRegisterBurst
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	h := &handler{
		cfg:     cfg,
		limiter: newLimiter(float64(cfg.RegisterPerMinute)/60, float64(cfg.RegisterBurst), cfg.Now),
	}
	return h, nil
}

type handler struct {
	cfg       Config
	limiter   *limiter
	lastPrune atomic.Int64 // unix seconds of the last PruneRetired
}

// result is what one request did, for the log line and metrics.
type result struct {
	code     int
	metric   string
	deviceID string // "" when unknown or invalid
	err      error  // internal error (logged, never sent)
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var serve func(http.ResponseWriter, *http.Request) result
	var name string
	switch r.URL.Path {
	case PathRegister:
		serve, name = h.register, "device register"
	case PathRefresh:
		serve, name = h.refresh, "token refresh"
	default:
		writeError(w, http.StatusNotFound, CodeNotFound)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, CodeMethodNotAllowed)
		return
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res := serve(w, r.WithContext(ctx))
	if h.cfg.Metrics != nil {
		c := h.cfg.Metrics.DeviceRegister
		if r.URL.Path == PathRefresh {
			c = h.cfg.Metrics.TokenRefresh
		}
		c.WithLabelValues(res.metric).Inc()
	}
	attrs := []slog.Attr{
		slog.String("result", res.metric),
		slog.Int("status", res.code),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()),
	}
	if res.deviceID != "" {
		attrs = append(attrs, slog.String("device_id", res.deviceID))
	}
	level := slog.LevelInfo
	if res.err != nil {
		attrs = append(attrs, slog.String("error", res.err.Error()))
		level = slog.LevelError
	}
	h.cfg.Log.LogAttrs(r.Context(), level, name, attrs...)
}

// decode reads a JSON body of at most maxBodyBytes into v.
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %w", errInvalidInput, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing data", errInvalidInput)
	}
	return nil
}

// newFamily returns a fresh refresh token and its family started at now.
func newFamily(now time.Time) (string, *db.RefreshFamily, error) {
	tok, err := auth.GenerateSecret()
	if err != nil {
		return "", nil, err
	}
	return tok, &db.RefreshFamily{CurrentHash: auth.HashSecret(tok), StartedAt: now, RefreshedAt: now}, nil
}

// expired reports whether f is past maxAge or idle for longer than idle.
func expired(f *db.RefreshFamily, now time.Time, idle, maxAge time.Duration) bool {
	return now.After(f.StartedAt.Add(maxAge)) || now.After(f.RefreshedAt.Add(idle))
}

// tokenResponse signs an access token and builds the 200 body. The
// refresh token expires at min(family start + maxAge, now + idle).
func tokenResponse(iss *auth.Issuer, device, refresh string, f *db.RefreshFamily, now time.Time,
	idle, maxAge time.Duration) (TokenResponse, error) {
	access, _, err := iss.Issue(device)
	if err != nil {
		return TokenResponse{}, err
	}
	exp := f.StartedAt.Add(maxAge)
	if idleExp := now.Add(idle); idleExp.Before(exp) {
		exp = idleExp
	}
	return TokenResponse{
		DeviceID:         device,
		AccessToken:      access,
		TokenType:        TokenTypeBearer,
		ExpiresIn:        int64(iss.TTL() / time.Second),
		RefreshToken:     refresh,
		RefreshExpiresAt: exp.UTC().Format(time.RFC3339),
	}, nil
}

// IssueConfig configures Issue.
type IssueConfig struct {
	Store   Store
	Issuer  *auth.Issuer
	IdleTTL time.Duration
	MaxAge  time.Duration
	Now     time.Time
}

// Issue is the devicectl shortcut: it makes the (batch-imported) device
// active, creating its row if needed, starts a new refresh family
// (retiring any previous one) and returns the token pair.
func Issue(ctx context.Context, cfg IssueConfig, device string) (TokenResponse, error) {
	var (
		refresh string
		fam     *db.RefreshFamily
	)
	err := cfg.Store.Update(ctx, device, func(s *db.DeviceState) error {
		if !s.InBatch {
			return fmt.Errorf("device %q: %w", device, db.ErrNotInBatch)
		}
		now := cfg.Now
		if s.Device == nil {
			s.Device = &db.Device{DeviceID: device, RegisteredAt: now, LastSeenAt: now}
		}
		if s.Device.Status != db.DeviceActive {
			s.Device.Status = db.DeviceActive
			s.Device.ApprovedAt = &now
		}
		s.RetireFamily(db.RetireReset, now)
		var err error
		if refresh, fam, err = newFamily(now); err != nil {
			return err
		}
		s.Family = fam
		return nil
	})
	if err != nil {
		return TokenResponse{}, err
	}
	return tokenResponse(cfg.Issuer, device, refresh, fam, cfg.Now, cfg.IdleTTL, cfg.MaxAge)
}

// ErrNotActive is returned by Activate for a rejected or revoked device.
var ErrNotActive = errors.New("device is rejected or revoked")

// Activate is the devicectl access-token shortcut: it makes the
// (batch-imported) device active, creating its row if needed, and leaves its
// refresh family alone. A rejected or revoked device stays as it is and
// gets ErrNotActive, so experiments cannot silently undo an ops decision.
func Activate(ctx context.Context, store Store, device string, now time.Time) error {
	return store.Update(ctx, device, func(s *db.DeviceState) error {
		if !s.InBatch {
			return fmt.Errorf("device %q: %w", device, db.ErrNotInBatch)
		}
		if s.Device == nil {
			s.Device = &db.Device{DeviceID: device, RegisteredAt: now, LastSeenAt: now}
		}
		switch s.Device.Status {
		case db.DeviceActive:
		case db.DeviceRejected, db.DeviceRevoked:
			return fmt.Errorf("device %q is %s: %w", device, s.Device.Status, ErrNotActive)
		default:
			s.Device.Status = db.DeviceActive
			s.Device.ApprovedAt = &now
		}
		return nil
	})
}

func equalHash(a, b []byte) bool {
	return a != nil && b != nil && subtle.ConstantTimeCompare(a, b) == 1
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v) // client may have gone away; nothing to do
}

func writeError(w http.ResponseWriter, code int, errCode string) {
	writeJSON(w, code, errorBody{Error: errCode})
}

// setRetryAfter sets Retry-After to d in whole seconds (at least 1).
func setRetryAfter(w http.ResponseWriter, d time.Duration) {
	w.Header().Set("Retry-After", strconv.FormatInt(int64(max(1, math.Ceil(d.Seconds()))), 10))
}

// unavailable answers 503 temporarily_unavailable with Retry-After: 5.
func unavailable(w http.ResponseWriter, metric, device string, err error) result {
	setRetryAfter(w, unavailableRetryAfter)
	writeError(w, http.StatusServiceUnavailable, CodeUnavailable)
	return result{code: http.StatusServiceUnavailable, metric: metric, deviceID: device, err: err}
}
