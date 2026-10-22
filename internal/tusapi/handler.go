// Package tusapi serves the tus 1.0 endpoints (/rest/files/ and
// /rest-http3/files/) with tusd as a library: an auth/ownership middleware
// in front of tusd, tusd hooks for create, finish and terminate, and crash
// recovery for staged uploads that were never finalized.
package tusapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/tus/tusd/v2/pkg/handler"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/finalize"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// Protocol labels (DB protocol column and metrics label).
const (
	ProtocolREST      = "rest"
	ProtocolRESTHTTP3 = "rest-http3"
)

// Base paths of the two tus endpoints.
const (
	BasePathREST      = "/rest/files/"
	BasePathRESTHTTP3 = "/rest-http3/files/"
)

// Extensions is the Tus-Extension value advertised on OPTIONS (contract §2).
const Extensions = "creation,termination,expiration"

// Custom tus status codes (requirements §7).
const (
	StatusChecksumMismatch = 460
	StatusAlreadyExists    = 461
)

const (
	// networkTimeout is the per-read body deadline (NFR-4 120 s read).
	networkTimeout = 120 * time.Second
	// finalizeTimeout bounds one finalizer run. It is detached from the
	// request, so a client that disconnects during finalization of a large
	// file does not waste the work.
	finalizeTimeout = 10 * time.Minute
)

// lockTimeout bounds waiting for the upload lock in the middleware (a
// variable so tests can shorten it).
var lockTimeout = 20 * time.Second

// Repository is the subset of *db.Repo the tus endpoints use.
type Repository interface {
	InsertUploading(ctx context.Context, protocol, ref string, m meta.Metadata) (db.Upload, error)
	Get(ctx context.Context, protocol, ref string) (db.Upload, error)
	FindCompletedBySHA(ctx context.Context, sha string) (db.Upload, error)
	IncrResume(ctx context.Context, protocol, ref string) error
	Touch(ctx context.Context, protocol, ref string) error
	SetStatus(ctx context.Context, protocol, ref string, s db.Status) error
}

// Finalizer runs the shared completion pipeline (*finalize.Finalizer).
type Finalizer interface {
	Run(ctx context.Context, in finalize.Input) (string, error)
}

// Verifier authenticates a request's Authorization header (*auth.Verifier).
type Verifier interface {
	VerifyHeader(header string) (string, error)
}

// Config configures one tus endpoint.
type Config struct {
	BasePath      string // BasePathREST or BasePathRESTHTTP3
	Protocol      string // ProtocolREST or ProtocolRESTHTTP3
	Rules         meta.Rules
	IncompleteTTL time.Duration // drives Upload-Expires
	Staging       *Staging
	Repo          Repository
	Finalizer     Finalizer
	Verifier      Verifier
	Metrics       *obs.Metrics
	Log           *slog.Logger
	Now           func() time.Time // nil = time.Now
}

// New returns the handler for one tus endpoint, to be mounted at
// cfg.BasePath (it strips the prefix itself, because tusd takes the upload
// ID from the whole remaining path).
func New(cfg Config) (http.Handler, error) {
	if cfg.Staging == nil || cfg.Repo == nil || cfg.Finalizer == nil || cfg.Verifier == nil || cfg.Metrics == nil || cfg.Log == nil {
		return nil, errors.New("tusapi: Staging, Repo, Finalizer, Verifier, Metrics and Log are required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	h := &endpoint{cfg: cfg}
	tusd, err := handler.NewHandler(handler.Config{
		StoreComposer:              cfg.Staging.composer(),
		BasePath:                   cfg.BasePath,
		MaxSize:                    cfg.Rules.MaxFileBytes,
		DisableDownload:            true,
		DisableConcatenation:       true,
		Cors:                       &handler.CorsConfig{Disable: true},
		Logger:                     newTusdLogger(cfg.Log),
		NetworkTimeout:             networkTimeout,
		PreUploadCreateCallback:    h.preCreate,
		PreFinishResponseCallback:  h.preFinish,
		PreUploadTerminateCallback: h.preTerminate,
	})
	if err != nil {
		return nil, fmt.Errorf("tusapi: create tusd handler: %w", err)
	}
	h.tusd = tusd
	return http.StripPrefix(cfg.BasePath, h), nil
}

// endpoint is the middleware in front of one tusd handler plus its hooks.
type endpoint struct {
	cfg  Config
	tusd http.Handler
}

// tusError builds a handler.Error whose body is exactly msg (plus a
// newline). Hooks must only return handler.Error values: tusd turns any
// other error into a 500 with the error text in the body.
func tusError(status int, code, msg string) handler.Error {
	return handler.Error{
		ErrorCode: code,
		Message:   msg,
		HTTPResponse: handler.HTTPResponse{
			StatusCode: status,
			Body:       msg + "\n",
			Header:     handler.HTTPHeader{"Content-Type": "text/plain; charset=utf-8"},
		},
	}
}

var (
	errUnauthenticated = tusError(http.StatusUnauthorized, "ERR_UNAUTHENTICATED", "unauthenticated")
	errForbidden       = tusError(http.StatusForbidden, "ERR_PERMISSION_DENIED", "device_id does not match token")
	errNotFound        = tusError(http.StatusNotFound, "ERR_UPLOAD_NOT_FOUND", "upload not found")
	errExpired         = tusError(http.StatusGone, "ERR_UPLOAD_EXPIRED", "upload expired")
	errChecksum        = tusError(StatusChecksumMismatch, "ERR_CHECKSUM_MISMATCH", "sha256 mismatch")
	errAlreadyExists   = handler.Error{ // body exactly "already exists" (FR-3)
		ErrorCode: "ERR_ALREADY_EXISTS",
		Message:   "already exists",
		HTTPResponse: handler.HTTPResponse{
			StatusCode: StatusAlreadyExists,
			Body:       "already exists",
			Header:     handler.HTTPHeader{"Content-Type": "text/plain; charset=utf-8"},
		},
	}
	errInternal     = tusError(http.StatusInternalServerError, "ERR_INTERNAL", "internal error")
	errShuttingDown = tusError(http.StatusServiceUnavailable, "ERR_SERVER_SHUTDOWN", "server shutting down")
	errLocked       = tusError(http.StatusLocked, "ERR_UPLOAD_LOCKED", "upload is locked")
)

// shuttingDown reports whether ctx was cancelled because the server is
// shutting down (cause handler.ErrServerShutdown, see httpx).
func shuttingDown(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), handler.ErrServerShutdown)
}

// internalError is the response for a failed storage or DB call of the
// middleware: 503 during shutdown (as tusd answers then), else 500.
func internalError(ctx context.Context) handler.Error {
	if shuttingDown(ctx) {
		return errShuttingDown
	}
	return errInternal
}

// lockError maps a failure to acquire the upload lock: 503 during
// shutdown, 423 when it was held too long (§7), else 500.
func lockError(ctx context.Context, err error) handler.Error {
	switch {
	case shuttingDown(ctx):
		return errShuttingDown
	case errors.Is(err, handler.ErrLockTimeout), errors.Is(err, handler.ErrFileLocked), errors.Is(err, context.DeadlineExceeded):
		return errLocked
	default:
		return errInternal
	}
}

// metaError maps a meta validation error to 400 or 413 naming the field.
func metaError(err error) handler.Error {
	if errors.Is(err, meta.ErrTooLarge) {
		return tusError(http.StatusRequestEntityTooLarge, "ERR_MAX_SIZE_EXCEEDED", err.Error())
	}
	return tusError(http.StatusBadRequest, "ERR_INVALID_METADATA", err.Error())
}

// finalizeError maps a finalizer outcome to the tus response error.
func finalizeError(err error) handler.Error {
	switch {
	case errors.Is(err, finalize.ErrChecksum):
		return errChecksum
	case errors.Is(err, finalize.ErrDuplicate):
		return errAlreadyExists
	case errors.Is(err, finalize.ErrInactive), errors.Is(err, db.ErrNotFound):
		return errNotFound
	default:
		return errInternal
	}
}

// preCreate validates the metadata, checks device and dedup, picks the
// UUIDv7 upload ID and inserts the 'uploading' row (FR-1..FR-4).
func (h *endpoint) preCreate(ev handler.HookEvent) (handler.HTTPResponse, handler.FileInfoChanges, error) {
	st := stateFrom(ev.Context)
	fail := func(e handler.Error, cause error) (handler.HTTPResponse, handler.FileInfoChanges, error) {
		st.fail(cause)
		return handler.HTTPResponse{}, handler.FileInfoChanges{}, e
	}
	// tusd's own parser silently drops malformed pairs; parse strictly.
	pairs, err := meta.ParseUploadMetadata(ev.HTTPRequest.Header.Get(meta.HeaderUploadMetadata))
	if err != nil {
		return fail(metaError(err), err)
	}
	m, err := meta.FromTus(pairs, ev.Upload.Size)
	if err == nil {
		err = m.Validate(h.cfg.Now(), h.cfg.Rules)
	}
	if err != nil {
		return fail(metaError(err), err)
	}
	if err := auth.CheckDevice(st.device, m.DeviceID); err != nil {
		return fail(errForbidden, err)
	}
	ctx := ev.Context
	switch _, err := h.cfg.Repo.FindCompletedBySHA(ctx, m.SHA256); {
	case err == nil:
		return fail(errAlreadyExists, finalize.ErrDuplicate)
	case !errors.Is(err, db.ErrNotFound):
		return fail(errInternal, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return fail(errInternal, fmt.Errorf("generate upload id: %w", err))
	}
	ref := id.String()
	if _, err := h.cfg.Repo.InsertUploading(ctx, h.cfg.Protocol, ref, m); err != nil {
		return fail(errInternal, err)
	}
	h.cfg.Metrics.Started.WithLabelValues(h.cfg.Protocol).Inc()
	st.ref = ref
	st.expires = h.cfg.Now().Add(h.cfg.IncompleteTTL)
	return handler.HTTPResponse{}, handler.FileInfoChanges{ID: ref}, nil
}

// preFinish runs the finalizer before tusd answers the last PATCH, so the
// client never sees 204 for the final chunk unless the row is completed.
func (h *endpoint) preFinish(ev handler.HookEvent) (handler.HTTPResponse, error) {
	st := stateFrom(ev.Context)
	if err := h.finalize(ev.Context, ev.Upload.ID); err != nil {
		st.fail(err)
		return handler.HTTPResponse{}, finalizeError(err)
	}
	st.completed = true
	return handler.HTTPResponse{}, nil
}

// finalize runs the finalizer for the staged upload id, detached from the
// request's cancellation.
func (h *endpoint) finalize(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	defer cancel()
	_, err := h.cfg.Finalizer.Run(ctx, h.cfg.Staging.input(h.cfg.Protocol, refFromID(id), id))
	return err
}

// preTerminate marks the row cancelled before tusd deletes the staged
// data. If the row is no longer 'uploading' the termination is refused.
func (h *endpoint) preTerminate(ev handler.HookEvent) (handler.HTTPResponse, error) {
	st := stateFrom(ev.Context)
	err := h.cfg.Repo.SetStatus(ev.Context, h.cfg.Protocol, refFromID(ev.Upload.ID), db.StatusCancelled)
	switch {
	case err == nil:
		h.cfg.Metrics.Failed.WithLabelValues(h.cfg.Protocol, obs.ReasonCancelled).Inc()
		return handler.HTTPResponse{}, nil
	case errors.Is(err, db.ErrNotUploading), errors.Is(err, db.ErrNotFound):
		st.fail(err)
		return handler.HTTPResponse{}, errNotFound
	default:
		st.fail(err)
		return handler.HTTPResponse{}, errInternal
	}
}
