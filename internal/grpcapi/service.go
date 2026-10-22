package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
	"github.com/resyahrial/sensor-upload-server/internal/staging"
)

// DefaultIdleTimeout is the per-stream idle timeout (NFR-4).
const DefaultIdleTimeout = 120 * time.Second

// Config holds the service settings.
type Config struct {
	// MaxChunkBytes is GRPC_MAX_CHUNK_BYTES, returned by Init and enforced
	// on every chunk.
	MaxChunkBytes int
	// AckEveryN is GRPC_ACK_EVERY_N; the final chunk is always acked.
	AckEveryN int
	// IdleTimeout ends a stream that receives no chunk for this long.
	// Zero means DefaultIdleTimeout.
	IdleTimeout time.Duration
	// Rules are the FR-1 validation limits.
	Rules meta.Rules
	// Now overrides the clock (tests); defaults to time.Now.
	Now func() time.Time
}

// Deps are the collaborators of a Service. All are required.
type Deps struct {
	Repo      Repo
	Finalizer Finalizer
	// Store is the gRPC staging store (STAGING_DIR/grpc).
	Store    *staging.Store
	Verifier *auth.Verifier
	Metrics  *obs.Metrics
	Logger   *slog.Logger
}

// Service implements uploadv1.UploadServiceServer.
type Service struct {
	uploadv1.UnimplementedUploadServiceServer

	cfg      Config
	repo     Repo
	fin      Finalizer
	store    *staging.Store
	verifier *auth.Verifier
	metrics  *obs.Metrics
	log      *slog.Logger
	active   *registry

	closing   chan struct{}
	closeOnce sync.Once
}

// NewService checks cfg and deps and returns a Service.
func NewService(cfg Config, d Deps) (*Service, error) {
	switch {
	case cfg.MaxChunkBytes <= 0:
		return nil, errors.New("grpcapi: MaxChunkBytes must be positive")
	case cfg.AckEveryN <= 0:
		return nil, errors.New("grpcapi: AckEveryN must be positive")
	case d.Repo == nil || d.Finalizer == nil || d.Store == nil || d.Verifier == nil || d.Metrics == nil || d.Logger == nil:
		return nil, errors.New("grpcapi: missing dependency")
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Service{
		cfg:      cfg,
		repo:     d.Repo,
		fin:      d.Finalizer,
		store:    d.Store,
		verifier: d.Verifier,
		metrics:  d.Metrics,
		log:      d.Logger.With("protocol", Protocol),
		active:   newRegistry(),
		closing:  make(chan struct{}),
	}, nil
}

// Active reports whether upload id is currently held by a stream (or a
// Cancel/finalize call). The expiry cleaner skips such uploads.
func (s *Service) Active(id string) bool { return s.active.held(id) }

// Acquire claims upload id like a stream does, so that no stream, Cancel or
// finalize can run on it until release is called. It returns ok=false if the
// upload is in use. The expiry cleaner holds it while expiring an upload.
func (s *Service) Acquire(id string) (release func(), ok bool) { return s.active.acquire(id) }

// Shutdown makes new calls fail with UNAVAILABLE and ends active streams
// with UNAVAILABLE after their current chunk. Call it before stopping the
// listeners. It is safe to call more than once.
func (s *Service) Shutdown() {
	s.closeOnce.Do(func() { close(s.closing) })
}

// drainPoll is how often Drain checks for remaining holders.
const drainPoll = 10 * time.Millisecond

// Drain waits until no stream, Cancel or finalize holds an upload, or until
// ctx is done (returning its error). Call it after Shutdown, so streams end
// with UNAVAILABLE after their current chunk before the listeners cancel
// request contexts.
func (s *Service) Drain(ctx context.Context) error {
	t := time.NewTicker(drainPoll)
	defer t.Stop()
	for s.active.size() > 0 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("grpcapi: drain: %w", context.Cause(ctx))
		case <-t.C:
		}
	}
	return nil
}

func (s *Service) shuttingDown() bool {
	select {
	case <-s.closing:
		return true
	default:
		return false
	}
}

var (
	errUnavailable = status.Error(codes.Unavailable, "server shutting down")
	errNotFound    = status.Error(codes.NotFound, "upload not found")
	errInternal    = status.Error(codes.Internal, "internal error")
	errCompleted   = status.Error(codes.AlreadyExists, "already exists")
	errBusy        = status.Error(codes.Aborted, "upload is in use by another stream")
)

// Init validates the metadata, deduplicates, and starts or resumes an upload.
func (s *Service) Init(ctx context.Context, req *uploadv1.InitRequest) (*uploadv1.InitResponse, error) {
	start := time.Now()
	device := deviceFrom(ctx)
	log := s.log.With("method", "Init", "device_id", device)

	m, err := s.metadata(req)
	if err != nil {
		return nil, s.logResult(log, start, status.Error(codes.InvalidArgument, err.Error()), err)
	}
	if err := auth.CheckDevice(device, m.DeviceID); err != nil {
		return nil, s.logResult(log, start, status.Error(codes.PermissionDenied, "device_id does not match token"), err)
	}
	done, err := s.repo.CompletedExists(ctx, m.SHA256)
	if err != nil {
		return nil, s.logResult(log, start, errInternal, err)
	}
	if done {
		return nil, s.logResult(log, start, errCompleted, nil)
	}

	u, err := s.repo.FindInProgress(ctx, m.SHA256, m.DeviceID)
	switch {
	case err == nil:
		res, rerr := s.resume(ctx, u)
		if res != nil || rerr != nil {
			log = log.With("upload_ref", u.ID, "offset_after", u.Committed)
			return res, s.logResult(log, start, rerr, nil)
		}
		// Staging data is gone: fall through and start a new upload.
	case !errors.Is(err, ErrNotFound):
		return nil, s.logResult(log, start, errInternal, err)
	}

	res, err := s.create(ctx, m)
	if err != nil {
		return nil, s.logResult(log, start, errInternal, err)
	}
	return res, s.logResult(log.With("upload_ref", res.GetUploadId(), "offset_after", int64(0)), start, nil, nil)
}

// metadata converts and validates the InitRequest (FR-1).
func (s *Service) metadata(req *uploadv1.InitRequest) (meta.Metadata, error) {
	capturedAt, err := meta.ParseCapturedAt(req.GetCapturedAt())
	if err != nil {
		return meta.Metadata{}, err
	}
	m := meta.Metadata{
		DeviceID:    req.GetDeviceId(),
		SHA256:      req.GetSha256(),
		Filename:    req.GetFilename(),
		ContentType: req.GetContentType(),
		CapturedAt:  capturedAt,
		Priority:    int(req.GetPriority()),
		Size:        req.GetSize(),
	}
	if err := m.Validate(s.cfg.Now(), s.cfg.Rules); err != nil {
		return meta.Metadata{}, err
	}
	return m, nil
}

// resume answers Init for an in-progress upload of the same sha256 and
// device. It returns (nil, nil) if the staging data is missing, so the
// caller starts a new upload instead.
func (s *Service) resume(ctx context.Context, u Upload) (*uploadv1.InitResponse, error) {
	if _, err := s.store.LoadInfo(u.ID); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.log.Warn("staging data missing, starting a new upload", "upload_ref", u.ID)
			return nil, nil
		}
		return nil, s.internal(err)
	}
	res := &uploadv1.InitResponse{UploadId: u.ID, CommittedOffset: u.Committed, MaxChunkBytes: int32(s.cfg.MaxChunkBytes)}
	if u.Committed == u.Meta.Size {
		// Every byte was committed but finalization did not finish (crash).
		release, ok := s.active.acquire(u.ID)
		if !ok {
			return res, nil // a stream is finalizing it; the client asks Status
		}
		defer release()
		if err := s.finalize(ctx, u); err != nil {
			return nil, err
		}
		return nil, errCompleted
	}
	if u.Committed > 0 {
		if err := s.countResume(ctx, u.ID); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// create inserts the row first and then the staging files, so a crash in
// between leaves a row without staging data (handled by resume) rather than
// unreferenced files.
func (s *Service) create(ctx context.Context, m meta.Metadata) (*uploadv1.InitResponse, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("new upload id: %w", err)
	}
	u := Upload{ID: id.String(), Meta: m, Status: StatusUploading, CreatedAt: s.cfg.Now()}
	if err := s.repo.Insert(ctx, u); err != nil {
		return nil, fmt.Errorf("insert upload: %w", err)
	}
	f, err := s.store.Create(u.ID, staging.Info{
		DeviceID:    m.DeviceID,
		SHA256:      m.SHA256,
		Size:        m.Size,
		Filename:    m.Filename,
		ContentType: m.ContentType,
		CapturedAt:  m.CapturedAt.Format(time.RFC3339Nano),
		Priority:    int32(m.Priority),
	})
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	s.metrics.Started.WithLabelValues(Protocol).Inc()
	return &uploadv1.InitResponse{UploadId: u.ID, MaxChunkBytes: int32(s.cfg.MaxChunkBytes)}, nil
}

// Status reports the committed offset of an upload owned by the caller.
func (s *Service) Status(ctx context.Context, req *uploadv1.StatusRequest) (*uploadv1.StatusResponse, error) {
	start := time.Now()
	log := s.log.With("method", "Status", "device_id", deviceFrom(ctx), "upload_ref", req.GetUploadId())

	u, err := s.lookup(ctx, req.GetUploadId())
	if err != nil {
		return nil, s.logResult(log, start, err, nil)
	}
	log = log.With("offset_after", u.Committed)
	if u.Status == StatusCompleted {
		return &uploadv1.StatusResponse{CommittedOffset: u.Meta.Size, Size: u.Meta.Size, Complete: true}, s.logResult(log, start, nil, nil)
	}
	res := &uploadv1.StatusResponse{CommittedOffset: u.Committed, Size: u.Meta.Size}
	if u.Committed == u.Meta.Size {
		// Retry a finalization that did not finish, unless a stream holds it.
		if release, ok := s.active.acquire(u.ID); ok {
			defer release()
			if err := s.finalize(ctx, u); err != nil {
				return nil, s.logResult(log, start, err, nil)
			}
			res.Complete = true
		}
		return res, s.logResult(log, start, nil, nil)
	}
	if u.Committed > 0 {
		if err := s.countResume(ctx, u.ID); err != nil {
			return nil, s.logResult(log, start, err, nil)
		}
	}
	return res, s.logResult(log, start, nil, nil)
}

// Cancel deletes the staging data of an in-progress upload and marks it
// cancelled.
func (s *Service) Cancel(ctx context.Context, req *uploadv1.CancelRequest) (*uploadv1.CancelResponse, error) {
	start := time.Now()
	log := s.log.With("method", "Cancel", "device_id", deviceFrom(ctx), "upload_ref", req.GetUploadId())

	u, err := s.lookup(ctx, req.GetUploadId())
	if err != nil {
		return nil, s.logResult(log, start, err, nil)
	}
	if u.Status != StatusUploading {
		return nil, s.logResult(log, start, errNotFound, nil)
	}
	release, ok := s.active.acquire(u.ID)
	if !ok {
		return nil, s.logResult(log, start, errBusy, nil)
	}
	defer release()
	// Re-check under the hold: a stream may have finished the upload (or
	// the cleaner expired it) between the lookup and the acquire.
	if u, err = s.lookup(ctx, u.ID); err != nil {
		return nil, s.logResult(log, start, err, nil)
	}
	if u.Status != StatusUploading {
		return nil, s.logResult(log, start, errNotFound, nil)
	}
	// Delete first: a failed MarkCancelled can be retried, and Delete
	// tolerates missing files.
	if err := s.store.Delete(u.ID); err != nil {
		return nil, s.logResult(log, start, errInternal, err)
	}
	if err := s.repo.MarkCancelled(ctx, u.ID); errors.Is(err, ErrNotFound) {
		return nil, s.logResult(log, start, errNotFound, err)
	} else if err != nil {
		return nil, s.logResult(log, start, errInternal, err)
	}
	s.metrics.Failed.WithLabelValues(Protocol, obs.ReasonCancelled).Inc()
	return &uploadv1.CancelResponse{}, s.logResult(log, start, nil, nil)
}

// lookup loads an upload for the calling device. Unknown ids, other devices'
// uploads and uploads that are no longer usable (cancelled, expired,
// failed_checksum) are all NOT_FOUND.
func (s *Service) lookup(ctx context.Context, id string) (Upload, error) {
	if id == "" {
		return Upload{}, status.Error(codes.InvalidArgument, "upload_id: is required")
	}
	if staging.ValidateID(id) != nil {
		return Upload{}, errNotFound
	}
	u, err := s.repo.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return Upload{}, errNotFound
	}
	if err != nil {
		return Upload{}, s.internal(err)
	}
	if u.Meta.DeviceID != deviceFrom(ctx) {
		return Upload{}, errNotFound
	}
	if u.Status != StatusUploading && u.Status != StatusCompleted {
		return Upload{}, errNotFound
	}
	return u, nil
}

func (s *Service) countResume(ctx context.Context, id string) error {
	if err := s.repo.IncrementResumes(ctx, id); err != nil {
		return s.internal(err)
	}
	s.metrics.Resumes.WithLabelValues(Protocol).Inc()
	return nil
}

// finalize runs the finalizer for a fully committed upload and maps its
// result to a gRPC status. The caller must hold the upload in the registry.
// The run is detached from ctx's cancellation (a client that goes away does
// not abort it mid-way) and bounded by finalizeTimeout.
func (s *Service) finalize(ctx context.Context, u Upload) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	defer cancel()
	err := s.fin.Run(ctx, FinalizeInput{
		Protocol:  Protocol,
		Ref:       u.ID,
		Meta:      u.Meta,
		CreatedAt: u.CreatedAt,
		Open: func() (io.ReadCloser, error) {
			return s.store.Reader(u.ID)
		},
		Delete: func() error { return s.store.Delete(u.ID) },
	})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrChecksumMismatch):
		return status.Error(codes.DataLoss, "sha-256 mismatch")
	case errors.Is(err, ErrDuplicate):
		return errCompleted
	case errors.Is(err, ErrNotFound):
		// Cancelled or expired meanwhile: the client's action is "Init again".
		return errNotFound
	default:
		return s.internal(err)
	}
}

// internal logs err and returns the generic INTERNAL status, so storage
// details never reach the client (§7).
func (s *Service) internal(err error) error {
	s.log.Error("internal error", "error", err)
	return errInternal
}

// logResult writes the per-request log line (§10) and returns st. cause is
// the internal error behind st, if any, and is logged instead of st.
func (s *Service) logResult(log *slog.Logger, start time.Time, st, cause error) error {
	code := status.Code(st)
	attrs := []any{"status", code.String(), "duration_ms", time.Since(start).Milliseconds()}
	switch {
	case cause != nil:
		attrs = append(attrs, "error", cause.Error())
	case st != nil:
		attrs = append(attrs, "error", status.Convert(st).Message())
	}
	level := slog.LevelInfo
	if code == codes.Internal {
		level = slog.LevelError
	}
	log.Log(context.Background(), level, "grpc request", attrs...)
	return st
}
