// Package cleanup expires incomplete uploads (FR-9) and finishes uploads
// whose finalization (FR-8) was interrupted. Every CLEANUP_INTERVAL it
//
//  1. retries the finalizer for 'uploading' rows whose bytes are all
//     staged (a transient finalize failure or a crash after the last byte),
//  2. lists the 'uploading' rows not updated for INCOMPLETE_TTL and
//     finalizes them if fully staged, otherwise deletes their staged data
//     and marks them 'expired'.
//
// Uploads that are in use (a tus request holds the lock, or a gRPC stream
// holds the upload) are skipped.
package cleanup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/finalize"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// GRPCProtocol is the uploads.protocol value of gRPC uploads.
const GRPCProtocol = "grpc"

// finalizeRetryLimit bounds the finalize retries of not yet stale, fully
// committed rows per pass; the rest wait for the next pass.
const finalizeRetryLimit = 32

// Repo is the subset of *db.Repo the cleaner uses.
type Repo interface {
	ListStale(ctx context.Context, ttl time.Duration) ([]db.Upload, error)
	ListFullyCommitted(ctx context.Context) ([]db.Upload, error)
	Get(ctx context.Context, protocol, ref string) (db.Upload, error)
	SetStatus(ctx context.Context, protocol, ref string, s db.Status) error
}

// Finalizer runs FR-8; *finalize.Finalizer satisfies it.
type Finalizer interface {
	Run(ctx context.Context, in finalize.Input) (string, error)
}

// GRPCUploads is the gRPC service's single-holder registry; *grpcapi.Service
// satisfies it.
type GRPCUploads interface {
	// Acquire claims upload id without waiting, so no stream, Cancel or
	// finalize can run on it until release is called. ok=false means it is
	// in use.
	Acquire(id string) (release func(), ok bool)
}

// GRPCStaging is the gRPC staging store; *staging.Store satisfies it.
type GRPCStaging interface {
	// Delete removes the staged data of id; missing data is not an error.
	Delete(id string) error
	// Reader opens the staged data of id read-only.
	Reader(id string) (*os.File, error)
	// Dir is the staging directory ({id}.bin, {id}.json); "" disables
	// the orphan sweep for gRPC.
	Dir() string
}

// TusStaging is the tus side of the cleaner, implemented by the tus package.
// ref is the uploads.upload_ref, which is the tusd upload ID. Both methods
// must be safe for concurrent use with tus requests.
type TusStaging interface {
	// TryLock takes the upload's lock without waiting and without asking a
	// current holder to release it: an in-flight tus request must never be
	// interrupted. It returns ok=false (and a nil unlock) if the lock is
	// held. On ok=true no tus request can operate on the upload until
	// unlock is called; unlock must be safe to call exactly once. err is
	// reserved for failures to determine the lock state.
	TryLock(ctx context.Context, ref string) (unlock func(), ok bool, err error)
	// Delete removes the staged upload (data and info). It is called only
	// while the caller holds the lock from TryLock and must return nil if
	// the upload is already gone. A later tus request for ref must get
	// 404/410.
	Delete(ctx context.Context, ref string) error
}

// TusFiles reads staged tus uploads, so the cleaner can finalize a fully
// staged one without a request. Both methods are called only while the
// caller holds the lock from TusStaging.TryLock.
type TusFiles interface {
	// Offset returns the number of bytes staged for ref. If nothing is
	// staged it returns an error matching fs.ErrNotExist.
	Offset(ctx context.Context, ref string) (int64, error)
	// Open returns a reader over the staged bytes of ref.
	Open(ctx context.Context, ref string) (io.ReadCloser, error)
}

// Config holds the cleaner settings.
type Config struct {
	// TTL is INCOMPLETE_TTL.
	TTL time.Duration
	// Interval is CLEANUP_INTERVAL.
	Interval time.Duration
	// TusDir is the tus filestore directory ({id}, {id}.info, {id}.lock,
	// {id}.stop) for the orphan sweep; "" disables the sweep for tus.
	TusDir string
	// OrphanAge is how old every staged file of an upload must be before
	// the orphan sweep may delete them; 0 means DefaultOrphanAge.
	OrphanAge time.Duration
}

// Deps are the cleaner's collaborators. Repo, GRPC, GRPCStaging,
// Finalizer, Metrics and Logger are required.
type Deps struct {
	Repo        Repo
	GRPC        GRPCUploads
	GRPCStaging GRPCStaging
	Finalizer   Finalizer
	// Tus maps each tus protocol ("rest", "rest-http3") to its staging. A
	// stale row of a protocol missing here is reported as an error and left
	// alone.
	Tus map[string]TusStaging
	// TusFiles reads the staged tus uploads of every tus protocol (refs
	// are unique across them). Nil means the cleaner cannot tell whether a
	// tus upload is fully staged (s3store staging) and expires stale tus
	// rows without trying to finalize them.
	TusFiles TusFiles
	Metrics  *obs.Metrics
	Logger   *slog.Logger
}

// Cleaner expires stale incomplete uploads and retries interrupted
// finalizations.
type Cleaner struct {
	cfg     Config
	repo    Repo
	fin     Finalizer
	staging map[string]backend // by protocol, gRPC included
	grpcDir string             // gRPC staging dir for the orphan sweep
	metrics *obs.Metrics
	log     *slog.Logger
}

// New checks cfg and d and returns a Cleaner.
func New(cfg Config, d Deps) (*Cleaner, error) {
	switch {
	case cfg.TTL <= 0:
		return nil, errors.New("cleanup: TTL must be positive")
	case cfg.Interval <= 0:
		return nil, errors.New("cleanup: Interval must be positive")
	case cfg.OrphanAge < 0:
		return nil, errors.New("cleanup: OrphanAge must not be negative")
	case d.Repo == nil || d.GRPC == nil || d.GRPCStaging == nil || d.Finalizer == nil || d.Metrics == nil || d.Logger == nil:
		return nil, errors.New("cleanup: missing dependency")
	}
	st := make(map[string]backend, len(d.Tus)+1)
	for p, s := range d.Tus {
		if p == GRPCProtocol || s == nil {
			return nil, fmt.Errorf("cleanup: invalid tus staging for protocol %q", p)
		}
		st[p] = tusBackend{TusStaging: s, files: d.TusFiles}
	}
	st[GRPCProtocol] = grpcBackend{uploads: d.GRPC, store: d.GRPCStaging}
	if cfg.OrphanAge == 0 {
		cfg.OrphanAge = DefaultOrphanAge
	}
	return &Cleaner{
		cfg: cfg, repo: d.Repo, fin: d.Finalizer, staging: st, grpcDir: d.GRPCStaging.Dir(),
		metrics: d.Metrics, log: d.Logger.With("component", "cleanup"),
	}, nil
}

// Run calls RunOnce at start and then every Interval until ctx is done.
// Errors are logged.
func (c *Cleaner) Run(ctx context.Context) {
	t := time.NewTicker(c.cfg.Interval)
	defer t.Stop()
	for {
		n, err := c.RunOnce(ctx)
		if err != nil {
			c.log.Error("expiry run failed", "expired", n, "error", err.Error())
		} else if n > 0 {
			c.log.Info("expiry run", "expired", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// pass is the state of one RunOnce.
type pass struct {
	// tried holds the uploads the finalizer already ran for, so an upload
	// listed both as fully committed and as stale is attempted once; the
	// value is finalizeIfStaged's expire result.
	tried map[[2]string]bool
	errs  []error
}

func (p *pass) fail(err error) { p.errs = append(p.errs, err) }

// RunOnce retries interrupted finalizations, expires every stale upload
// that is not in use and not fully staged, then deletes orphaned staged
// files, and returns how many uploads it expired. A failure on one upload
// does not stop the others; all failures are returned joined.
func (c *Cleaner) RunOnce(ctx context.Context) (expired int, err error) {
	p := &pass{tried: map[[2]string]bool{}}
	c.retryFinalize(ctx, p)
	expired = c.expireStale(ctx, p)
	if ctx.Err() == nil {
		c.sweepOrphans(ctx, p)
	}
	return expired, errors.Join(p.errs...)
}

// expireStale runs expire for every stale upload and returns how many it
// expired.
func (c *Cleaner) expireStale(ctx context.Context, p *pass) (expired int) {
	stale, err := c.repo.ListStale(ctx, c.cfg.TTL)
	if err != nil {
		p.fail(fmt.Errorf("cleanup: %w", err))
		return 0
	}
	for _, u := range stale {
		if err := ctx.Err(); err != nil {
			p.fail(err)
			break
		}
		ok, err := c.expire(ctx, p, u)
		if err != nil {
			p.fail(fmt.Errorf("cleanup: expire %s/%s: %w", u.Protocol, u.Ref, err))
			continue
		}
		if ok {
			expired++
		}
	}
	return expired
}

// retryFinalize runs the finalizer for up to finalizeRetryLimit
// 'uploading' rows with every byte committed. For gRPC that is a stream
// whose finalize failed and whose client has not come back yet (tus never
// updates committed_bytes before completion, so tus rows are finalized only
// once stale, by expire, or by the client's next HEAD/PATCH).
func (c *Cleaner) retryFinalize(ctx context.Context, p *pass) {
	rows, err := c.repo.ListFullyCommitted(ctx)
	if err != nil {
		p.fail(fmt.Errorf("cleanup: %w", err))
		return
	}
	n := 0
	for _, u := range rows {
		if n == finalizeRetryLimit || ctx.Err() != nil {
			return // the next pass continues
		}
		st, ok := c.staging[u.Protocol]
		if !ok {
			continue // reported by expire once stale
		}
		ran, err := c.withLock(ctx, st, u, func(cur db.Upload) (bool, error) {
			if cur.Status != db.StatusUploading {
				return false, nil
			}
			_, ran, err := c.finalizeIfStaged(ctx, p, st, cur)
			return ran, err
		})
		if err != nil {
			p.fail(fmt.Errorf("cleanup: finalize %s/%s: %w", u.Protocol, u.Ref, err))
		}
		if ran {
			n++
		}
	}
}

// withLock runs f with the upload's lock held and its current row. It
// returns false without calling f if the upload is in use.
func (c *Cleaner) withLock(ctx context.Context, st backend, u db.Upload, f func(cur db.Upload) (bool, error)) (bool, error) {
	unlock, ok, err := st.TryLock(ctx, u.Ref)
	if err != nil {
		return false, fmt.Errorf("lock: %w", err)
	}
	if !ok {
		c.log.Debug("upload in use, skipped", "protocol", u.Protocol, "upload_ref", u.Ref)
		return false, nil
	}
	defer unlock()
	cur, err := c.repo.Get(ctx, u.Protocol, u.Ref)
	if err != nil {
		return false, err
	}
	return f(cur)
}

// finalizeIfStaged runs the finalizer for u (whose lock the caller holds)
// if every byte of it is staged and it was not tried in this pass yet.
// ran reports whether the finalizer ran; expire reports whether the
// upload may still be expired: nothing ran because it is not fully staged,
// or the finalizer found the row inactive (cancelled, expired or gone). A
// finalizer error other than an outcome is returned, and the upload is
// left for the next pass.
func (c *Cleaner) finalizeIfStaged(ctx context.Context, p *pass, st backend, u db.Upload) (expire, ran bool, err error) {
	k := [2]string{u.Protocol, u.Ref}
	if expire, ok := p.tried[k]; ok {
		return expire, false, nil
	}
	full, err := st.staged(ctx, u)
	if err != nil {
		return false, false, fmt.Errorf("check staged data: %w", err)
	}
	if !full {
		return true, false, nil
	}
	key, err := c.fin.Run(ctx, finalize.Input{
		Protocol: u.Protocol,
		Ref:      u.Ref,
		Open:     func(ctx context.Context) (io.ReadCloser, error) { return st.open(ctx, u.Ref) },
		Delete:   func(ctx context.Context) error { return st.Delete(ctx, u.Ref) },
	})
	expire = errors.Is(err, finalize.ErrInactive) || errors.Is(err, db.ErrNotFound)
	p.tried[k] = expire
	switch {
	case err == nil:
		c.log.Info("fully staged upload finalized", "protocol", u.Protocol, "upload_ref", u.Ref,
			"device_id", u.DeviceID, "storage_key", key)
		return false, true, nil
	case expire, errors.Is(err, finalize.ErrChecksum), errors.Is(err, finalize.ErrDuplicate):
		// An outcome: recorded, staging deleted and logged by Run.
		return expire, true, nil
	default:
		return false, true, fmt.Errorf("finalize: %w", err)
	}
}

// expire handles one stale upload while holding its lock: a fully staged
// upload is finalized, any other is expired. It reports whether it expired
// the upload; false without error means it was in use, changed since it
// was listed, or was finalized.
//
// Staging is deleted before the row is marked expired: a crash in between
// leaves an 'uploading' row without staged data, which the next run lists
// and expires again (and both protocols already treat missing staging data
// as "upload gone"). The other order would leave staged files behind an
// 'expired' row (which the orphan sweep would delete, later).
func (c *Cleaner) expire(ctx context.Context, p *pass, u db.Upload) (bool, error) {
	st, ok := c.staging[u.Protocol]
	if !ok {
		return false, errors.New("no staging configured for protocol")
	}
	log := c.log.With("protocol", u.Protocol, "upload_ref", u.Ref, "device_id", u.DeviceID)
	return c.withLock(ctx, st, u, func(cur db.Upload) (bool, error) {
		// Re-check under the lock: a request may have used the upload
		// between ListStale and TryLock.
		if cur.Status != db.StatusUploading || !cur.UpdatedAt.Equal(u.UpdatedAt) {
			log.Debug("stale upload changed meanwhile, skipped", "status", string(cur.Status))
			return false, nil
		}
		// A complete upload is never thrown away: finalize it instead.
		if ok, _, err := c.finalizeIfStaged(ctx, p, st, cur); err != nil || !ok {
			return false, err
		}

		if err := st.Delete(ctx, u.Ref); err != nil {
			return false, fmt.Errorf("delete staging: %w", err)
		}
		if err := c.repo.SetStatus(ctx, u.Protocol, u.Ref, db.StatusExpired); err != nil {
			if errors.Is(err, db.ErrNotUploading) {
				log.Info("stale upload finished meanwhile, not expired", "error", err.Error())
				return false, nil
			}
			return false, fmt.Errorf("mark expired: %w", err)
		}
		c.metrics.Failed.WithLabelValues(u.Protocol, obs.ReasonExpired).Inc()
		log.Info("upload expired",
			"committed_bytes", cur.CommittedBytes, "size_bytes", cur.Size,
			"updated_at", cur.UpdatedAt, "ttl", c.cfg.TTL.String())
		return true, nil
	})
}

// backend is one protocol's staging as the cleaner uses it.
type backend interface {
	TusStaging
	// staged reports whether every byte of u is staged, so the finalizer
	// can run on it.
	staged(ctx context.Context, u db.Upload) (bool, error)
	open(ctx context.Context, ref string) (io.ReadCloser, error)
}

// tusBackend adds TusFiles (if any) to a TusStaging.
type tusBackend struct {
	TusStaging
	files TusFiles // nil: unknown, never fully staged
}

func (t tusBackend) staged(ctx context.Context, u db.Upload) (bool, error) {
	if t.files == nil {
		return false, nil
	}
	n, err := t.files.Offset(ctx, u.Ref)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return n == u.Size, nil
}

func (t tusBackend) open(ctx context.Context, ref string) (io.ReadCloser, error) {
	if t.files == nil {
		return nil, errors.New("tus staging is not readable")
	}
	return t.files.Open(ctx, ref)
}

// grpcBackend adapts the gRPC registry and staging store to the tus shape,
// so both protocols go through the same paths.
type grpcBackend struct {
	uploads GRPCUploads
	store   GRPCStaging
}

func (g grpcBackend) TryLock(_ context.Context, ref string) (func(), bool, error) {
	release, ok := g.uploads.Acquire(ref)
	return release, ok, nil
}

func (g grpcBackend) Delete(_ context.Context, ref string) error { return g.store.Delete(ref) }

// staged requires every byte to be committed (acked; committed_bytes is
// authoritative, the .bin may hold a torn unacked write) and present in
// the .bin.
func (g grpcBackend) staged(_ context.Context, u db.Upload) (bool, error) {
	if u.CommittedBytes != u.Size {
		return false, nil
	}
	f, err := g.store.Reader(u.Ref)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close() // read-only
	fi, err := f.Stat()
	if err != nil {
		return false, fmt.Errorf("stat staged data: %w", err)
	}
	return fi.Size() == u.Size, nil
}

func (g grpcBackend) open(_ context.Context, ref string) (io.ReadCloser, error) {
	return g.store.Reader(ref)
}
