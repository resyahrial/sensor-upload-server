package cleanup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// DefaultRetentionBatch is how many completed uploads one retention batch
// lists and deletes.
const DefaultRetentionBatch = 500

// RetentionRepo is the subset of *db.Repo retention uses.
type RetentionRepo interface {
	// ListCompleted returns up to limit completed rows completed more
	// than minAge ago (0: all), oldest first.
	ListCompleted(ctx context.Context, minAge time.Duration, limit int) ([]db.Upload, error)
	// DeleteCompleted deletes the completed row id, calling deleteObject
	// with its storage key before the delete commits. ErrNotFound means
	// there is no such completed row (any more).
	DeleteCompleted(ctx context.Context, id uuid.UUID, deleteObject func(ctx context.Context, storageKey string) error) error
}

// ObjectDeleter removes stored objects; objstore.Store satisfies it.
// Deleting a missing key must not be an error.
type ObjectDeleter interface {
	Delete(ctx context.Context, key string) error
}

// DiskUsage is a filesystem's size as df reports it: Total is used plus
// the space available to unprivileged users (blocks reserved for root
// count as neither).
type DiskUsage struct {
	Total uint64 // bytes
	Used  uint64 // bytes
}

// Ratio is Used/Total (0 for an empty filesystem).
func (d DiskUsage) Ratio() float64 {
	if d.Total == 0 {
		return 0
	}
	return float64(d.Used) / float64(d.Total)
}

// StatFunc reports the usage of the filesystem holding path; Statfs is
// the real one.
type StatFunc func(path string) (DiskUsage, error)

// RetentionConfig holds the retention settings.
type RetentionConfig struct {
	// MaxAge is RETENTION_MAX_AGE; 0 disables age-based deletion.
	MaxAge time.Duration
	// Interval is how often the age pass runs (CLEANUP_INTERVAL).
	Interval time.Duration
	// DiskPath is the watched filesystem (DISK_PATH).
	DiskPath string
	// HighWatermark and LowWatermark are used-space percentages
	// (DISK_HIGH_WATERMARK, DISK_LOW_WATERMARK), 1..100, low < high.
	HighWatermark int
	LowWatermark  int
	// DiskInterval is DISK_CHECK_INTERVAL.
	DiskInterval time.Duration
	// BatchSize is the number of uploads per batch; 0 means
	// DefaultRetentionBatch.
	BatchSize int
}

// RetentionDeps are retention's collaborators; all but Stat are required.
type RetentionDeps struct {
	Repo  RetentionRepo
	Store ObjectDeleter
	// Stat measures DiskPath; nil means Statfs.
	Stat    StatFunc
	Metrics *obs.Metrics
	Logger  *slog.Logger
}

// Retention deletes completed uploads (object, then row) once they are
// older than MaxAge, and the oldest ones whenever DiskPath's filesystem is
// at or above the high watermark, until it is below the low watermark.
// Uploads that are not completed are never touched.
type Retention struct {
	cfg       RetentionConfig
	high, low float64 // ratios
	repo      RetentionRepo
	store     ObjectDeleter
	stat      StatFunc
	metrics   *obs.Metrics
	log       *slog.Logger
}

// NewRetention checks cfg and d, measures DiskPath once (so a missing
// mount fails at startup) and returns a Retention.
func NewRetention(cfg RetentionConfig, d RetentionDeps) (*Retention, error) {
	switch {
	case cfg.MaxAge < 0:
		return nil, errors.New("retention: MaxAge must not be negative")
	case cfg.Interval <= 0 || cfg.DiskInterval <= 0:
		return nil, errors.New("retention: Interval and DiskInterval must be positive")
	case cfg.DiskPath == "":
		return nil, errors.New("retention: DiskPath is required")
	case cfg.LowWatermark < 1 || cfg.HighWatermark > 100 || cfg.LowWatermark >= cfg.HighWatermark:
		return nil, fmt.Errorf("retention: watermarks must satisfy 1 <= low (%d) < high (%d) <= 100",
			cfg.LowWatermark, cfg.HighWatermark)
	case cfg.BatchSize < 0:
		return nil, errors.New("retention: BatchSize must not be negative")
	case d.Repo == nil || d.Store == nil || d.Metrics == nil || d.Logger == nil:
		return nil, errors.New("retention: missing dependency")
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultRetentionBatch
	}
	if d.Stat == nil {
		d.Stat = Statfs
	}
	if _, err := d.Stat(cfg.DiskPath); err != nil {
		return nil, fmt.Errorf("retention: DISK_PATH: %w", err)
	}
	return &Retention{
		cfg: cfg, high: float64(cfg.HighWatermark) / 100, low: float64(cfg.LowWatermark) / 100,
		repo: d.Repo, store: d.Store, stat: d.Stat, metrics: d.Metrics,
		log: d.Logger.With("component", "retention"),
	}, nil
}

// Run checks the disk and runs the age pass at start, then checks the
// disk every DiskInterval and runs the age pass every Interval, until ctx
// is done. Both run in this goroutine, never concurrently. Errors are
// logged.
func (r *Retention) Run(ctx context.Context) {
	disk := time.NewTicker(r.cfg.DiskInterval)
	defer disk.Stop()
	age := time.NewTicker(r.cfg.Interval)
	defer age.Stop()
	r.runDisk(ctx)
	r.runAge(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-disk.C:
			r.runDisk(ctx)
		case <-age.C:
			r.runAge(ctx)
		}
	}
}

func (r *Retention) runDisk(ctx context.Context) {
	if n, err := r.RelieveDisk(ctx); err != nil && ctx.Err() == nil {
		r.log.Error("disk pressure check failed", "deleted", n, "error", err.Error())
	}
}

func (r *Retention) runAge(ctx context.Context) {
	if n, err := r.PurgeAged(ctx); err != nil && ctx.Err() == nil {
		r.log.Error("retention pass failed", "deleted", n, "error", err.Error())
	}
}

// PurgeAged deletes every completed upload older than MaxAge, in batches
// of BatchSize, and returns how many it deleted. A batch with a failure
// ends the pass (the failed uploads stay for the next one), so a
// persistent failure cannot loop.
func (r *Retention) PurgeAged(ctx context.Context) (int, error) {
	if r.cfg.MaxAge == 0 {
		return 0, nil
	}
	total := 0
	for {
		batch, err := r.repo.ListCompleted(ctx, r.cfg.MaxAge, r.cfg.BatchSize)
		if err != nil {
			return total, fmt.Errorf("retention: %w", err)
		}
		if len(batch) == 0 {
			return total, nil
		}
		n, err := r.deleteBatch(ctx, batch, obs.RetentionAge, nil)
		total += n
		r.log.Info("retention batch", "reason", obs.RetentionAge, "listed", len(batch), "deleted", n,
			"max_age", r.cfg.MaxAge.String(), "failed", err != nil)
		if err != nil || len(batch) < r.cfg.BatchSize {
			return total, err
		}
	}
}

// RelieveDisk measures DiskPath and, at or above the high watermark,
// deletes the oldest completed uploads, re-measuring after each, until
// usage is below the low watermark. It stops early, with a warning, when
// nothing completed is left or a whole batch did not lower the usage
// (DiskPath is not where the objects live, or the store frees space
// later); the next check continues. It returns how many it deleted.
func (r *Retention) RelieveDisk(ctx context.Context) (int, error) {
	du, err := r.measure()
	if err != nil {
		return 0, err
	}
	if du.Ratio() < r.high {
		return 0, nil
	}
	r.log.Warn("disk usage at or above high watermark, deleting oldest completed uploads",
		"disk_path", r.cfg.DiskPath, "used_ratio", du.Ratio(), "high_watermark", r.high, "low_watermark", r.low)
	total := 0
	for {
		batch, err := r.repo.ListCompleted(ctx, 0, r.cfg.BatchSize)
		if err != nil {
			return total, fmt.Errorf("retention: %w", err)
		}
		if len(batch) == 0 {
			if du.Ratio() >= r.high {
				r.log.Warn("disk usage still at or above high watermark; no completed uploads left to delete",
					"disk_path", r.cfg.DiskPath, "used_ratio", du.Ratio(), "high_watermark", r.high)
			}
			return total, nil
		}
		before := du
		reached := false
		n, err := r.deleteBatch(ctx, batch, obs.RetentionDisk, func() (bool, error) {
			cur, err := r.measure()
			if err != nil {
				return true, err
			}
			du = cur
			reached = du.Ratio() < r.low
			return reached, nil
		})
		total += n
		r.log.Info("retention batch", "reason", obs.RetentionDisk, "listed", len(batch), "deleted", n,
			"used_ratio_before", before.Ratio(), "used_ratio", du.Ratio(), "failed", err != nil)
		switch {
		case err != nil, reached:
			return total, err
		case n > 0 && du.Used >= before.Used:
			r.log.Warn("disk usage did not drop after deleting a batch; stopping until the next check "+
				"(is DISK_PATH the object store's disk?)", "disk_path", r.cfg.DiskPath, "used_ratio", du.Ratio())
			return total, nil
		}
	}
}

// measure stats DiskPath and updates disk_used_ratio.
func (r *Retention) measure() (DiskUsage, error) {
	du, err := r.stat(r.cfg.DiskPath)
	if err != nil {
		return DiskUsage{}, fmt.Errorf("retention: stat %s: %w", r.cfg.DiskPath, err)
	}
	r.metrics.DiskUsedRatio.Set(du.Ratio())
	return du, nil
}

// deleteBatch deletes each upload of batch (object, then row) and returns
// how many it deleted. after (if not nil) runs after each delete; stop
// ends the batch early. Rows already gone are skipped silently; the other
// failures are returned joined.
func (r *Retention) deleteBatch(ctx context.Context, batch []db.Upload, reason string, after func() (stop bool, err error)) (int, error) {
	n := 0
	var errs []error
	for _, u := range batch {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		err := r.repo.DeleteCompleted(ctx, u.ID, r.store.Delete)
		switch {
		case errors.Is(err, db.ErrNotFound):
			continue // deleted meanwhile
		case err != nil:
			errs = append(errs, fmt.Errorf("retention: delete %s/%s: %w", u.Protocol, u.Ref, err))
			continue
		}
		n++
		r.metrics.RetentionDeleted.WithLabelValues(reason).Inc()
		r.log.Debug("completed upload deleted", "reason", reason, "protocol", u.Protocol, "upload_ref", u.Ref,
			"device_id", u.DeviceID, "storage_key", u.StorageKey, "size_bytes", u.Size, "completed_at", u.CompletedAt)
		if after != nil {
			stop, err := after()
			if err != nil {
				errs = append(errs, err)
			}
			if stop {
				break
			}
		}
	}
	return n, errors.Join(errs...)
}
