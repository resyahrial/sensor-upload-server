package obs

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Failure reasons for Metrics.Failed (requirements §10).
const (
	ReasonChecksum  = "checksum"
	ReasonDuplicate = "duplicate"
	ReasonExpired   = "expired"
	ReasonCancelled = "cancelled"
	ReasonInternal  = "internal"
)

// Results for Metrics.FileLookups.
const (
	LookupFound    = "found"
	LookupNotFound = "not_found"
	LookupInvalid  = "invalid"
	LookupError    = "error"
)

// Results for Metrics.FileLists (GET /files).
const (
	ListOK      = "ok"
	ListInvalid = "invalid"
	ListError   = "error"
)

// Results for Metrics.DeviceRegister (POST /v1/devices/register).
const (
	RegisterPending        = "pending"         // new or still pending: 202
	RegisterReregistered   = "reregistered"    // active with expired family, back to pending: 202
	RegisterIssued         = "issued"          // active: 200 token pair
	RegisterNotActive      = "not_active"      // rejected or revoked: 403
	RegisterSecretMismatch = "secret_mismatch" // 409
	RegisterInvalid        = "invalid"         // 400
	RegisterRateLimited    = "rate_limited"    // 429
	RegisterError          = "error"           // 503
)

// Results for Metrics.TokenRefresh (POST /v1/devices/token/refresh).
const (
	RefreshOK           = "ok"
	RefreshInvalid      = "invalid"       // malformed request: 400
	RefreshInvalidGrant = "invalid_grant" // unknown or superseded-by-reset token: 401
	RefreshReuse        = "reuse"         // rotated-out token presented again: 401
	RefreshExpired      = "expired"       // idle or max age exceeded: 401
	RefreshNotActive    = "not_active"    // 403
	RefreshError        = "error"         // 503
)

// Reasons for Metrics.RetentionDeleted.
const (
	RetentionAge  = "age"  // completed longer than RETENTION_MAX_AGE ago
	RetentionDisk = "disk" // disk usage at or above DISK_HIGH_WATERMARK
)

// Metrics holds every upload metric from requirements §10. All upload metrics are
// labelled by protocol; Failed is additionally labelled by reason.
type Metrics struct {
	Started          *prometheus.CounterVec
	Completed        *prometheus.CounterVec
	Failed           *prometheus.CounterVec
	Resumes          *prometheus.CounterVec
	BytesReceived    *prometheus.CounterVec
	ChunkBytes       *prometheus.HistogramVec
	Duration         *prometheus.HistogramVec
	FinalizeDuration *prometheus.HistogramVec
	InProgress       *prometheus.GaugeVec
	GRPCStreamResets *prometheus.CounterVec
	// StagingOrphans counts uploads whose leftover staged files the
	// cleaner deleted, labelled by staging area ("grpc" or "tus").
	StagingOrphans *prometheus.CounterVec
	// FileLookups counts GET /files/sha256/{sha256} requests, labelled by
	// result (one of the Lookup* constants).
	FileLookups *prometheus.CounterVec
	// FileLists counts GET /files requests, labelled by result (one of
	// the List* constants).
	FileLists *prometheus.CounterVec
	// RetentionDeleted counts completed uploads (object and row) deleted
	// by retention, labelled by reason (RetentionAge, RetentionDisk).
	RetentionDeleted *prometheus.CounterVec
	// DeviceRegister counts device registration requests, by result
	// (Register* constants).
	DeviceRegister *prometheus.CounterVec
	// TokenRefresh counts refresh token requests, by result (Refresh*
	// constants); result "reuse" is a security event.
	TokenRefresh *prometheus.CounterVec
	// DiskUsedRatio is the used fraction (0..1) of DISK_PATH's
	// filesystem at the last disk check.
	DiskUsedRatio prometheus.Gauge
}

// NewMetrics creates the upload metrics and registers them with reg.
// Passing a fresh prometheus.NewRegistry() keeps tests isolated.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	proto := []string{"protocol"}
	m := &Metrics{
		Started: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upload_started_total", Help: "Uploads created.",
		}, proto),
		Completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upload_completed_total", Help: "Uploads verified and stored.",
		}, proto),
		Failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upload_failed_total", Help: "Uploads that ended without being stored, by reason.",
		}, []string{"protocol", "reason"}),
		Resumes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upload_resumes_total", Help: "Offset queries that resumed an upload at offset > 0.",
		}, proto),
		BytesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upload_bytes_received_total", Help: "Payload bytes durably received.",
		}, proto),
		ChunkBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "upload_chunk_bytes",
			Help:    "Size of received chunks in bytes.",
			Buckets: prometheus.ExponentialBuckets(4<<10, 2, 12), // 4 KiB .. 8 MiB
		}, proto),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "upload_duration_seconds",
			Help:    "Time from upload creation to completion.",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 16), // 0.5 s .. ~4.5 h
		}, proto),
		FinalizeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "upload_finalize_seconds",
			Help:    "Time spent verifying and storing a completed upload.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14), // 10 ms .. ~80 s
		}, proto),
		InProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "upload_in_progress", Help: "Uploads currently receiving data.",
		}, proto),
		GRPCStreamResets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_stream_resets_total", Help: "gRPC upload streams that ended with an error before completion.",
		}, proto),
		StagingOrphans: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "staging_orphans_deleted_total", Help: "Uploads whose leftover staged files (terminal or unknown row) were deleted.",
		}, []string{"staging"}),
		FileLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "file_lookups_total", Help: "Public \"is this file saved?\" lookups by sha256, by result.",
		}, []string{"result"}),
		FileLists: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "file_lists_total", Help: "Public listings of completed uploads (GET /files), by result.",
		}, []string{"result"}),
		RetentionDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "uploads_retention_deleted_total", Help: "Completed uploads deleted by retention (object and row), by reason (age, disk).",
		}, []string{"reason"}),
		DeviceRegister: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "device_register_total", Help: "Device registration requests, by result.",
		}, []string{"result"}),
		TokenRefresh: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "token_refresh_total", Help: "Refresh token requests, by result (reuse = rotated-out token presented again).",
		}, []string{"result"}),
		DiskUsedRatio: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "disk_used_ratio", Help: "Used fraction of the DISK_PATH filesystem (as df: used / (used + available)) at the last check.",
		}),
	}
	for _, c := range []prometheus.Collector{
		m.Started, m.Completed, m.Failed, m.Resumes, m.BytesReceived,
		m.ChunkBytes, m.Duration, m.FinalizeDuration, m.InProgress, m.GRPCStreamResets,
		m.StagingOrphans, m.FileLookups, m.FileLists, m.RetentionDeleted, m.DeviceRegister, m.TokenRefresh, m.DiskUsedRatio,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}
