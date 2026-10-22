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
	}
	for _, c := range []prometheus.Collector{
		m.Started, m.Completed, m.Failed, m.Resumes, m.BytesReceived,
		m.ChunkBytes, m.Duration, m.FinalizeDuration, m.InProgress, m.GRPCStreamResets,
		m.StagingOrphans,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}
