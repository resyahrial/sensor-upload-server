package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewMetricsRegistersAll(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	m.Started.WithLabelValues("rest").Inc()
	m.Failed.WithLabelValues("grpc", ReasonChecksum).Inc()
	m.ChunkBytes.WithLabelValues("rest").Observe(262144)
	m.InProgress.WithLabelValues("rest-http3").Set(2)
	m.RetentionDeleted.WithLabelValues(RetentionAge).Inc()
	m.RetentionDeleted.WithLabelValues(RetentionDisk).Add(2)
	m.DiskUsedRatio.Set(0.5)

	if got := testutil.ToFloat64(m.Failed.WithLabelValues("grpc", ReasonChecksum)); got != 1 {
		t.Errorf("failed counter = %v", got)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, want := range []string{"upload_started_total", "upload_failed_total", "upload_chunk_bytes", "upload_in_progress",
		"uploads_retention_deleted_total", "disk_used_ratio"} {
		if !names[want] {
			t.Errorf("metric %s not gathered", want)
		}
	}
	if _, err := NewMetrics(reg); err == nil {
		t.Error("registering twice on the same registry should fail")
	}
}
