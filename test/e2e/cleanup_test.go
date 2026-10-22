//go:build integration

package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/resyahrial/sensor-upload-server/internal/cleanup"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/finalize"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// noGRPC stands in for the gRPC side of the cleaner (no gRPC rows here).
type noGRPC struct{}

func (noGRPC) Acquire(string) (func(), bool)      { return func() {}, true }
func (noGRPC) Delete(string) error                { return nil }
func (noGRPC) Reader(id string) (*os.File, error) { return nil, os.ErrNotExist }
func (noGRPC) Dir() string                        { return "" }

// noFinalize fails: no upload in this test is fully staged.
type noFinalize struct{}

func (noFinalize) Run(_ context.Context, in finalize.Input) (string, error) {
	return "", fmt.Errorf("unexpected finalize of %s/%s", in.Protocol, in.Ref)
}

// TestCleanupExpiresStaleTus: a stale tus upload is expired (staging
// deleted, later requests get 410); a stale upload with a PATCH in flight
// is skipped and can still complete.
func TestCleanupExpiresStaleTus(t *testing.T) {
	s := StartServer(t, nil)
	ctx := context.Background()
	m, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	tus := s.App.TusCleanup()
	cl, err := cleanup.New(cleanup.Config{TTL: time.Hour, Interval: time.Hour}, cleanup.Deps{
		Repo: s.Repo, GRPC: noGRPC{}, GRPCStaging: noGRPC{}, Finalizer: noFinalize{},
		Tus:     map[string]cleanup.TusStaging{"rest": tus, "rest-http3": tus},
		Metrics: m, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	age := func(url string) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, `UPDATE uploads SET updated_at = now() - interval '2 hours' WHERE upload_ref = $1`, refOf(url)); err != nil {
			t.Fatal(err)
		}
	}

	type upload struct {
		ep  endpointCase
		c   *Tus
		f   File
		loc string
	}
	var stale, active []upload
	var inflights []*inflight
	for _, ep := range []endpointCase{endpoints[1], endpoints[2]} {
		dev := device()
		c := Endpoint(t, s, ep.proto, ep.base, dev)
		f := NewFile(t, dev, 2*chunk)
		loc := c.MustCreate(f)
		c.SendFrom(loc, NewFileFrom(f, chunk), 0, chunk)
		age(loc)
		stale = append(stale, upload{ep, c, f, loc})

		g := NewFile(t, dev, 2*chunk)
		la := c.MustCreate(g)
		p := Endpoint(t, s, ep.proto, ep.base, dev).StartPatch(la, 0, g.Data, chunk)
		WaitStaged(t, s, la, chunk)
		age(la)
		active = append(active, upload{ep, c, g, la})
		inflights = append(inflights, p)
	}

	n, err := cl.RunOnce(ctx)
	if err != nil || n != len(stale) {
		t.Fatalf("RunOnce = %d, %v; want %d", n, err, len(stale))
	}
	for _, u := range stale {
		if row := Row(t, s, u.ep.protocol, u.loc); row.Status != db.StatusExpired {
			t.Errorf("%s stale row = %s", u.ep.name, row.Status)
		}
		AssertNotStaged(t, s, u.loc)
		if off := u.c.Head(u.loc); off != -http.StatusGone {
			t.Errorf("%s HEAD after expiry = %d, want 410", u.ep.name, -off)
		}
		if res, _ := u.c.Patch(u.loc, chunk, u.f.Data[chunk:]); res.StatusCode != http.StatusGone {
			t.Errorf("%s PATCH after expiry = %d, want 410", u.ep.name, res.StatusCode)
		}
	}
	for i, u := range active {
		if row := Row(t, s, u.ep.protocol, u.loc); row.Status != db.StatusUploading {
			t.Errorf("%s active row = %s", u.ep.name, row.Status)
		}
		if _, err := os.Stat(filepath.Join(s.TusDir(), refOf(u.loc))); err != nil {
			t.Errorf("%s active staging deleted: %v", u.ep.name, err)
		}
		p := inflights[i]
		go func() { _, _ = p.pw.Write(u.f.Data[chunk:]); _ = p.pw.Close() }()
		if r := p.wait(t); r.err != nil || r.status != http.StatusNoContent {
			t.Fatalf("%s in-flight PATCH = %d %q %v", u.ep.name, r.status, r.body, r.err)
		}
		AssertStored(t, s, u.ep.protocol, u.loc, u.f)
	}
}
