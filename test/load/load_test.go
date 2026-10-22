//go:build load

package load

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

const (
	smokeSize = 64 << 10
	// probeSize is the fresh upload sent while NFR-3 holds uploads open.
	probeSize      = 1 << 20
	probeTimeout   = 30 * time.Second
	healthzLimit   = time.Second
	nfr2Timeout    = 60 * time.Minute
	diskMarginByte = 4 << 30
)

// outcome is the result of one upload.
type outcome struct {
	Device   string
	Ref      string
	Duration time.Duration
	Err      error
}

// TestNFR2Memory uploads LOAD_FILES files of LOAD_FILE_SIZE concurrently in
// LOAD_CHUNK pieces and asserts peak server RSS < LOAD_RSS_LIMIT and every
// row completed.
func TestNFR2Memory(t *testing.T) {
	// Staging holds every file; completed objects are deleted as soon as
	// they are verified, but up to all of them can exist at once.
	need := 2*int64(cfg.Files)*cfg.FileSize + diskMarginByte
	free, err := freeBytes(cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if free < need {
		t.Fatalf("not enough disk space in %s: %.1f GB free, need %.1f GB for %d x %s "+
			"(lower LOAD_FILES/LOAD_FILE_SIZE or set LOAD_DIR)", cfg.Dir, float64(free)/1e9, float64(need)/1e9,
			cfg.Files, mb(cfg.FileSize))
	}

	s := startServer(t)
	smoke(t, s, 0xfeed2)

	files := make([]file, cfg.Files)
	start := time.Now()
	if err := forEach(cfg.Files, workers(), func(i int) error {
		f, err := newFile(fmt.Sprintf("load2-%03d", i), uint64(i), cfg.FileSize)
		files[i] = f
		return err
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("generated %d x %s content hashes in %s", cfg.Files, mb(cfg.FileSize), time.Since(start).Round(time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), nfr2Timeout)
	defer cancel()
	rss := sampleRSS(s.PID)
	time.Sleep(time.Second) // idle baseline samples
	start = time.Now()
	results := make([]outcome, len(files))
	var wg sync.WaitGroup
	for i, f := range files {
		wg.Go(func() { results[i] = uploadAndVerify(ctx, s, f) })
	}
	wg.Wait()
	wall := time.Since(start)
	st := rss.Stop()

	total := int64(len(files)) * cfg.FileSize
	durs := durations(results)
	t.Logf("RESULT NFR-2 protocol=%s files=%d size=%s chunk=%d server_gomaxprocs=%d", cfg.Protocol, cfg.Files,
		mb(cfg.FileSize), cfg.Chunk, cfg.ServerProcs)
	t.Logf("RESULT NFR-2 wall=%s throughput=%.1f MB/s total=%s per-upload p50=%s max=%s",
		wall.Round(time.Millisecond), float64(total)/1e6/wall.Seconds(), mb(total), pct(durs, 50), pct(durs, 100))
	t.Logf("RESULT NFR-2 server RSS %s (limit %s)", st, mb(cfg.RSSLimit))

	if failed := failures(results); len(failed) > 0 {
		t.Errorf("%d of %d uploads failed; first: %s: %v", len(failed), len(results), failed[0].Device, failed[0].Err)
	}
	if st.Peak >= cfg.RSSLimit {
		t.Errorf("peak server RSS %s >= limit %s", mb(st.Peak), mb(cfg.RSSLimit))
	}
	if !s.alive() {
		t.Errorf("server died during the test")
	}
}

// TestNFR3Concurrency holds LOAD_INPROGRESS uploads in progress at once
// (first chunk sent, the rest trickling in over LOAD_HOLD), checks that
// /healthz answers and a fresh upload completes meanwhile, then that every
// held upload completes.
func TestNFR3Concurrency(t *testing.T) {
	s := startServer(t)
	smoke(t, s, 0xfeed3)

	n := cfg.InProgress
	files := make([]file, n)
	if err := forEach(n, workers(), func(i int) error {
		f, err := newFile(fmt.Sprintf("load3-%03d", i), 1_000_000+uint64(i), cfg.InProgressSize)
		files[i] = f
		return err
	}); err != nil {
		t.Fatal(err)
	}

	first := int64(cfg.Chunk)
	const piece = 4 << 10
	pieces := (cfg.InProgressSize - first + piece - 1) / piece
	pace := cfg.Hold / time.Duration(pieces)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Hold+10*time.Minute)
	defer cancel()
	rss := sampleRSS(s.PID)
	results := make([]outcome, n)
	var ready, wg sync.WaitGroup
	ready.Add(n)
	start := time.Now()
	for i, f := range files {
		wg.Go(func() {
			results[i] = heldUpload(ctx, s, f, first, piece, pace, ready.Done)
		})
	}
	ready.Wait()
	t.Logf("%d uploads in progress after %s (first chunk %d B committed; rest at %d B every %s)",
		n, time.Since(start).Round(time.Millisecond), first, piece, pace)

	// Probe in the middle of the hold.
	time.Sleep(cfg.Hold / 3)
	inProgress, gerr := s.gauge(ctx, "upload_in_progress", dbProtocol(cfg.Protocol))
	var hz []time.Duration
	var herr error
	for range 10 {
		d, err := s.healthz(ctx)
		if err != nil {
			herr = err
			break
		}
		hz = append(hz, d)
		time.Sleep(100 * time.Millisecond)
	}
	pctx, pcancel := context.WithTimeout(ctx, probeTimeout)
	pf, err := newFile("load3-probe", 2_000_000, probeSize)
	if err != nil {
		t.Fatal(err)
	}
	probe := uploadAndVerify(pctx, s, pf)
	pcancel()
	midRSS := rss.Peak()

	wg.Wait()
	wall := time.Since(start)
	st := rss.Stop()

	durs := durations(results)
	t.Logf("RESULT NFR-3 protocol=%s in_progress=%d size=%s hold=%s server_gomaxprocs=%d",
		cfg.Protocol, n, mb(cfg.InProgressSize), cfg.Hold, cfg.ServerProcs)
	t.Logf("RESULT NFR-3 server upload_in_progress gauge mid-hold=%v (err %v)", inProgress, gerr)
	t.Logf("RESULT NFR-3 /healthz latency max=%s p50=%s over %d probes (err %v)", pct(hz, 100), pct(hz, 50), len(hz), herr)
	t.Logf("RESULT NFR-3 fresh %s upload during hold: %s (err %v)", mb(probeSize), probe.Duration.Round(time.Millisecond), probe.Err)
	t.Logf("RESULT NFR-3 wall=%s held-upload p50=%s max=%s; peak RSS while held %s", wall.Round(time.Millisecond),
		pct(durs, 50), pct(durs, 100), mb(midRSS))
	t.Logf("RESULT NFR-3 server RSS %s (limit %s)", st, mb(cfg.RSSLimit))

	if failed := failures(results); len(failed) > 0 {
		t.Errorf("%d of %d held uploads failed; first: %s: %v", len(failed), n, failed[0].Device, failed[0].Err)
	}
	if herr != nil {
		t.Errorf("healthz during hold: %v", herr)
	} else if m := pct(hz, 100); m > healthzLimit {
		t.Errorf("healthz max latency %s > %s", m, healthzLimit)
	}
	if probe.Err != nil {
		t.Errorf("fresh upload during hold: %v", probe.Err)
	}
	if st.Peak >= cfg.RSSLimit {
		t.Errorf("peak server RSS %s >= limit %s", mb(st.Peak), mb(cfg.RSSLimit))
	}
	if !s.alive() {
		t.Errorf("server died during the test")
	}
}

// smoke uploads a small file and skips the test when the protocol is not
// mounted on this server build (gRPC before it is wired).
func smoke(t *testing.T, s *server, seed uint64) {
	t.Helper()
	f, err := newFile("load-smoke", seed, smokeSize)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	o := uploadAndVerify(ctx, s, f)
	if errors.Is(o.Err, errUnimplemented) {
		t.Skipf("protocol %s is not served by this build (%v); re-run after it is wired into internal/app", cfg.Protocol, o.Err)
	}
	if o.Err != nil {
		t.Fatalf("smoke upload over %s: %v", cfg.Protocol, o.Err)
	}
}

// uploadAndVerify uploads f in LOAD_CHUNK pieces, checks the DB row is
// completed with the right size and digest, then deletes the stored object
// to bound disk use.
func uploadAndVerify(ctx context.Context, s *server, f file) outcome {
	o := outcome{Device: f.Device}
	start := time.Now()
	u, err := newUploader(cfg.Protocol, s, f)
	if err != nil {
		o.Err = err
		return o
	}
	defer u.close()
	if o.Err = u.create(ctx); o.Err != nil {
		return o
	}
	o.Ref = u.ref()
	if o.Err = u.send(ctx, f.reader(), 0, f.Size, cfg.Chunk, 0); o.Err != nil {
		return o
	}
	o.Duration = time.Since(start)
	o.Err = verify(ctx, s, u.ref(), f)
	return o
}

// heldUpload creates f, sends its first `first` bytes, calls inProgress,
// then sends the rest in pieces paced by pace.
func heldUpload(ctx context.Context, s *server, f file, first int64, piece int, pace time.Duration, inProgress func()) outcome {
	o := outcome{Device: f.Device}
	signal := sync.OnceFunc(inProgress)
	defer signal()
	start := time.Now()
	u, err := newUploader(cfg.Protocol, s, f)
	if err != nil {
		o.Err = err
		return o
	}
	defer u.close()
	if o.Err = u.create(ctx); o.Err != nil {
		return o
	}
	o.Ref = u.ref()
	src := f.reader()
	if o.Err = u.send(ctx, src, 0, first, int(first), 0); o.Err != nil {
		return o
	}
	signal()
	if o.Err = u.send(ctx, src, first, f.Size, piece, pace); o.Err != nil {
		return o
	}
	o.Duration = time.Since(start)
	o.Err = verify(ctx, s, u.ref(), f)
	return o
}

// verify checks the row and removes the stored object.
func verify(ctx context.Context, s *server, ref string, f file) error {
	row, err := s.Repo.Get(ctx, dbProtocol(cfg.Protocol), ref)
	if err != nil {
		return fmt.Errorf("load row %s: %w", ref, err)
	}
	if row.Status != db.StatusCompleted || row.Size != f.Size || row.SHA256 != f.SHA256 || row.StorageKey == "" {
		return fmt.Errorf("row %s: status %s size %d sha %s key %q, want completed %d %s",
			ref, row.Status, row.Size, row.SHA256, row.StorageKey, f.Size, f.SHA256)
	}
	obj := filepath.Join(s.ObjDir, filepath.FromSlash(row.StorageKey))
	fi, err := os.Stat(obj)
	if err != nil {
		return fmt.Errorf("stored object: %w", err)
	}
	if fi.Size() != f.Size {
		return fmt.Errorf("stored object %s: %d bytes, want %d", row.StorageKey, fi.Size(), f.Size)
	}
	if err := os.Remove(obj); err != nil {
		return fmt.Errorf("remove stored object: %w", err)
	}
	return nil
}

// forEach runs fn(0..n-1) on at most par goroutines and returns the first
// error.
func forEach(n, par int, fn func(i int) error) error {
	errs := make([]error, n)
	sem := make(chan struct{}, par)
	var wg sync.WaitGroup
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			errs[i] = fn(i)
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

func failures(rs []outcome) []outcome {
	var out []outcome
	for _, r := range rs {
		if r.Err != nil {
			out = append(out, r)
		}
	}
	return out
}

func durations(rs []outcome) []time.Duration {
	var out []time.Duration
	for _, r := range rs {
		if r.Err == nil {
			out = append(out, r.Duration)
		}
	}
	return out
}

// pct returns the p-th percentile (nearest rank) of ds, 0 if empty.
func pct(ds []time.Duration, p int) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Sorted(slices.Values(ds))
	i := max(0, (len(s)*p+99)/100-1)
	return s[i].Round(time.Millisecond)
}
