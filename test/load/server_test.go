//go:build load

package load

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

const (
	rssInterval  = 250 * time.Millisecond
	readyTimeout = 90 * time.Second
	stopTimeout  = 30 * time.Second
)

// server is the cmd/server binary running as a subprocess.
type server struct {
	URL    string // https://127.0.0.1:port (TCP and UDP)
	Admin  string // http://127.0.0.1:port (metrics, health)
	Native string // 127.0.0.1:port of the native gRPC listener
	ObjDir string
	Repo   *db.Repo
	PID    int

	cmd  *exec.Cmd
	done chan struct{}
	err  error // set before done is closed
	log  string
}

// startServer starts a fresh server with its own staging and object
// directories under a temp dir; it is stopped when the test ends.
func startServer(t *testing.T) *server {
	t.Helper()
	dir, err := os.MkdirTemp(cfg.Dir, "upload-load-srv-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	adminPort, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	nativePort, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	s := &server{
		URL:    "https://127.0.0.1:" + port,
		Admin:  "http://127.0.0.1:" + adminPort,
		Native: "127.0.0.1:" + nativePort,
		ObjDir: filepath.Join(dir, "objects"),
		done:   make(chan struct{}),
		log:    filepath.Join(dir, "server.log"),
	}
	logf, err := os.Create(s.log)
	if err != nil {
		t.Fatal(err)
	}
	defer logf.Close() // the child keeps its own descriptor

	env := map[string]string{
		"LISTEN_ADDR":        "127.0.0.1:" + port,
		"METRICS_ADDR":       "127.0.0.1:" + adminPort,
		"GRPC_NATIVE_ADDR":   "127.0.0.1:" + nativePort,
		"TLS_CERT_FILE":      certFile,
		"TLS_KEY_FILE":       keyFile,
		"DATABASE_URL":       testDSN,
		"STAGING_DIR":        filepath.Join(dir, "staging"),
		"OBJSTORE_LOCAL_DIR": s.ObjDir,
		"DEV_MODE":           "true",
		"LOG_LEVEL":          "warn",
		"JWT_ALGS":           "HS256",
		"JWT_HMAC_SECRET":    hmacSecret,
		"MAX_FILE_BYTES":     strconv.FormatInt(max(cfg.FileSize, 500<<20), 10),
		"SHUTDOWN_TIMEOUT":   "10s",
		"GOMAXPROCS":         strconv.Itoa(cfg.ServerProcs),
		"HOME":               dir,
		"PATH":               os.Getenv("PATH"),
	}
	s.cmd = exec.Command(serverBin)
	for k, v := range env {
		s.cmd.Env = append(s.cmd.Env, k+"="+v)
	}
	s.cmd.Stdout, s.cmd.Stderr = logf, logf
	if err := s.cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	s.PID = s.cmd.Process.Pid
	go func() {
		s.err = s.cmd.Wait()
		close(s.done)
	}()
	t.Cleanup(func() { s.stop(t) })

	if err := s.waitReady(); err != nil {
		t.Fatalf("server not ready: %v\n%s", err, s.tail(40))
	}
	pool, err := pgxpool.New(context.Background(), testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s.Repo = db.NewRepo(pool)
	return s
}

func (s *server) waitReady() error {
	deadline := time.Now().Add(readyTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-s.done:
			return fmt.Errorf("server exited: %v", s.err)
		default:
		}
		res, err := http.Get(s.Admin + "/readyz")
		if err == nil {
			_ = res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("timed out waiting for /readyz")
}

// alive reports whether the process is still running.
func (s *server) alive() bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

// stop sends SIGTERM and waits (SIGKILL after stopTimeout).
func (s *server) stop(t *testing.T) {
	if !s.alive() {
		if s.err != nil {
			t.Errorf("server exited early: %v\n%s", s.err, s.tail(40))
		}
		return
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.done:
	case <-time.After(stopTimeout):
		_ = s.cmd.Process.Kill()
		<-s.done
		t.Errorf("server did not stop within %s", stopTimeout)
	}
	if lines := s.tail(20); lines != "" {
		t.Logf("server log tail (warn and above):\n%s", lines)
	}
}

// tail returns the last n lines of the server log.
func (s *server) tail(n int) string {
	f, err := os.Open(s.log)
	if err != nil {
		return ""
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		lines = append(lines, sc.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}

// healthz returns the latency of GET /healthz.
func (s *server) healthz(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Admin+"/healthz", nil)
	if err != nil {
		return 0, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body)
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("healthz: status %d", res.StatusCode)
	}
	return time.Since(start), nil
}

// gauge reads a metric value with the given protocol label from /metrics.
func (s *server) gauge(ctx context.Context, name, protocol string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Admin+"/metrics", nil)
	if err != nil {
		return 0, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	sc := bufio.NewScanner(res.Body)
	label := `protocol="` + protocol + `"`
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name+"{") || !strings.Contains(line, label) {
			continue
		}
		f := strings.Fields(line)
		return strconv.ParseFloat(f[len(f)-1], 64)
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("metric %s{%s} not found", name, label)
}

// freePort returns a port free on 127.0.0.1 for both TCP and UDP (the
// server binds HTTP/3 on the TCP port).
func freePort() (string, error) {
	for range 20 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return "", err
		}
		port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
		u, err := net.ListenPacket("udp", "127.0.0.1:"+port)
		_ = l.Close()
		if err == nil {
			_ = u.Close()
			return port, nil
		}
	}
	return "", errors.New("no free TCP+UDP port")
}

// rssSampler samples a process's resident set size.
type rssSampler struct {
	pid  int
	stop chan struct{}
	done chan struct{}

	mu      sync.Mutex
	samples []int64 // bytes
	errs    int
}

// rssStats summarizes the samples.
type rssStats struct {
	Peak, Median, First, Last int64 // bytes
	Samples, Errors           int
}

func (r rssStats) String() string {
	return fmt.Sprintf("peak %s, median %s, first %s, last %s (%d samples every %s, %d errors)",
		mb(r.Peak), mb(r.Median), mb(r.First), mb(r.Last), r.Samples, rssInterval, r.Errors)
}

// sampleRSS samples pid every rssInterval until Stop.
func sampleRSS(pid int) *rssSampler {
	r := &rssSampler{pid: pid, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		tick := time.NewTicker(rssInterval)
		defer tick.Stop()
		for {
			r.sample()
			select {
			case <-r.stop:
				return
			case <-tick.C:
			}
		}
	}()
	return r
}

func (r *rssSampler) sample() {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(r.pid)).Output()
	kib, perr := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil || perr != nil {
		r.errs++
		return
	}
	r.samples = append(r.samples, kib<<10)
}

// Peak returns the highest sample so far.
func (r *rssSampler) Peak() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Max(append([]int64{0}, r.samples...))
}

// Stop ends sampling and returns the statistics.
func (r *rssSampler) Stop() rssStats {
	close(r.stop)
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	st := rssStats{Samples: len(r.samples), Errors: r.errs}
	if len(r.samples) == 0 {
		return st
	}
	st.First, st.Last = r.samples[0], r.samples[len(r.samples)-1]
	sorted := slices.Sorted(slices.Values(r.samples))
	st.Peak, st.Median = sorted[len(sorted)-1], sorted[len(sorted)/2]
	return st
}

// mb formats bytes as decimal megabytes (the NFR-2 unit).
func mb(b int64) string { return fmt.Sprintf("%.1f MB", float64(b)/1e6) }

// freeBytes returns the space available to unprivileged users on dir's
// filesystem.
func freeBytes(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}
