//go:build integration

// Package e2e runs the whole server in-process against real Postgres (and
// MinIO) containers, with real TLS/QUIC listeners on random ports.
package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/quic-go/quic-go/http3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/resyahrial/sensor-upload-server/internal/app"
	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/config"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/objstore"
)

const postgresImage = "docker.io/library/postgres:16-alpine"

// Shared across tests (set up once in TestMain).
var (
	testDSN string
	keys    *db.DeviceKeys // device_keys of the shared database
	certPEM []byte // self-signed server certificate
	keyPEM  []byte
)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("uploads"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintln(os.Stderr, "terminate postgres:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	if testDSN, err = ctr.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintln(os.Stderr, "postgres dsn:", err)
		return 1
	}
	pool, err := db.Open(ctx, testDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open database:", err)
		return 1
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	keys = db.NewDeviceKeys(pool)
	if certPEM, keyPEM, err = selfSignedCert(); err != nil {
		fmt.Fprintln(os.Stderr, "certificate:", err)
		return 1
	}
	return m.Run()
}

func selfSignedCert() (certOut, keyOut []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}), nil
}

// FaultStore wraps an object store and fails the next N Puts.
type FaultStore struct {
	objstore.Store
	failPuts atomic.Int32
}

// FailPuts makes the next n Put calls fail.
func (f *FaultStore) FailPuts(n int) { f.failPuts.Store(int32(n)) }

// Put fails while failures are pending, else delegates.
func (f *FaultStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if f.failPuts.Add(-1) >= 0 {
		return errors.New("injected object store failure")
	}
	f.failPuts.Store(0)
	return f.Store.Put(ctx, key, r, size)
}

// Server is a restartable in-process upload server. Env overrides are
// applied on top of the defaults (local object store, file staging,
// device keys in the shared database). The port stays the same across restarts, so upload
// URLs remain valid.
type Server struct {
	t     *testing.T
	env   map[string]string
	Dir   string      // root of staging/objects/certs
	Store *FaultStore // nil when OBJSTORE=s3
	Repo  *db.Repo
	pool  *pgxpool.Pool

	mu     sync.Mutex
	App    *app.App // current instance
	port   string
	native string // native gRPC port; empty when disabled
	cancel context.CancelFunc
	done   chan error
}

// StartServer starts a server; it is stopped when the test ends.
func StartServer(t *testing.T, env map[string]string) *Server {
	t.Helper()
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	base := map[string]string{
		"LISTEN_ADDR":        "127.0.0.1:0",
		"GRPC_NATIVE_ADDR":   "127.0.0.1:0",
		"METRICS_ADDR":       "127.0.0.1:0",
		"TLS_CERT_FILE":      certFile,
		"TLS_KEY_FILE":       keyFile,
		"DATABASE_URL":       testDSN,
		"STAGING_DIR":        filepath.Join(dir, "staging"),
		"OBJSTORE_LOCAL_DIR": filepath.Join(dir, "objects"),
		"LOG_LEVEL":          "warn",
		"MAX_FILE_BYTES":     strconv.Itoa(64 << 20),
		"SHUTDOWN_TIMEOUT":   "3s",
		// The temp dir is on the developer's disk, which may well be more
		// than 85% full: keep disk-pressure retention from deleting the
		// tests' completed uploads (the tests share one database).
		"DISK_HIGH_WATERMARK": "100",
		"DISK_LOW_WATERMARK":  "99",
	}
	for k, v := range env {
		base[k] = v
	}
	s := &Server{t: t, env: base, Dir: dir}
	if base["OBJSTORE"] != config.BackendS3 {
		local, err := objstore.NewLocal(base["OBJSTORE_LOCAL_DIR"])
		if err != nil {
			t.Fatal(err)
		}
		s.Store = &FaultStore{Store: local}
	}
	pool, err := db.Open(context.Background(), testDSN)
	if err != nil {
		t.Fatal(err)
	}
	s.pool, s.Repo = pool, db.NewRepo(pool)
	s.start()
	t.Cleanup(func() {
		s.Stop()
		pool.Close()
	})
	return s
}

func (s *Server) start() {
	s.t.Helper()
	env := s.env
	if s.port != "" {
		env["LISTEN_ADDR"] = "127.0.0.1:" + s.port
	}
	if s.native != "" {
		env["GRPC_NATIVE_ADDR"] = "127.0.0.1:" + s.native
	}
	cfg, err := config.Load(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		s.t.Fatal(err)
	}
	opts := app.Options{Config: cfg, Log: slog.New(slog.NewJSONHandler(tWriter{s.t}, &slog.HandlerOptions{Level: cfg.LogLevel}))}
	if s.Store != nil {
		opts.ObjectStore = s.Store
	}
	ctx, cancel := context.WithCancel(context.Background())
	a, err := app.New(ctx, opts)
	if err != nil {
		cancel()
		s.t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	s.mu.Lock()
	s.port = strconv.Itoa(a.Addr().(*net.TCPAddr).Port)
	if addr := a.GRPCNativeAddr(); addr != nil {
		s.native = strconv.Itoa(addr.(*net.TCPAddr).Port)
	}
	s.cancel, s.done, s.App = cancel, done, a
	s.mu.Unlock()
}

// Stop shuts the server down gracefully (like SIGTERM) and waits.
func (s *Server) Stop() {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done = nil, nil
	s.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	if err := <-done; err != nil {
		s.t.Logf("server stopped with: %v", err)
	}
}

// Restart stops the server and starts it again on the same port with the
// same directories.
func (s *Server) Restart() {
	s.t.Helper()
	s.Stop()
	s.start()
}

// URL returns the base URL, e.g. https://127.0.0.1:port.
func (s *Server) URL() string { return "https://127.0.0.1:" + s.port }

// NativeAddr returns the native gRPC listener's host:port (empty when
// GRPC_NATIVE_ADDR is disabled).
func (s *Server) NativeAddr() string {
	if s.native == "" {
		return ""
	}
	return "127.0.0.1:" + s.native
}

// GRPCDir is the gRPC staging directory.
func (s *Server) GRPCDir() string { return filepath.Join(s.env["STAGING_DIR"], "grpc") }

// TusDir is the tus filestore directory.
func (s *Server) TusDir() string { return filepath.Join(s.env["STAGING_DIR"], "tus") }

var (
	tokensMu sync.Mutex
	tokens   = map[string]string{} // device -> token issued by Token
)

// Token returns a valid device token: the first call for a device stores a
// new key in device_keys (like `devicekey set`), later calls return the
// same token so that every client of one device stays authenticated.
func Token(t *testing.T, device string) string {
	t.Helper()
	tokensMu.Lock()
	defer tokensMu.Unlock()
	if tok, ok := tokens[device]; ok {
		return tok
	}
	return setKeyLocked(t, device)
}

// SetDeviceKey stores a new key for device, replacing (and invalidating)
// any previous one, and returns its token; Token returns it from now on.
func SetDeviceKey(t *testing.T, device string) string {
	t.Helper()
	tokensMu.Lock()
	defer tokensMu.Unlock()
	return setKeyLocked(t, device)
}

func setKeyLocked(t *testing.T, device string) string {
	t.Helper()
	key, err := auth.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := keys.Set(ctx, device, auth.HashKey(key)); err != nil {
		t.Fatal(err)
	}
	tok := auth.FormatToken(device, key)
	tokens[device] = tok
	return tok
}

// TLSConfig trusts the test certificate.
func TLSConfig() *tls.Config {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

// Transport names.
const (
	H1 = "h1"
	H2 = "h2"
	H3 = "h3"
)

// NewClient returns an HTTP client speaking exactly the given protocol.
func NewClient(t *testing.T, proto string) *http.Client {
	t.Helper()
	var rt http.RoundTripper
	switch proto {
	case H3:
		tr := &http3.Transport{TLSClientConfig: TLSConfig()}
		t.Cleanup(func() { _ = tr.Close() })
		rt = tr
	default:
		tr := &http.Transport{TLSClientConfig: TLSConfig(), ForceAttemptHTTP2: proto == H2}
		if proto == H1 {
			tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		}
		t.Cleanup(tr.CloseIdleConnections)
		rt = tr
	}
	return &http.Client{Transport: rt, Timeout: 60 * time.Second}
}

// tWriter sends server logs to the test log.
type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
