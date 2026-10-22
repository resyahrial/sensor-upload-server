//go:build load

// Package load runs the NFR-2 (memory) and NFR-3 (concurrency) load tests.
//
// The real server binary (cmd/server) is built and started as a subprocess
// against a Postgres testcontainer and the local object store, so the
// measured RSS is the server's alone (ps -o rss=, sampled every 250 ms).
// Upload content is generated on the fly from a seeded ChaCha8 stream on
// the client side, so no client-side files are written. Run it with
// `make load-test`; the knobs are the LOAD_* environment variables (see
// loadConfigFromEnv).
package load

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const postgresImage = "docker.io/library/postgres:16-alpine"

// Set up once in TestMain.
var (
	cfg       loadConfig
	testDSN   string
	serverBin string
	certFile  string
	keyFile   string
	caPEM     []byte
)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	var err error
	if cfg, err = loadConfigFromEnv(os.LookupEnv); err != nil {
		fmt.Fprintln(os.Stderr, "load config:", err)
		return 2
	}
	fmt.Fprintf(os.Stderr, "load config: %+v\n", cfg)

	base, err := os.MkdirTemp(cfg.Dir, "upload-load-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "temp dir:", err)
		return 1
	}
	defer os.RemoveAll(base)

	if serverBin, err = buildServer(base); err != nil {
		fmt.Fprintln(os.Stderr, "build server:", err)
		return 1
	}
	if certFile, keyFile, err = writeCert(base); err != nil {
		fmt.Fprintln(os.Stderr, "certificate:", err)
		return 1
	}

	ctx := context.Background()
	ctr, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("uploads"),
		postgres.WithUsername("load"),
		postgres.WithPassword("load"),
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
	return m.Run()
}

// loadConfig holds the LOAD_* knobs.
type loadConfig struct {
	Protocol string // rest | rest-http3 | grpc | grpc-native
	Dir      string // parent of all temp dirs (server staging/objects)

	// NFR-2
	Files    int
	FileSize int64
	Chunk    int
	RSSLimit int64 // bytes

	// NFR-3
	InProgress     int
	InProgressSize int64
	Hold           time.Duration

	ServerProcs int // GOMAXPROCS of the server (2 = the NFR-3 2 vCPU target)
}

// loadConfigFromEnv reads:
//
//	LOAD_PROTOCOL         rest | rest-http3 | grpc | grpc-native (rest)
//	LOAD_DIR              temp dir parent                 (os.TempDir())
//	LOAD_FILES            NFR-2 concurrent uploads        (50)
//	LOAD_FILE_SIZE        NFR-2 file size                 (200MiB)
//	LOAD_CHUNK            chunk size                      (256KiB)
//	LOAD_RSS_LIMIT        peak server RSS limit           (300MB)
//	LOAD_INPROGRESS       NFR-3 concurrent uploads        (200)
//	LOAD_INPROGRESS_SIZE  NFR-3 file size                 (1MiB)
//	LOAD_HOLD             NFR-3 time each upload stays in progress (30s)
//	LOAD_SERVER_PROCS     server GOMAXPROCS               (2)
func loadConfigFromEnv(lookup func(string) (string, bool)) (loadConfig, error) {
	c := loadConfig{
		Protocol: "rest", Dir: os.TempDir(),
		Files: 50, FileSize: 200 << 20, Chunk: 256 << 10, RSSLimit: 300 * 1000 * 1000,
		InProgress: 200, InProgressSize: 1 << 20, Hold: 30 * time.Second,
		ServerProcs: 2,
	}
	get := func(k string) (string, bool) {
		v, ok := lookup(k)
		v = strings.TrimSpace(v)
		return v, ok && v != ""
	}
	var errs []string
	fail := func(k string, err error) { errs = append(errs, fmt.Sprintf("%s: %v", k, err)) }
	str := func(k string, dst *string) {
		if v, ok := get(k); ok {
			*dst = v
		}
	}
	num := func(k string, dst *int) {
		if v, ok := get(k); ok {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				fail(k, fmt.Errorf("%q is not a positive integer", v))
				return
			}
			*dst = n
		}
	}
	size := func(k string, dst *int64) {
		if v, ok := get(k); ok {
			n, err := parseSize(v)
			if err != nil || n <= 0 {
				fail(k, fmt.Errorf("%q is not a positive size (e.g. 200MiB)", v))
				return
			}
			*dst = n
		}
	}
	str("LOAD_PROTOCOL", &c.Protocol)
	str("LOAD_DIR", &c.Dir)
	num("LOAD_FILES", &c.Files)
	size("LOAD_FILE_SIZE", &c.FileSize)
	var chunk = int64(c.Chunk)
	size("LOAD_CHUNK", &chunk)
	c.Chunk = int(chunk)
	size("LOAD_RSS_LIMIT", &c.RSSLimit)
	num("LOAD_INPROGRESS", &c.InProgress)
	size("LOAD_INPROGRESS_SIZE", &c.InProgressSize)
	num("LOAD_SERVER_PROCS", &c.ServerProcs)
	if v, ok := get("LOAD_HOLD"); ok {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			fail("LOAD_HOLD", fmt.Errorf("%q is not a positive duration", v))
		} else {
			c.Hold = d
		}
	}
	switch c.Protocol {
	case protoREST, protoRESTHTTP3, protoGRPC, protoGRPCNative:
	default:
		fail("LOAD_PROTOCOL", fmt.Errorf("%q is not rest, rest-http3, grpc or grpc-native", c.Protocol))
	}
	if c.InProgressSize <= int64(c.Chunk) {
		fail("LOAD_INPROGRESS_SIZE", fmt.Errorf("must be larger than LOAD_CHUNK (%d)", c.Chunk))
	}
	if len(errs) > 0 {
		return loadConfig{}, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return c, nil
}

// parseSize accepts a byte count with an optional KB/MB/GB or KiB/MiB/GiB
// suffix.
func parseSize(s string) (int64, error) {
	units := []struct {
		suffix string
		mul    int64
	}{
		{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1},
	}
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
			return n * u.mul, err
		}
	}
	return strconv.ParseInt(s, 10, 64)
}

// buildServer compiles cmd/server into dir, as `make build` does.
func buildServer(dir string) (string, error) {
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMOD: %w", err)
	}
	root := filepath.Dir(strings.TrimSpace(string(out)))
	bin := filepath.Join(dir, "server")
	cmd := exec.Command("go", "build", "-trimpath", "-o", bin, "./cmd/server")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("go build ./cmd/server: %w\n%s", err, out)
	}
	return bin, nil
}

// writeCert writes a self-signed localhost certificate and key into dir.
func writeCert(dir string) (certPath, keyPath string, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
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
		return "", "", err
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", "", err
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	certPath, keyPath = filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	if err := os.WriteFile(certPath, caPEM, 0o600); err != nil {
		return "", "", err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

// workers bounds CPU-heavy client work (content hashing).
func workers() int { return max(1, runtime.NumCPU()-1) }
