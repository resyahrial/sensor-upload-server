//go:build remote

// Package remote is a manually run end-to-end suite against an already
// running server (e.g. a VPS). It never runs in `make test` or
// `make test-integration` (build tag remote). Run it with
// `make e2e-remote` (all) or `make e2e-remote RUN=TestRemoteUploadREST`
// (one); `make e2e-remote-list` lists the tests.
//
// Configuration (environment):
//
//	REMOTE_URL           https://host:port of the TLS listener (required; tests skip without it)
//	REMOTE_GRPC_NATIVE   host:port of the native gRPC listener (optional; native tests skip)
//	REMOTE_ADMIN_URL     http://host:port of the admin listener (optional; /healthz, /readyz)
//	REMOTE_CA_FILE       PEM CA or self-signed server certificate to trust
//	REMOTE_INSECURE=1    skip TLS verification instead (fallback)
//
// Access tokens (ES256 JWTs, e.g. from `make vps-device-token DEVICE=ID`,
// README "Authentication"):
//
//	REMOTE_TOKEN         access token of the test device (required)
//	REMOTE_OTHER_TOKEN   access token of a second device (optional; the
//	                     cross-device subtest skips without it)
//
// The device is the token's sub claim. Access tokens are short-lived
// (ACCESS_TOKEN_TTL, 1 h by default): mint them right before a run. Every
// run uploads fresh random content, so reruns never collide.
package remote

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

// env is the parsed configuration.
type env struct {
	URL        string
	GRPCNative string
	AdminURL   string
	TLS        *tls.Config

	Device      string
	OtherDevice string // "" = cross-device subtest skips
	token       string
	otherToken  string
}

var (
	envOnce   sync.Once
	envCached *env
	envErr    error
)

// remoteEnv returns the configuration, skipping the test when REMOTE_URL is
// unset and failing it when the configuration is invalid.
func remoteEnv(t *testing.T) *env {
	t.Helper()
	envOnce.Do(func() { envCached, envErr = loadEnv(os.Getenv) })
	if errors.Is(envErr, errNoURL) {
		t.Skip("REMOTE_URL not set (see test/remote/env_test.go)")
	}
	if envErr != nil {
		t.Fatal(envErr)
	}
	return envCached
}

var errNoURL = errors.New("REMOTE_URL not set")

func loadEnv(getenv func(string) string) (*env, error) {
	e := &env{
		URL:        strings.TrimRight(getenv("REMOTE_URL"), "/"),
		GRPCNative: getenv("REMOTE_GRPC_NATIVE"),
		AdminURL:   strings.TrimRight(getenv("REMOTE_ADMIN_URL"), "/"),
	}
	if e.URL == "" {
		return nil, errNoURL
	}
	if !strings.HasPrefix(e.URL, "https://") {
		return nil, fmt.Errorf("REMOTE_URL %q must start with https://", e.URL)
	}
	var err error
	if e.TLS, err = tlsConfig(getenv("REMOTE_CA_FILE"), getenv("REMOTE_INSECURE") == "1"); err != nil {
		return nil, err
	}
	if e.token = getenv("REMOTE_TOKEN"); e.token == "" {
		return nil, errors.New("REMOTE_TOKEN is required (make vps-device-token DEVICE=...)")
	}
	if e.Device, err = auth.DeviceFromToken(e.token); err != nil {
		return nil, fmt.Errorf("REMOTE_TOKEN: %w", err)
	}
	if e.otherToken = getenv("REMOTE_OTHER_TOKEN"); e.otherToken != "" {
		if e.OtherDevice, err = auth.DeviceFromToken(e.otherToken); err != nil {
			return nil, fmt.Errorf("REMOTE_OTHER_TOKEN: %w", err)
		}
		if e.OtherDevice == e.Device {
			return nil, errors.New("REMOTE_OTHER_TOKEN must belong to another device than REMOTE_TOKEN")
		}
	}
	return e, nil
}

// Token returns the access token for e.Device.
func (e *env) Token(t *testing.T) string {
	t.Helper()
	return e.token
}

// OtherToken returns the access token for e.OtherDevice, skipping the test if there
// is none.
func (e *env) OtherToken(t *testing.T) string {
	t.Helper()
	if e.OtherDevice == "" {
		t.Skip("no second device: set REMOTE_OTHER_TOKEN")
	}
	return e.otherToken
}

func tlsConfig(caFile string, insecure bool) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure} //nolint:gosec // opt-in REMOTE_INSECURE
	if caFile == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("REMOTE_CA_FILE: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("REMOTE_CA_FILE: no certificates in %s", caFile)
	}
	cfg.RootCAs = pool
	return cfg, nil
}
