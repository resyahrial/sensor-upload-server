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
// Device tokens, one of:
//
//	REMOTE_HMAC_SECRET + REMOTE_DEVICE   mint HS256 tokens (dev servers with JWT_ALGS incl. HS256)
//	REMOTE_JWT_KEY + REMOTE_DEVICE       mint ES256 tokens with this PEM P-256 private key
//	REMOTE_TOKEN [+ REMOTE_DEVICE]       a fixed device JWT (device defaults to its device_id claim)
//
// The cross-device test needs a second device: minted ("<device>-other")
// with a secret/key, or REMOTE_OTHER_TOKEN for a fixed token. Every run
// uploads fresh random content, so reruns never collide.
package remote

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

const tokenTTL = time.Hour

// env is the parsed configuration.
type env struct {
	URL        string
	GRPCNative string
	AdminURL   string
	TLS        *tls.Config

	Device      string
	OtherDevice string // "" = cross-device test skips
	token       string
	otherToken  string
	mint        func(device string) (string, error) // nil with a fixed token
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
		Device:     getenv("REMOTE_DEVICE"),
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

	secret, keyFile, token := getenv("REMOTE_HMAC_SECRET"), getenv("REMOTE_JWT_KEY"), getenv("REMOTE_TOKEN")
	switch {
	case secret != "":
		e.mint = func(d string) (string, error) {
			return auth.Sign(auth.HS256, []byte(secret), d, time.Now(), tokenTTL)
		}
	case keyFile != "":
		pem, err := os.ReadFile(keyFile)
		if err != nil {
			return nil, fmt.Errorf("REMOTE_JWT_KEY: %w", err)
		}
		key, err := auth.ParseES256PrivateKey(pem)
		if err != nil {
			return nil, fmt.Errorf("REMOTE_JWT_KEY: %w", err)
		}
		e.mint = func(d string) (string, error) { return auth.Sign(auth.ES256, key, d, time.Now(), tokenTTL) }
	case token != "":
		e.token = token
		if e.Device == "" {
			if e.Device, err = deviceClaim(token); err != nil {
				return nil, fmt.Errorf("REMOTE_TOKEN: %w (set REMOTE_DEVICE)", err)
			}
		}
		if other := getenv("REMOTE_OTHER_TOKEN"); other != "" {
			e.otherToken = other
			if e.OtherDevice, err = deviceClaim(other); err != nil {
				return nil, fmt.Errorf("REMOTE_OTHER_TOKEN: %w", err)
			}
		}
	default:
		return nil, errors.New("set REMOTE_HMAC_SECRET, REMOTE_JWT_KEY or REMOTE_TOKEN")
	}
	if e.mint != nil {
		if e.Device == "" {
			return nil, errors.New("REMOTE_DEVICE is required with REMOTE_HMAC_SECRET or REMOTE_JWT_KEY")
		}
		e.OtherDevice = e.Device + "-other"
	}
	return e, nil
}

// Token returns a device token for e.Device.
func (e *env) Token(t *testing.T) string {
	t.Helper()
	return e.tokenFor(t, e.Device, e.token)
}

// OtherToken returns a token for e.OtherDevice, skipping the test if there
// is none.
func (e *env) OtherToken(t *testing.T) string {
	t.Helper()
	if e.OtherDevice == "" {
		t.Skip("no second device: set REMOTE_OTHER_TOKEN, or use REMOTE_HMAC_SECRET / REMOTE_JWT_KEY")
	}
	return e.tokenFor(t, e.OtherDevice, e.otherToken)
}

func (e *env) tokenFor(t *testing.T, device, fixed string) string {
	t.Helper()
	if e.mint == nil {
		return fixed
	}
	tok, err := e.mint(device)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// deviceClaim reads the device_id claim of a JWT without verifying it (the
// server does that).
func deviceClaim(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("JWT payload: %w", err)
	}
	var c struct {
		DeviceID string `json:"device_id"`
	}
	if err := json.Unmarshal(payload, &c); err != nil || c.DeviceID == "" {
		return "", errors.New("JWT has no device_id claim")
	}
	return c.DeviceID, nil
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
