package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

// RefreshPath is the token refresh endpoint, relative to the server base URL.
const RefreshPath = "/v1/devices/token/refresh"

// refreshAt is the fraction of expires_in after which the access token is
// refreshed proactively.
const refreshAt = 0.8

// defaultRefreshTimeout bounds one refresh request.
const defaultRefreshTimeout = 30 * time.Second

// TokenSource supplies access tokens to the transports. Implementations are
// safe for concurrent use.
type TokenSource interface {
	// Token returns the access token to send now.
	Token(ctx context.Context) (string, error)
	// Refresh is called after the server rejected used as token_expired.
	// It returns nil once a newer token is available (possibly obtained by a
	// concurrent caller), or an error (ErrPermanent if refreshing can never
	// succeed).
	Refresh(ctx context.Context, used string) error
}

// StaticToken returns a TokenSource that always sends token. An expired
// static token ends the run with a permanent error.
func StaticToken(token string) TokenSource { return staticToken(token) }

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }

func (staticToken) Refresh(context.Context, string) error {
	return fmt.Errorf("%w: %w: a static -token cannot be refreshed; mint a new one (devicectl access-token) or use -refresh-token-file",
		ErrPermanent, ErrTokenExpired)
}

// withToken runs fn with the current access token. If fn fails with
// ErrTokenExpired, the token is refreshed once and fn runs once more.
func withToken(ctx context.Context, src TokenSource, fn func(token string) error) error {
	tok, err := src.Token(ctx)
	if err != nil {
		return err
	}
	err = fn(tok)
	if !errors.Is(err, ErrTokenExpired) {
		return err
	}
	if rerr := src.Refresh(ctx, tok); rerr != nil {
		return fmt.Errorf("%w; refresh: %w", err, rerr)
	}
	if tok, err = src.Token(ctx); err != nil {
		return err
	}
	return fn(tok)
}

// TokenPair is the refresh endpoint's response and the content of a
// -refresh-token-file (`devicectl issue` prints the same shape). Only
// RefreshToken is required in a file.
type TokenPair struct {
	DeviceID         string `json:"device_id,omitempty"`
	AccessToken      string `json:"access_token,omitempty"`
	TokenType        string `json:"token_type,omitempty"`
	ExpiresIn        int64  `json:"expires_in,omitempty"` // seconds
	RefreshToken     string `json:"refresh_token"`
	RefreshExpiresAt string `json:"refresh_expires_at,omitempty"`
}

// RefreshConfig configures a RefreshingSource.
type RefreshConfig struct {
	// File holds the TokenPair JSON. It is rewritten atomically (mode 0600)
	// after every refresh, before the new tokens are used.
	File string
	// URL is the server base URL; refreshes POST to URL + RefreshPath.
	URL string
	// TLS configures the default HTTPS client.
	TLS *tls.Config
	// HTTPClient overrides the HTTPS client (tests).
	HTTPClient *http.Client
	// Timeout bounds one refresh request (default 30 s).
	Timeout time.Duration
	// Logger receives refresh logs; nil discards them.
	Logger *slog.Logger
	// Now overrides the clock (tests); defaults to time.Now.
	Now func() time.Time
}

// RefreshingSource keeps an access token fresh with a rotating refresh
// token: proactively once refreshAt of expires_in has elapsed since the
// token was received (local clock, not the JWT exp), and on demand after a
// token_expired rejection. Concurrent refreshes share one request.
type RefreshingSource struct {
	file    string
	url     string
	hc      *http.Client
	timeout time.Duration
	log     *slog.Logger
	now     func() time.Time

	mu       sync.Mutex
	pair     TokenPair
	device   string
	received time.Time // when pair.AccessToken was received
	inflight *refreshCall
}

type refreshCall struct {
	done chan struct{}
	err  error
}

// NewRefreshingSource loads cfg.File. The file's modification time stands
// in for the receipt time of its access token.
func NewRefreshingSource(cfg RefreshConfig) (*RefreshingSource, error) {
	if cfg.File == "" || cfg.URL == "" {
		return nil, errors.New("refresh token file and url are required")
	}
	b, err := os.ReadFile(cfg.File)
	if err != nil {
		return nil, fmt.Errorf("read refresh token file: %w", err)
	}
	var pair TokenPair
	if err := json.Unmarshal(b, &pair); err != nil {
		return nil, fmt.Errorf("parse refresh token file %s: %w", cfg.File, err)
	}
	if pair.RefreshToken == "" {
		return nil, fmt.Errorf("refresh token file %s has no refresh_token", cfg.File)
	}
	device := pair.DeviceID
	if device == "" && pair.AccessToken != "" {
		device, _ = auth.DeviceFromToken(pair.AccessToken)
	}
	if device == "" {
		return nil, fmt.Errorf("refresh token file %s has no device_id", cfg.File)
	}
	st, err := os.Stat(cfg.File)
	if err != nil {
		return nil, fmt.Errorf("stat refresh token file: %w", err)
	}
	base := cfg.URL
	if !strings.Contains(base, "://") {
		base = "https://" + base
	}
	s := &RefreshingSource{
		file:     cfg.File,
		url:      strings.TrimRight(base, "/") + RefreshPath,
		hc:       cfg.HTTPClient,
		timeout:  cfg.Timeout,
		log:      cfg.Logger,
		now:      cfg.Now,
		pair:     pair,
		device:   device,
		received: st.ModTime(),
	}
	if s.hc == nil {
		s.hc = refreshHTTPClient(cfg.TLS)
	}
	if s.timeout <= 0 {
		s.timeout = defaultRefreshTimeout
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

func refreshHTTPClient(cfg *tls.Config) *http.Client {
	if cfg != nil {
		cfg = cfg.Clone()
	} else {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return &http.Client{Transport: &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:     cfg,
		TLSHandshakeTimeout: 15 * time.Second,
		ForceAttemptHTTP2:   true,
		IdleConnTimeout:     90 * time.Second,
	}}
}

// Device returns the device_id the tokens belong to.
func (s *RefreshingSource) Device() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.device
}

// Close releases idle connections of the refresh client.
func (s *RefreshingSource) Close() { s.hc.CloseIdleConnections() }

// due reports whether the access token should be refreshed before use.
// The caller holds s.mu.
func (s *RefreshingSource) due() bool {
	if s.pair.AccessToken == "" {
		return true
	}
	if s.pair.ExpiresIn <= 0 {
		return false // lifetime unknown: refresh on token_expired only
	}
	life := time.Duration(s.pair.ExpiresIn) * time.Second
	return s.now().Sub(s.received) >= time.Duration(float64(life)*refreshAt)
}

// Token returns the access token, refreshing it first when it is due. If a
// proactive refresh fails transiently while the token has not yet reached
// expires_in, the current token is still returned.
func (s *RefreshingSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	tok, due := s.pair.AccessToken, s.due()
	usable := tok != "" && (s.pair.ExpiresIn <= 0 ||
		s.now().Sub(s.received) < time.Duration(s.pair.ExpiresIn)*time.Second)
	s.mu.Unlock()
	if !due {
		return tok, nil
	}
	if err := s.Refresh(ctx, tok); err != nil {
		if usable && !errors.Is(err, ErrPermanent) && ctx.Err() == nil {
			s.log.Warn("proactive token refresh failed, using current access token", "error", err)
			return tok, nil
		}
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pair.AccessToken, nil
}

// Refresh exchanges the refresh token for a new pair unless the access
// token has already changed since used was handed out. One request serves
// all concurrent callers; it runs detached from ctx (bounded by the
// configured timeout) so a cancelled caller cannot interrupt persisting the
// new pair.
func (s *RefreshingSource) Refresh(ctx context.Context, used string) error {
	s.mu.Lock()
	if s.pair.AccessToken != used && s.pair.AccessToken != "" {
		s.mu.Unlock()
		return nil
	}
	c := s.inflight
	if c == nil {
		c = &refreshCall{done: make(chan struct{})}
		s.inflight = c
		refreshToken := s.pair.RefreshToken
		go func() {
			pair, err := s.exchange(refreshToken)
			s.mu.Lock()
			if err == nil {
				s.pair, s.received = pair, s.now()
			}
			s.inflight = nil
			s.mu.Unlock()
			c.err = err
			close(c.done)
		}()
	}
	s.mu.Unlock()
	select {
	case <-c.done:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// exchange POSTs refreshToken, persists the returned pair and returns it.
func (s *RefreshingSource) exchange(refreshToken string) (TokenPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	body, err := json.Marshal(map[string]string{"refresh_token": refreshToken})
	if err != nil {
		return TokenPair{}, fmt.Errorf("refresh: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return TokenPair{}, fmt.Errorf("refresh: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := s.hc.Do(req)
	if err != nil {
		return TokenPair{}, fmt.Errorf("refresh: %w", err) // transient
	}
	defer drain(res)
	if res.StatusCode != http.StatusOK {
		return TokenPair{}, refreshErr(res.StatusCode, errorCode(res))
	}
	var pair TokenPair
	if err := json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&pair); err != nil {
		return TokenPair{}, fmt.Errorf("refresh: decode response: %w", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		return TokenPair{}, errors.New("refresh: response lacks access_token or refresh_token")
	}
	if pair.DeviceID == "" {
		pair.DeviceID = s.device
	}
	if pair.DeviceID != s.device {
		return TokenPair{}, fmt.Errorf("refresh: %w: tokens issued for device %q, want %q", ErrPermanent, pair.DeviceID, s.device)
	}
	// The presented refresh token stays valid until the new one is used, so
	// failing here (before any use) loses nothing.
	if err := writeFileAtomic(s.file, pair); err != nil {
		return TokenPair{}, fmt.Errorf("refresh: persist tokens: %w", err)
	}
	s.log.Info("access token refreshed", "device", s.device, "expires_in", pair.ExpiresIn, "refresh_expires_at", pair.RefreshExpiresAt)
	return pair, nil
}

// refreshErr maps a refresh endpoint error status.
func refreshErr(code int, reason string) error {
	if reason == "" {
		reason = http.StatusText(code)
	}
	switch code {
	case http.StatusUnauthorized:
		return fmt.Errorf("refresh: HTTP 401 %s: %w: %w: the device must be registered again", reason, ErrPermanent, ErrInvalidGrant)
	case http.StatusForbidden:
		return fmt.Errorf("refresh: HTTP 403 %s: %w: %w", reason, ErrPermanent, ErrDeviceNotActive)
	case http.StatusBadRequest:
		return fmt.Errorf("refresh: HTTP 400 %s: %w", reason, ErrPermanent)
	default:
		// 503 temporarily_unavailable, other 5xx, 429, …: transient.
		return fmt.Errorf("refresh: HTTP %d %s", code, reason)
	}
}

// writeFileAtomic replaces path with pair as JSON: a temp file in the same
// directory, fsync, rename, then fsync of the directory. Mode is 0600.
func writeFileAtomic(path string, pair TokenPair) (err error) {
	b, err := json.MarshalIndent(pair, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err = tmp.Write(b); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync() // best effort: makes the rename durable
		_ = d.Close()
	}
	return nil
}
