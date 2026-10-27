package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/auth/authtest"
)

// fakeRefresh serves RefreshPath like the server: it accepts the current
// refresh token (and the previous one until the current one is used) and
// returns a new pair whose access token is minted by keys.
type fakeRefresh struct {
	t    *testing.T
	keys *authtest.Keys
	srv  *httptest.Server

	expiresIn int64
	delay     time.Duration
	// reply, if set, can answer request n (1-based) with a status and
	// error code instead of rotating.
	reply func(n int) (int, string)

	calls atomic.Int32
	mu    sync.Mutex
	valid map[string]bool
	last  TokenPair
}

func newFakeRefresh(t *testing.T, keys *authtest.Keys, refreshToken string) *fakeRefresh {
	t.Helper()
	f := &fakeRefresh{t: t, keys: keys, expiresIn: 3600, valid: map[string]bool{refreshToken: true}}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeRefresh) serve(w http.ResponseWriter, r *http.Request) {
	n := int(f.calls.Add(1))
	time.Sleep(f.delay)
	w.Header().Set("Content-Type", "application/json")
	fail := func(code int, reason string) {
		w.WriteHeader(code)
		_, _ = fmt.Fprintf(w, `{"error":%q}`, reason)
	}
	if r.Method != http.MethodPost || r.URL.Path != RefreshPath || r.Header.Get("Authorization") != "" {
		fail(http.StatusBadRequest, "invalid_request")
		return
	}
	if f.reply != nil {
		if code, reason := f.reply(n); code != 0 {
			fail(code, reason)
			return
		}
	}
	var req struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(http.StatusBadRequest, "invalid_request")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.valid[req.RefreshToken] {
		fail(http.StatusUnauthorized, "invalid_grant")
		return
	}
	next, err := auth.GenerateSecret()
	if err != nil {
		f.t.Error(err)
	}
	f.valid = map[string]bool{req.RefreshToken: true, next: true}
	f.last = TokenPair{
		DeviceID: "dev-1", AccessToken: f.keys.Token(f.t, "dev-1"), TokenType: "Bearer", ExpiresIn: f.expiresIn,
		RefreshToken: next, RefreshExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}
	_ = json.NewEncoder(w).Encode(f.last)
}

func (f *fakeRefresh) lastPair() TokenPair {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

// writePair writes a token file and returns its path.
func writePair(t *testing.T, dir string, p TokenPair) string {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "tokens.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func readPair(t *testing.T, path string) TokenPair {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var p TokenPair
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// clock is an adjustable test clock on top of the real one.
type clock struct{ off atomic.Int64 }

func (c *clock) now() time.Time          { return time.Now().Add(time.Duration(c.off.Load())) }
func (c *clock) advance(d time.Duration) { c.off.Add(int64(d)) }

func newSource(t *testing.T, file string, f *fakeRefresh, c *clock) *RefreshingSource {
	t.Helper()
	cfg := RefreshConfig{File: file, URL: f.srv.URL, HTTPClient: f.srv.Client()}
	if c != nil {
		cfg.Now = c.now
	}
	s, err := NewRefreshingSource(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// tusJWT makes f verify tokens with keys like the server (401 token_expired
// / invalid_token).
func tusJWT(f *fakeTus, keys *authtest.Keys) {
	f.authorize = func(h string) (int, string) {
		dev, err := keys.Verifier.VerifyHeader(context.Background(), h)
		switch {
		case errors.Is(err, auth.ErrTokenExpired):
			return http.StatusUnauthorized, "token_expired"
		case err != nil:
			return http.StatusUnauthorized, "invalid_token"
		case dev != "dev-1":
			return http.StatusForbidden, "device_not_active"
		}
		return 0, ""
	}
}

func grpcJWT(keys *authtest.Keys, h string) error {
	_, err := keys.Verifier.VerifyHeader(context.Background(), h)
	switch {
	case errors.Is(err, auth.ErrTokenExpired):
		return status.Error(codes.Unauthenticated, "token_expired")
	case err != nil:
		return status.Error(codes.Unauthenticated, "invalid_token")
	}
	return nil
}

func TestRefreshingSourceLoad(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	f := newFakeRefresh(t, keys, "r0")

	// device_id from the access token's sub when the file has none.
	s := newSource(t, writePair(t, dir, TokenPair{AccessToken: keys.Token(t, "dev-1"), RefreshToken: "r0"}), f, nil)
	if s.Device() != "dev-1" {
		t.Fatalf("device = %q", s.Device())
	}
	for _, bad := range []string{`{}`, `{"refresh_token":"r0"}`, `nope`} {
		p := filepath.Join(dir, "bad.json")
		if err := os.WriteFile(p, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewRefreshingSource(RefreshConfig{File: p, URL: "https://h"}); err == nil {
			t.Errorf("file %s: want error", bad)
		}
	}
	if _, err := NewRefreshingSource(RefreshConfig{File: filepath.Join(dir, "missing"), URL: "https://h"}); err == nil {
		t.Error("missing file: want error")
	}

	// Without an access token the first Token call refreshes.
	s = newSource(t, writePair(t, dir, TokenPair{DeviceID: "dev-1", RefreshToken: "r0"}), f, nil)
	tok, err := s.Token(t.Context())
	if err != nil || tok != f.lastPair().AccessToken || f.calls.Load() != 1 {
		t.Fatalf("token %q err %v calls %d", tok, err, f.calls.Load())
	}
}

func TestRefreshingSourceProactive(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	f := newFakeRefresh(t, keys, "r0")
	f.expiresIn = 100
	first := keys.Token(t, "dev-1")
	c := &clock{}
	s := newSource(t, writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: first, ExpiresIn: 100, RefreshToken: "r0"}), f, c)

	c.advance(79 * time.Second)
	if tok, err := s.Token(t.Context()); err != nil || tok != first || f.calls.Load() != 0 {
		t.Fatalf("before 80%%: token changed=%v err %v calls %d", tok != first, err, f.calls.Load())
	}
	c.advance(2 * time.Second) // 81 s of 100 s
	second, err := s.Token(t.Context())
	if err != nil || second == first || f.calls.Load() != 1 {
		t.Fatalf("after 80%%: token changed=%v err %v calls %d", second != first, err, f.calls.Load())
	}
	// The new token's clock starts at receipt.
	c.advance(79 * time.Second)
	if tok, _ := s.Token(t.Context()); tok != second || f.calls.Load() != 1 {
		t.Fatalf("second token refreshed early (calls %d)", f.calls.Load())
	}

	// A transient failure of a proactive refresh keeps the still-valid token.
	f.reply = func(int) (int, string) { return http.StatusServiceUnavailable, "temporarily_unavailable" }
	c.advance(2 * time.Second)
	if tok, err := s.Token(t.Context()); err != nil || tok != second {
		t.Fatalf("transient proactive failure: err %v", err)
	}
	// Once expires_in has fully elapsed it reports the error.
	c.advance(30 * time.Second)
	if _, err := s.Token(t.Context()); err == nil || errors.Is(err, ErrPermanent) {
		t.Fatalf("past expires_in: err %v, want transient error", err)
	}
}

func TestRefreshingSourceSingleFlight(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	f := newFakeRefresh(t, keys, "r0")
	f.delay = 50 * time.Millisecond
	old := keys.Expired(t, "dev-1")
	s := newSource(t, writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: old, ExpiresIn: 3600, RefreshToken: "r0"}), f, nil)

	var wg sync.WaitGroup
	toks := make([]string, 20)
	errs := make([]error, 20)
	for i := range toks {
		wg.Go(func() {
			if errs[i] = s.Refresh(t.Context(), old); errs[i] == nil {
				toks[i], errs[i] = s.Token(t.Context())
			}
		})
	}
	wg.Wait()
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("refresh requests = %d, want 1", n)
	}
	for i := range toks {
		if errs[i] != nil || toks[i] != f.lastPair().AccessToken {
			t.Fatalf("caller %d: err %v", i, errs[i])
		}
	}
	// A late caller holding the old token does not refresh again.
	if err := s.Refresh(t.Context(), old); err != nil || f.calls.Load() != 1 {
		t.Fatalf("stale refresh: err %v calls %d", err, f.calls.Load())
	}
}

func TestRefreshingSourcePersistsAtomically(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	f := newFakeRefresh(t, keys, "r0")
	path := writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: keys.Expired(t, "dev-1"), RefreshToken: "r0"})
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	s := newSource(t, path, f, nil)
	tok, _ := s.Token(t.Context())
	if err := s.Refresh(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	if got := readPair(t, path); got != f.lastPair() {
		t.Fatalf("file = %+v, want %+v", got, f.lastPair())
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want 0600", st.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("dir has %d entries, want only the token file (no temp files)", len(entries))
	}
	// The persisted file loads as a source for the next run.
	if s2 := newSource(t, path, f, nil); s2.Device() != "dev-1" {
		t.Fatalf("reload device %q", s2.Device())
	}

	// An unwritable directory fails the refresh before the new pair is used.
	if os.Getuid() != 0 {
		before, _ := s.Token(t.Context())
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		if err := s.Refresh(t.Context(), before); err == nil || errors.Is(err, ErrPermanent) {
			t.Fatalf("unwritable dir: err %v, want transient", err)
		}
		if after, _ := s.Token(t.Context()); after != before {
			t.Fatal("unpersisted token was used")
		}
	}
}

func TestRefreshErrors(t *testing.T) {
	for _, tc := range []struct {
		code      int
		reason    string
		permanent bool
		is        error
	}{
		{401, "invalid_grant", true, ErrInvalidGrant},
		{403, "device_not_active", true, ErrDeviceNotActive},
		{400, "invalid_request", true, ErrPermanent},
		{503, "temporarily_unavailable", false, nil},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			keys := authtest.New(t)
			f := newFakeRefresh(t, keys, "r0")
			f.reply = func(int) (int, string) { return tc.code, tc.reason }
			path := writePair(t, t.TempDir(), TokenPair{DeviceID: "dev-1", RefreshToken: "r0"})
			s := newSource(t, path, f, nil)
			_, err := s.Token(t.Context())
			if err == nil || errors.Is(err, ErrPermanent) != tc.permanent || (tc.is != nil && !errors.Is(err, tc.is)) {
				t.Fatalf("err = %v", err)
			}
			if !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("err %q does not name %s", err, tc.reason)
			}
			if got := readPair(t, path); got.RefreshToken != "r0" {
				t.Fatal("file changed after a failed refresh")
			}
		})
	}
	// Network failure: transient.
	path := writePair(t, t.TempDir(), TokenPair{DeviceID: "dev-1", RefreshToken: "r0"})
	s, err := NewRefreshingSource(RefreshConfig{File: path, URL: "https://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Token(t.Context()); err == nil || errors.Is(err, ErrPermanent) {
		t.Fatalf("network failure: %v", err)
	}
}

func TestTusReactiveRefreshRetries(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	path, data, sha := writeFile(t, dir, "r.flac", 2*testChunk+3)
	f := newFakeTus("/rest/files/")
	tusJWT(f, keys)
	fr := newFakeRefresh(t, keys, "r0")
	tokens := writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: keys.Expired(t, "dev-1"), ExpiresIn: 3600, RefreshToken: "r0"})

	cfg := tusConfig(t, f)
	cfg.Token, cfg.Tokens = "", newSource(t, tokens, fr, nil)
	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	if res.Attempts != 1 || fr.calls.Load() != 1 || f.rejected != 1 {
		t.Fatalf("attempts %d refreshes %d rejected %d", res.Attempts, fr.calls.Load(), f.rejected)
	}
	if calls := f.callLog(); calls[0] != "POST 401" || calls[1] != "POST 201" {
		t.Fatalf("calls = %v", calls)
	}
	if got, _ := f.stored(sha); string(got) != string(data) {
		t.Fatal("stored data differs")
	}
	if readPair(t, tokens) != fr.lastPair() {
		t.Fatal("token file not updated")
	}
}

// TestTusReactiveRefreshMidUpload expires the token between PATCHes: the
// rejected PATCH is retried with the same offset and a fresh body.
func TestTusReactiveRefreshMidUpload(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	path, data, sha := writeFile(t, dir, "m.flac", 3*testChunk)
	f := newFakeTus("/rest/files/")
	fr := newFakeRefresh(t, keys, "r0")
	first := keys.Token(t, "dev-1")
	var patches atomic.Int32
	f.authorize = func(h string) (int, string) {
		if h == "Bearer "+first && patches.Add(1) > 2 { // POST + first PATCH pass
			return http.StatusUnauthorized, "token_expired"
		}
		if _, err := keys.Verifier.VerifyHeader(context.Background(), h); err != nil {
			return http.StatusUnauthorized, "invalid_token"
		}
		return 0, ""
	}
	tokens := writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: first, ExpiresIn: 3600, RefreshToken: "r0"})
	cfg := tusConfig(t, f)
	cfg.Tokens = newSource(t, tokens, fr, nil)
	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	if res.Attempts != 1 || fr.calls.Load() != 1 {
		t.Fatalf("attempts %d refreshes %d", res.Attempts, fr.calls.Load())
	}
	if got, _ := f.stored(sha); string(got) != string(data) {
		t.Fatal("stored data differs")
	}
}

func TestTusConcurrentRefreshSingleFlight(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	f := newFakeTus("/rest/files/")
	tusJWT(f, keys)
	fr := newFakeRefresh(t, keys, "r0")
	fr.delay = 30 * time.Millisecond
	tokens := writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: keys.Expired(t, "dev-1"), ExpiresIn: 3600, RefreshToken: "r0"})
	cfg := tusConfig(t, f)
	cfg.Concurrency = 4
	cfg.Tokens = newSource(t, tokens, fr, nil)
	var paths []string
	for i := range 4 {
		p, _, _ := writeFile(t, dir, fmt.Sprintf("c%d.jpg", i), 1000+i)
		paths = append(paths, p)
	}
	for _, r := range runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), paths...) {
		expect(t, r, ResultOK)
	}
	if n := fr.calls.Load(); n != 1 {
		t.Fatalf("refresh requests = %d, want 1 shared by all workers", n)
	}
}

func TestTusInvalidGrantStops(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	path, _, _ := writeFile(t, dir, "i.flac", 100)
	f := newFakeTus("/rest/files/")
	tusJWT(f, keys)
	fr := newFakeRefresh(t, keys, "other") // r0 is unknown: invalid_grant
	tokens := writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: keys.Expired(t, "dev-1"), ExpiresIn: 3600, RefreshToken: "r0"})
	cfg := tusConfig(t, f)
	cfg.Tokens = newSource(t, tokens, fr, nil)
	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultFailed)
	if res.Attempts != 1 || !errors.Is(res.Err, ErrPermanent) || !errors.Is(res.Err, ErrInvalidGrant) {
		t.Fatalf("attempts %d err %v", res.Attempts, res.Err)
	}
	if readPair(t, tokens).RefreshToken != "r0" {
		t.Fatal("token file changed")
	}
}

func TestTusRefreshUnavailableBacksOff(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	path, _, _ := writeFile(t, dir, "u.flac", 100)
	f := newFakeTus("/rest/files/")
	tusJWT(f, keys)
	fr := newFakeRefresh(t, keys, "r0")
	fr.reply = func(n int) (int, string) {
		if n == 1 {
			return http.StatusServiceUnavailable, "temporarily_unavailable"
		}
		return 0, ""
	}
	tokens := writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: keys.Expired(t, "dev-1"), ExpiresIn: 3600, RefreshToken: "r0"})
	cfg := tusConfig(t, f)
	cfg.Tokens = newSource(t, tokens, fr, nil)
	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	if res.Attempts != 2 || fr.calls.Load() != 2 {
		t.Fatalf("attempts %d refreshes %d", res.Attempts, fr.calls.Load())
	}
}

func TestTusAuthErrorsWithStaticToken(t *testing.T) {
	keys := authtest.New(t)
	for _, tc := range []struct {
		name  string
		token string
		is    error
	}{
		{"expired", keys.Expired(t, "dev-1"), ErrTokenExpired},
		{"invalid", authtest.New(t).Token(t, "dev-1"), ErrInvalidToken},
		{"not active", keys.Token(t, "dev-2"), ErrDeviceNotActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path, _, _ := writeFile(t, dir, "s.flac", 100)
			f := newFakeTus("/rest/files/")
			tusJWT(f, keys)
			cfg := tusConfig(t, f)
			cfg.Token = tc.token
			res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
			expect(t, res, ResultFailed)
			if res.Attempts != 1 || !errors.Is(res.Err, ErrPermanent) || !errors.Is(res.Err, tc.is) {
				t.Fatalf("attempts %d err %v", res.Attempts, res.Err)
			}
		})
	}
}

// TestGRPCReactiveRefresh: Init is rejected as token_expired (the file's
// token is expired), then the first Upload stream is too; each refresh is
// followed by one retry and the upload completes in one attempt.
func TestGRPCReactiveRefresh(t *testing.T) {
	keys := authtest.New(t)
	dir := t.TempDir()
	path, data, sha := writeFile(t, dir, "g.flac", 3*testChunk+1)
	f := newFakeGRPC()
	var uploads atomic.Int32
	f.authorize = func(method, h string) error {
		if method == "upload" && uploads.Add(1) == 1 {
			return status.Error(codes.Unauthenticated, "token_expired")
		}
		return grpcJWT(keys, h)
	}
	fr := newFakeRefresh(t, keys, "r0")
	tokens := writePair(t, dir, TokenPair{DeviceID: "dev-1", AccessToken: keys.Expired(t, "dev-1"), ExpiresIn: 3600, RefreshToken: "r0"})
	cfg := grpcConfig(t, f, ProtoGRPC)
	cfg.Token, cfg.Tokens = "", newSource(t, tokens, fr, nil)
	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	if res.Attempts != 1 || fr.calls.Load() != 2 || uploads.Load() != 2 {
		t.Fatalf("attempts %d refreshes %d upload streams %d", res.Attempts, fr.calls.Load(), uploads.Load())
	}
	if got, _ := f.stored(sha); string(got) != string(data) {
		t.Fatal("stored data differs")
	}
	if readPair(t, tokens) != fr.lastPair() {
		t.Fatal("token file not updated")
	}
}

func TestGRPCAuthErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		is   error
	}{
		{"expired static", status.Error(codes.Unauthenticated, "token_expired"), ErrTokenExpired},
		{"invalid", status.Error(codes.Unauthenticated, "invalid_token"), ErrInvalidToken},
		{"not active", status.Error(codes.PermissionDenied, "device_not_active"), ErrDeviceNotActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path, _, _ := writeFile(t, dir, "a.flac", 100)
			f := newFakeGRPC()
			f.authorize = func(string, string) error { return tc.err }
			res := runOnce(t, t.Context(), grpcConfig(t, f, ProtoGRPC), filepath.Join(dir, "s.db"), path)[0]
			expect(t, res, ResultFailed)
			if res.Attempts != 1 || !errors.Is(res.Err, ErrPermanent) || !errors.Is(res.Err, tc.is) {
				t.Fatalf("attempts %d err %v", res.Attempts, res.Err)
			}
		})
	}
}

func TestResponseErr(t *testing.T) {
	mk := func(method string, code int, body, wwwAuth string) *http.Response {
		rec := httptest.NewRecorder()
		if wwwAuth != "" {
			rec.Header().Set("WWW-Authenticate", wwwAuth)
		}
		rec.WriteHeader(code)
		_, _ = rec.WriteString(body)
		res := rec.Result()
		res.Request = httptest.NewRequest(method, "/", nil)
		return res
	}
	for _, tc := range []struct {
		res       *http.Response
		is        error
		permanent bool
	}{
		{mk("POST", 401, `{"error":"token_expired"}`, ""), ErrTokenExpired, false},
		{mk("POST", 401, `{"error":"invalid_token"}`, ""), ErrInvalidToken, true},
		{mk("POST", 401, ``, ""), ErrInvalidToken, true},
		{mk("HEAD", 401, ``, ""), ErrTokenExpired, false},
		{mk("HEAD", 401, ``, `Bearer error="invalid_token"`), ErrInvalidToken, true},
		{mk("PATCH", 401, ``, `Bearer error="invalid_token", error_description="token_expired"`), ErrTokenExpired, false},
		{mk("PATCH", 403, `{"error":"device_not_active"}`, ""), ErrDeviceNotActive, true},
		{mk("PATCH", 403, ``, ""), ErrPermanent, true},
		{mk("PATCH", 409, ``, ""), ErrOffsetMismatch, false},
	} {
		err := responseErr("op", tc.res)
		if !errors.Is(err, tc.is) || errors.Is(err, ErrPermanent) != tc.permanent {
			t.Errorf("%s %d: %v", tc.res.Request.Method, tc.res.StatusCode, err)
		}
	}
}
