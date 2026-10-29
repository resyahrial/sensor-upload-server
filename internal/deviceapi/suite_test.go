package deviceapi

// The state machine suite runs against the in-memory fake
// (deviceapi_test.go) and against Postgres through *db.Devices
// (deviceapi_integration_test.go), so both implement the same rules.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/auth/authtest"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

// Short lifetimes keep the clock arithmetic readable.
const (
	testIdleTTL    = 10 * 24 * time.Hour
	testMaxAge     = 30 * 24 * time.Hour
	testRetryAfter = time.Hour
	testRemoteAddr = "192.0.2.1:40000"
)

// testStore is a Store plus the admin operations devicectl uses.
type testStore interface {
	Store
	SetStatus(ctx context.Context, id string, status db.DeviceStatus, now time.Time) error
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type env struct {
	t       *testing.T
	store   testStore
	clock   *clock
	keys    *authtest.Keys
	issuer  *auth.Issuer
	h       http.Handler
	metrics *obs.Metrics
	logs    *syncBuffer
}

// newEnv builds a handler over store. perMinute 0 = a limit high enough
// not to interfere.
func newEnv(t *testing.T, store testStore, perMinute int) *env {
	t.Helper()
	if perMinute == 0 {
		perMinute = 10000
	}
	e := &env{t: t, store: store, clock: &clock{t: time.Now().UTC().Truncate(time.Second)},
		keys: authtest.New(t), logs: &syncBuffer{}}
	var err error
	e.issuer, err = auth.NewIssuer(auth.IssuerConfig{
		Key: e.keys.Key, Issuer: authtest.Issuer, TTL: time.Hour, Now: e.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if e.metrics, err = obs.NewMetrics(prometheus.NewRegistry()); err != nil {
		t.Fatal(err)
	}
	e.h, err = NewHandler(Config{
		Store: store, Issuer: e.issuer,
		IdleTTL: testIdleTTL, MaxAge: testMaxAge, RetryAfter: testRetryAfter,
		RegisterPerMinute: perMinute, RegisterBurst: perMinute,
		Now: e.clock.Now, Metrics: e.metrics,
		Log: slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) do(method, path, body, remote string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = remote
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

func (e *env) register(device, secret string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(map[string]any{"device_id": device, "poll_secret": secret, "lat": -6.2, "lon": 106.8, "firmware": "1.2.3"})
	return e.do(http.MethodPost, PathRegister, string(b), testRemoteAddr)
}

func (e *env) refresh(token string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(RefreshRequest{RefreshToken: token})
	return e.do(http.MethodPost, PathRefresh, string(b), testRemoteAddr)
}

func (e *env) approve(device string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(ApproveRequest{DeviceID: device})
	return e.do(http.MethodPost, PathApprove, string(b), testRemoteAddr)
}

// wantApproved checks a 200 approve answer for device.
func wantApproved(t *testing.T, rec *httptest.ResponseRecorder, device string) {
	t.Helper()
	var b ApproveResponse
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &b) != nil || b.DeviceID != device || b.Status != "active" {
		t.Fatalf("approve = %d %s, want 200 %s active", rec.Code, rec.Body, device)
	}
}

func (e *env) approveMetric(result string) float64 {
	return testutil.ToFloat64(e.metrics.DeviceApprove.WithLabelValues(result))
}

func (e *env) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	e.t.Cleanup(cancel)
	return ctx
}

// newDevice returns a fresh, never registered device id.
func (e *env) newDevice() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "dev-" + hex.EncodeToString(b)
}

func (e *env) setStatus(device string, status db.DeviceStatus) {
	e.t.Helper()
	if err := e.store.SetStatus(e.ctx(), device, status, e.clock.Now()); err != nil {
		e.t.Fatalf("SetStatus(%s, %s): %v", device, status, err)
	}
}

func (e *env) status(device string) db.DeviceStatus {
	e.t.Helper()
	d, err := e.store.Get(e.ctx(), device)
	if err != nil {
		e.t.Fatal(err)
	}
	return d.Status
}

func newSecret(t *testing.T) string {
	t.Helper()
	s, err := auth.GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// active registers, approves and collects a token pair for a new device.
func (e *env) active() (device, secret string, tr TokenResponse) {
	e.t.Helper()
	device, secret = e.newDevice(), newSecret(e.t)
	wantPending(e.t, e.register(device, secret))
	e.setStatus(device, db.DeviceActive)
	return device, secret, e.tokens(e.register(device, secret), device)
}

func wantError(t *testing.T, rec *httptest.ResponseRecorder, code int, errCode string) {
	t.Helper()
	if rec.Code != code {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var b errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil || b.Error != errCode {
		t.Fatalf("body = %s, want error %q", rec.Body, errCode)
	}
}

func wantPending(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Retry-After"); got != strconv.Itoa(int(testRetryAfter/time.Second)) {
		t.Fatalf("Retry-After = %q", got)
	}
	var b pendingBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil || b.Status != StatusPending {
		t.Fatalf("body = %s", rec.Body)
	}
}

// tokens checks a 200 token pair for device and returns it. The access
// token is verified only while the test clock is near real time.
func (e *env) tokens(rec *httptest.ResponseRecorder, device string) TokenResponse {
	e.t.Helper()
	if rec.Code != http.StatusOK {
		e.t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body)
	}
	var tr TokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &tr); err != nil {
		e.t.Fatal(err)
	}
	if tr.DeviceID != device || tr.TokenType != "Bearer" || tr.ExpiresIn != 3600 || !auth.ValidSecret(tr.RefreshToken) {
		e.t.Fatalf("token pair = %+v", tr)
	}
	if _, err := time.Parse(time.RFC3339, tr.RefreshExpiresAt); err != nil {
		e.t.Fatalf("refresh_expires_at %q: %v", tr.RefreshExpiresAt, err)
	}
	if d := time.Until(e.clock.Now()); d > -time.Minute && d < time.Minute {
		sub, err := e.keys.Verifier.Verify(tr.AccessToken)
		if err != nil || sub != device {
			e.t.Fatalf("Verify(access_token) = %q, %v", sub, err)
		}
	}
	return tr
}

func (e *env) refreshMetric(result string) float64 {
	return testutil.ToFloat64(e.metrics.TokenRefresh.WithLabelValues(result))
}

// runSuite runs the state machine scenarios; newStore returns the store
// for one scenario (it may be shared: device ids are unique).
func runSuite(t *testing.T, newStore func(t *testing.T) testStore) {
	scenarios := []struct {
		name string
		fn   func(t *testing.T, e *env)
	}{
		{"unknown device registers pending", func(t *testing.T, e *env) {
			dev := e.newDevice()
			wantPending(t, e.register(dev, newSecret(t)))
			d, err := e.store.Get(e.ctx(), dev)
			if err != nil || d.Status != db.DevicePending {
				t.Fatalf("Get = %+v, %v; want pending", d, err)
			}
		}},
		{"public approve", func(t *testing.T, e *env) {
			dev, secret := e.newDevice(), newSecret(t)
			wantError(t, e.approve(dev), http.StatusNotFound, CodeDeviceNotFound)
			wantPending(t, e.register(dev, secret))
			// Any letter case names the same device; the answer is lowercase.
			wantApproved(t, e.approve(strings.ToUpper(dev)), dev)
			if d, err := e.store.Get(e.ctx(), dev); err != nil || d.Status != db.DeviceActive || d.ApprovedAt == nil {
				t.Fatalf("Get = %+v, %v; want active with approved_at", d, err)
			}
			e.tokens(e.register(dev, secret), dev)
			wantApproved(t, e.approve(dev), dev) // already active: no-op
			if e.approveMetric(obs.ApproveApproved) != 1 || e.approveMetric(obs.ApproveAlreadyActive) != 1 ||
				e.approveMetric(obs.ApproveNotFound) != 1 {
				t.Fatal("approve metrics")
			}
			// A revoked (or rejected) device is not re-activated publicly.
			e.setStatus(dev, db.DeviceRevoked)
			wantError(t, e.approve(dev), http.StatusForbidden, CodeDeviceNotActive)
			if d, _ := e.store.Get(e.ctx(), dev); d.Status != db.DeviceRevoked {
				t.Fatalf("status = %s, want revoked", d.Status)
			}
			wantError(t, e.approve("bad_id"), http.StatusBadRequest, CodeInvalidRequest)
			wantError(t, e.do(http.MethodPost, PathApprove, `{`, testRemoteAddr), http.StatusBadRequest, CodeInvalidRequest)
		}},
		{"pending then mismatch", func(t *testing.T, e *env) {
			dev, secret := e.newDevice(), newSecret(t)
			wantPending(t, e.register(dev, secret))
			wantPending(t, e.register(dev, secret))
			wantError(t, e.register(dev, newSecret(t)), http.StatusConflict, CodePollSecretMismatch)
			d, err := e.store.Get(e.ctx(), dev)
			if err != nil {
				t.Fatal(err)
			}
			if d.Status != db.DevicePending || d.Lat == nil || *d.Lat != -6.2 || d.Firmware == nil || *d.Firmware != "1.2.3" {
				t.Fatalf("device = %+v", d)
			}
		}},
		{"approve issues tokens; lost 200 collected again", func(t *testing.T, e *env) {
			dev, secret, first := e.active()
			second := e.tokens(e.register(dev, secret), dev)
			if second.RefreshToken == first.RefreshToken {
				t.Fatal("register returned the same refresh token twice")
			}
			// The replaced family's token is reset, not reuse.
			wantError(t, e.refresh(first.RefreshToken), http.StatusUnauthorized, CodeInvalidGrant)
			if e.refreshMetric(obs.RefreshReuse) != 0 {
				t.Fatal("replaced family counted as reuse")
			}
			e.tokens(e.refresh(second.RefreshToken), dev)
			// Used once: the poll secret is cleared.
			wantError(t, e.register(dev, secret), http.StatusConflict, CodePollSecretMismatch)
		}},
		{"refresh_expires_at", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			start := e.clock.Now()
			if want := start.Add(testIdleTTL).Format(time.RFC3339); tr.RefreshExpiresAt != want {
				t.Fatalf("refresh_expires_at = %s, want %s", tr.RefreshExpiresAt, want)
			}
			for _, days := range []time.Duration{9, 9, 7} { // 25 days, within the idle TTL each
				e.clock.Advance(days * 24 * time.Hour)
				tr = e.tokens(e.refresh(tr.RefreshToken), dev)
			}
			if want := start.Add(testMaxAge).Format(time.RFC3339); tr.RefreshExpiresAt != want {
				t.Fatalf("refresh_expires_at = %s, want max age %s", tr.RefreshExpiresAt, want)
			}
		}},
		{"lost response: refresh with current twice", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			n1 := e.tokens(e.refresh(tr.RefreshToken), dev)
			n2 := e.tokens(e.refresh(tr.RefreshToken), dev) // n1's response was lost
			// n1 was replaced by n2: presenting it is reuse.
			wantError(t, e.refresh(n1.RefreshToken), http.StatusUnauthorized, CodeInvalidGrant)
			if e.refreshMetric(obs.RefreshReuse) != 1 {
				t.Fatal("reuse of replaced next not counted")
			}
			n3 := e.tokens(e.refresh(n2.RefreshToken), dev) // promote n2
			// The original current is retired by the promotion.
			wantError(t, e.refresh(tr.RefreshToken), http.StatusUnauthorized, CodeInvalidGrant)
			e.tokens(e.refresh(n3.RefreshToken), dev)
		}},
		{"crash after saving next", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			next := e.tokens(e.refresh(tr.RefreshToken), dev)
			// Device saved next, then crashed before using it; next works,
			// and keeps working as current if that response is lost too.
			a := e.tokens(e.refresh(next.RefreshToken), dev)
			b := e.tokens(e.refresh(next.RefreshToken), dev)
			e.tokens(e.refresh(b.RefreshToken), dev)
			wantError(t, e.refresh(a.RefreshToken), http.StatusUnauthorized, CodeInvalidGrant)
		}},
		{"reuse detection does not revoke", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			next := e.tokens(e.refresh(tr.RefreshToken), dev)
			cur := e.tokens(e.refresh(next.RefreshToken), dev) // tr retired
			wantError(t, e.refresh(tr.RefreshToken), http.StatusUnauthorized, CodeInvalidGrant)
			if e.refreshMetric(obs.RefreshReuse) != 1 {
				t.Fatal("reuse not counted")
			}
			logs := e.logs.String()
			if !strings.Contains(logs, `level=WARN msg="refresh token reuse" device_id=`+dev) {
				t.Fatalf("no reuse warning in logs:\n%s", logs)
			}
			for _, s := range []string{tr.RefreshToken, next.RefreshToken, cur.RefreshToken} {
				if strings.Contains(logs, s) {
					t.Fatal("logs contain a refresh token")
				}
			}
			if e.status(dev) != db.DeviceActive {
				t.Fatal("reuse changed the device status")
			}
			e.tokens(e.refresh(cur.RefreshToken), dev)
		}},
		{"unknown token", func(t *testing.T, e *env) {
			wantError(t, e.refresh(newSecret(t)), http.StatusUnauthorized, CodeInvalidGrant)
		}},
		{"idle expiry", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			e.clock.Advance(testIdleTTL - time.Second)
			tr = e.tokens(e.refresh(tr.RefreshToken), dev)
			e.clock.Advance(testIdleTTL + time.Second)
			wantError(t, e.refresh(tr.RefreshToken), http.StatusUnauthorized, CodeInvalidGrant)
		}},
		{"max age expiry", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			for range 3 { // 27 days, each step within the idle TTL
				e.clock.Advance(9 * 24 * time.Hour)
				tr = e.tokens(e.refresh(tr.RefreshToken), dev)
			}
			e.clock.Advance(3*24*time.Hour + time.Second)
			wantError(t, e.refresh(tr.RefreshToken), http.StatusUnauthorized, CodeInvalidGrant)
		}},
		{"re-registration after expiry", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			e.tokens(e.refresh(tr.RefreshToken), dev) // secret cleared
			e.clock.Advance(testIdleTTL + time.Second)
			secret := newSecret(t)
			wantPending(t, e.register(dev, secret))
			if e.status(dev) != db.DevicePending {
				t.Fatal("not pending after re-registration")
			}
			wantError(t, e.refresh(tr.RefreshToken), http.StatusForbidden, CodeDeviceNotActive)
			wantError(t, e.register(dev, newSecret(t)), http.StatusConflict, CodePollSecretMismatch)
			e.setStatus(dev, db.DeviceActive)
			e.clock.Set(time.Now().UTC().Truncate(time.Second)) // tokens verify again
			e.tokens(e.register(dev, secret), dev)
		}},
		{"active without family or secret re-registers", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			e.tokens(e.refresh(tr.RefreshToken), dev) // secret cleared
			e.setStatus(dev, db.DeviceRevoked)        // family deleted
			e.setStatus(dev, db.DeviceActive)
			wantPending(t, e.register(dev, newSecret(t)))
		}},
		{"revoke", func(t *testing.T, e *env) {
			dev, secret, tr := e.active()
			e.setStatus(dev, db.DeviceRevoked)
			wantError(t, e.refresh(tr.RefreshToken), http.StatusForbidden, CodeDeviceNotActive)
			wantError(t, e.register(dev, secret), http.StatusForbidden, CodeDeviceNotActive)
		}},
		{"revoke after the token was used", func(t *testing.T, e *env) {
			dev, secret, tr := e.active()
			next := e.tokens(e.refresh(tr.RefreshToken), dev)
			e.setStatus(dev, db.DeviceRevoked)
			wantError(t, e.refresh(next.RefreshToken), http.StatusForbidden, CodeDeviceNotActive)
			wantError(t, e.refresh(tr.RefreshToken), http.StatusForbidden, CodeDeviceNotActive)
			// The poll secret was cleared by the refresh.
			wantError(t, e.register(dev, secret), http.StatusConflict, CodePollSecretMismatch)
		}},
		{"reject", func(t *testing.T, e *env) {
			dev, secret := e.newDevice(), newSecret(t)
			wantPending(t, e.register(dev, secret))
			e.setStatus(dev, db.DeviceRejected)
			wantError(t, e.register(dev, secret), http.StatusForbidden, CodeDeviceNotActive)
		}},
		{"issue starts a new family", func(t *testing.T, e *env) {
			dev, _, tr := e.active()
			cfg := IssueConfig{Store: e.store, Issuer: e.issuer, IdleTTL: testIdleTTL, MaxAge: testMaxAge, Now: e.clock.Now()}
			issued, err := Issue(e.ctx(), cfg, dev)
			if err != nil {
				t.Fatal(err)
			}
			wantError(t, e.refresh(tr.RefreshToken), http.StatusUnauthorized, CodeInvalidGrant)
			e.tokens(e.refresh(issued.RefreshToken), dev)

			fresh := e.newDevice() // no devices row yet
			if issued, err = Issue(e.ctx(), cfg, fresh); err != nil {
				t.Fatal(err)
			}
			if e.status(fresh) != db.DeviceActive {
				t.Fatal("issue did not activate the device")
			}
			e.tokens(e.refresh(issued.RefreshToken), fresh)
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			sc.fn(t, newEnv(t, newStore(t), 0))
		})
	}
}
