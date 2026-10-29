package deviceapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/auth/authtest"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/obs"
)

func TestSuiteFake(t *testing.T) {
	runSuite(t, func(*testing.T) testStore { return newFakeStore() })
}

func TestNewHandlerValidates(t *testing.T) {
	keys := authtest.New(t)
	ok := Config{Store: newFakeStore(), Issuer: keys.Issuer, IdleTTL: time.Hour, MaxAge: 2 * time.Hour, RetryAfter: time.Minute}
	if _, err := NewHandler(ok); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for name, mut := range map[string]func(*Config){
		"no store":       func(c *Config) { c.Store = nil },
		"no issuer":      func(c *Config) { c.Issuer = nil },
		"zero idle":      func(c *Config) { c.IdleTTL = 0 },
		"zero max age":   func(c *Config) { c.MaxAge = 0 },
		"zero retry":     func(c *Config) { c.RetryAfter = 0 },
		"negative limit": func(c *Config) { c.RegisterPerMinute = -1 },
	} {
		c := ok
		mut(&c)
		if _, err := NewHandler(c); err == nil {
			t.Errorf("%s: NewHandler succeeded", name)
		}
	}
}

func TestRoutingAndMethods(t *testing.T) {
	e := newEnv(t, newFakeStore(), 0)
	wantError(t, e.do(http.MethodPost, Prefix+"nope", "{}", testRemoteAddr), http.StatusNotFound, CodeNotFound)
	wantError(t, e.do(http.MethodPost, PathRegister+"/", "{}", testRemoteAddr), http.StatusNotFound, CodeNotFound)
	for _, path := range []string{PathRegister, PathRefresh} {
		rec := e.do(http.MethodGet, path, "", testRemoteAddr)
		wantError(t, rec, http.StatusMethodNotAllowed, CodeMethodNotAllowed)
		if rec.Header().Get("Allow") != http.MethodPost {
			t.Fatalf("Allow = %q", rec.Header().Get("Allow"))
		}
	}
}

func TestInvalidRequests(t *testing.T) {
	e := newEnv(t, newFakeStore(), 0)
	secret := newSecret(t)
	big := `{"device_id":"dev-0001","poll_secret":"` + secret + `","firmware":"` + strings.Repeat("x", 5000) + `"}`
	for name, body := range map[string]string{
		"not json":          `{`,
		"trailing data":     `{"device_id":"dev-0001","poll_secret":"` + secret + `"} {}`,
		"bad device id":     `{"device_id":"Dev_1","poll_secret":"` + secret + `"}`,
		"missing secret":    `{"device_id":"dev-0001"}`,
		"upper-case secret": `{"device_id":"dev-0001","poll_secret":"` + strings.ToUpper(secret) + `"}`,
		"short secret":      `{"device_id":"dev-0001","poll_secret":"abc"}`,
		"lat out of range":  `{"device_id":"dev-0001","poll_secret":"` + secret + `","lat":91}`,
		"lon out of range":  `{"device_id":"dev-0001","poll_secret":"` + secret + `","lon":-181}`,
		"firmware too long": `{"device_id":"dev-0001","poll_secret":"` + secret + `","firmware":"` + strings.Repeat("x", 129) + `"}`,
		"body over 4 KiB":   big,
	} {
		t.Run("register "+name, func(t *testing.T) {
			wantError(t, e.do(http.MethodPost, PathRegister, body, testRemoteAddr), http.StatusBadRequest, CodeInvalidRequest)
		})
	}
	for name, body := range map[string]string{
		"not json":      `[`,
		"missing token": `{}`,
		"short token":   `{"refresh_token":"abcd"}`,
		"non-hex token": `{"refresh_token":"` + strings.Repeat("z", 64) + `"}`,
	} {
		t.Run("refresh "+name, func(t *testing.T) {
			wantError(t, e.do(http.MethodPost, PathRefresh, body, testRemoteAddr), http.StatusBadRequest, CodeInvalidRequest)
		})
	}
	// Optional fields may be omitted.
	wantPending(t, e.do(http.MethodPost, PathRegister, `{"device_id":"dev-0001","poll_secret":"`+secret+`"}`, testRemoteAddr))
}

func TestRegisterRateLimit(t *testing.T) {
	store := newFakeStore()
	e := newEnv(t, store, 10)
	h, err := NewHandler(Config{Store: store, Issuer: e.issuer, IdleTTL: testIdleTTL, MaxAge: testMaxAge,
		RetryAfter: testRetryAfter, Now: e.clock.Now, Metrics: e.metrics}) // default 10/min, burst 10
	if err != nil {
		t.Fatal(err)
	}
	e.h = h
	dev, secret := e.newDevice(), newSecret(t)
	for i := range 10 {
		if rec := e.register(dev, secret); rec.Code != http.StatusAccepted {
			t.Fatalf("request %d: status %d", i, rec.Code)
		}
	}
	rec := e.register(dev, secret)
	wantError(t, rec, http.StatusTooManyRequests, CodeRateLimited)
	if got := rec.Header().Get("Retry-After"); got != "6" {
		t.Fatalf("Retry-After = %q, want 6", got)
	}
	// Another client IP has its own bucket.
	if rec := e.do(http.MethodPost, PathRegister, `{"device_id":"`+dev+`","poll_secret":"`+secret+`"}`, "198.51.100.7:1"); rec.Code != http.StatusAccepted {
		t.Fatalf("other IP: status %d", rec.Code)
	}
	e.clock.Advance(6 * time.Second)
	wantPending(t, e.register(dev, secret))
	wantError(t, e.register(dev, secret), http.StatusTooManyRequests, CodeRateLimited)
	if got := testutil.ToFloat64(e.metrics.DeviceRegister.WithLabelValues(obs.RegisterRateLimited)); got != 2 {
		t.Fatalf("rate_limited metric = %v", got)
	}
}

func TestLimiterSweepsIdleBuckets(t *testing.T) {
	c := &clock{t: time.Unix(1000, 0)}
	l := newLimiter(1, 2, c.Now)
	for _, k := range []string{"a", "b"} {
		if ok, _ := l.allow(k); !ok {
			t.Fatal("first request refused")
		}
	}
	c.Advance(2 * rateLimiterSweepEvery)
	l.allow("c")
	if len(l.buckets) != 1 {
		t.Fatalf("buckets after sweep = %d, want 1", len(l.buckets))
	}
}

func TestStoreFailureIs503(t *testing.T) {
	store := newFakeStore()
	e := newEnv(t, store, 0)
	dev, secret, tr := e.active()
	store.fail = errors.New("connection refused")
	for name, rec := range map[string]func() int{
		"register": func() int { return check503(t, e.register(dev, secret).Result()) },
		"refresh":  func() int { return check503(t, e.refresh(tr.RefreshToken).Result()) },
	} {
		if rec() != http.StatusServiceUnavailable {
			t.Fatalf("%s: not 503", name)
		}
	}
	if got := testutil.ToFloat64(e.metrics.TokenRefresh.WithLabelValues(obs.RefreshError)); got != 1 {
		t.Fatalf("refresh error metric = %v", got)
	}
	if !strings.Contains(e.logs.String(), "connection refused") {
		t.Fatal("store error not logged")
	}
}

func check503(t *testing.T, res *http.Response) int {
	t.Helper()
	if res.StatusCode == http.StatusServiceUnavailable && res.Header.Get("Retry-After") != "5" {
		t.Fatalf("Retry-After = %q, want 5", res.Header.Get("Retry-After"))
	}
	return res.StatusCode
}

func TestPruneRetired(t *testing.T) {
	store := newFakeStore()
	e := newEnv(t, store, 0)
	dev, _, tr := e.active()
	next := e.tokens(e.refresh(tr.RefreshToken), dev) // prunes (first call), nothing to prune
	e.tokens(e.refresh(next.RefreshToken), dev)       // tr retired now
	store.mu.Lock()
	retired := len(store.retired)
	store.mu.Unlock()
	if retired == 0 {
		t.Fatal("nothing retired")
	}
	// Past max age (and the prune interval) a refresh of another device
	// prunes the old hashes.
	e.clock.Advance(testMaxAge + time.Hour)
	other, _, otr := e.active()
	e.tokens(e.refresh(otr.RefreshToken), other)
	store.mu.Lock()
	defer store.mu.Unlock()
	for k, at := range store.retireAt {
		if at.Before(e.clock.Now().Add(-testMaxAge)) {
			t.Fatalf("retired hash %x from %s not pruned", k, at)
		}
	}
	if _, ok := store.retired[string(auth.HashSecret(tr.RefreshToken))]; ok {
		t.Fatal("old retired hash not pruned")
	}
}

func TestRegisterLowercasesDeviceID(t *testing.T) {
	store := newFakeStore()
	e := newEnv(t, store, 0)
	secret := newSecret(t)
	wantPending(t, e.register("WIH-001", secret))
	if d := store.device("wih-001"); d.Status != db.DevicePending {
		t.Fatalf("wih-001 = %+v, want pending", d)
	}
	// Any spelling is the same device: same secret stays pending.
	wantPending(t, e.register("Wih-001", secret))
	wantError(t, e.register("wih-001", newSecret(t)), http.StatusConflict, CodePollSecretMismatch)
}

func TestRegisterUpdatesPendingLocation(t *testing.T) {
	store := newFakeStore()
	e := newEnv(t, store, 0)
	secret := newSecret(t)
	wantPending(t, e.register("dev-0002", secret))
	e.clock.Advance(time.Minute)
	wantPending(t, e.do(http.MethodPost, PathRegister, `{"device_id":"dev-0002","poll_secret":"`+secret+`","lat":1.5}`, testRemoteAddr))
	d := store.device("dev-0002")
	if *d.Lat != 1.5 || *d.Lon != 106.8 || !d.LastSeenAt.Equal(e.clock.Now()) || d.Status != db.DevicePending {
		t.Fatalf("device = %+v", d)
	}
}
