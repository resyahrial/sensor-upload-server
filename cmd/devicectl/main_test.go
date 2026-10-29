package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/deviceapi"
)

// fakeStore is an in-memory deviceStore.
type fakeStore struct {
	devices  map[string]db.Device
	families map[string]db.RefreshFamily
	retired  []db.RetiredHash
	listed   db.DeviceStatus
	opened   int
}

func newFake() *fakeStore {
	return &fakeStore{devices: map[string]db.Device{}, families: map[string]db.RefreshFamily{}}
}

func (f *fakeStore) Update(_ context.Context, id string, fn func(s *db.DeviceState) error) error {
	s := &db.DeviceState{DeviceID: id}
	if d, ok := f.devices[id]; ok {
		s.Device = &d
	}
	if fam, ok := f.families[id]; ok {
		s.Family = &fam
	}
	if err := fn(s); err != nil {
		return err
	}
	if s.Device != nil {
		f.devices[id] = *s.Device
	}
	if s.Family != nil {
		f.families[id] = *s.Family
	} else {
		delete(f.families, id)
	}
	f.retired = append(f.retired, s.Retire...)
	return nil
}

func (f *fakeStore) FindRefreshHash(context.Context, []byte) (db.TokenMatch, error) {
	return db.TokenMatch{}, db.ErrTokenNotFound
}

func (f *fakeStore) Get(_ context.Context, id string) (db.Device, error) {
	d, ok := f.devices[id]
	if !ok {
		return db.Device{}, db.ErrDeviceNotFound
	}
	return d, nil
}

func (f *fakeStore) PruneRetired(context.Context, time.Time) (int64, error) { return 0, nil }

func (f *fakeStore) List(_ context.Context, status db.DeviceStatus) ([]db.Device, error) {
	f.listed = status
	var out []db.Device
	for _, d := range f.devices {
		if status == "" || d.Status == status {
			out = append(out, d)
		}
	}
	slices.SortFunc(out, func(a, b db.Device) int { return strings.Compare(a.DeviceID, b.DeviceID) })
	return out, nil
}

func (f *fakeStore) SetStatus(ctx context.Context, id string, status db.DeviceStatus, now time.Time) error {
	return f.Update(ctx, id, func(s *db.DeviceState) error { return db.ApplyStatus(s, status, now) })
}

var testNow = time.Date(2026, 10, 27, 3, 0, 0, 0, time.UTC)

type harness struct {
	store          *fakeStore
	env            map[string]string
	stdout, stderr bytes.Buffer
	key            *ecdsa.PrivateKey
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	key, err := auth.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	pem, err := auth.EncodePrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwt.pem")
	if err := os.WriteFile(path, pem, 0o600); err != nil {
		t.Fatal(err)
	}
	return &harness{store: newFake(), key: key, env: map[string]string{"JWT_SIGNING_KEY_FILE": path}}
}

func (h *harness) run(args ...string) error {
	h.stdout.Reset()
	h.stderr.Reset()
	return run(context.Background(), args, deps{
		open: func(context.Context) (deviceStore, func(), error) {
			h.store.opened++
			return h.store, func() {}, nil
		},
		getenv: func(k string) string { return h.env[k] },
		now:    func() time.Time { return testNow },
		stdout: &h.stdout,
		stderr: &h.stderr,
	})
}

// verify parses an access token signed by h.key (at testNow) and returns
// its claims.
func (h *harness) verify(t *testing.T, tok string) *auth.Claims {
	t.Helper()
	var c auth.Claims
	_, err := jwt.ParseWithClaims(tok, &c, func(*jwt.Token) (any, error) { return &h.key.PublicKey, nil },
		jwt.WithTimeFunc(func() time.Time { return testNow }), jwt.WithValidMethods([]string{"ES256"}))
	if err != nil {
		t.Fatalf("parse access token: %v", err)
	}
	return &c
}

func TestUsageAndArgumentErrors(t *testing.T) {
	h := newHarness(t)
	if err := h.run("help"); !errors.Is(err, flag.ErrHelp) || !strings.Contains(h.stderr.String(), "devicectl approve") {
		t.Fatalf("help: %v", err)
	}
	for _, args := range [][]string{
		{},
		{"bogus"},
		{"approve"},
		{"approve", "-device", "Bad_ID"},
		{"approve", "-device", "dev-0001", "extra"},
		{"list", "-status", "gone"},
		{"batch-import", "-device", "dev-0001"},
		{"access-token", "-device", "dev-0001", "-ttl", "-1h"},
	} {
		if err := h.run(args...); err == nil {
			t.Errorf("%v: no error", args)
		}
	}
	if h.store.opened != 0 {
		t.Fatalf("store opened %d times for invalid arguments", h.store.opened)
	}
}

func TestListAndStatusCommands(t *testing.T) {
	h := newHarness(t)
	lat, lon, fw := -6.2, 106.816666, "1.2\t3"
	h.store.devices["dev-0001"] = db.Device{DeviceID: "dev-0001", Status: db.DevicePending, Lat: &lat, Lon: &lon,
		Firmware: &fw, RegisteredAt: testNow, LastSeenAt: testNow.Add(time.Minute)}
	h.store.devices["dev-0002"] = db.Device{DeviceID: "dev-0002", Status: db.DeviceActive, RegisteredAt: testNow, LastSeenAt: testNow}
	h.store.families["dev-0002"] = db.RefreshFamily{CurrentHash: []byte("c"), NextHash: []byte("n")}

	if err := h.run("list"); err != nil {
		t.Fatal(err)
	}
	want := "DEVICE_ID\tSTATUS\tLAT\tLON\tFIRMWARE\tREGISTERED_AT\tLAST_SEEN_AT\n" +
		"dev-0001\tpending\t-6.2\t106.816666\t1.2 3\t2026-10-27T03:00:00Z\t2026-10-27T03:01:00Z\n" +
		"dev-0002\tactive\t-\t-\t-\t2026-10-27T03:00:00Z\t2026-10-27T03:00:00Z\n"
	if h.stdout.String() != want {
		t.Fatalf("list =\n%q\nwant\n%q", h.stdout.String(), want)
	}
	if err := h.run("list", "-status", "active"); err != nil || h.store.listed != db.DeviceActive {
		t.Fatalf("list -status: %v, listed %q", err, h.store.listed)
	}

	if err := h.run("approve", "-device", "dev-0001"); err != nil {
		t.Fatal(err)
	}
	if d := h.store.devices["dev-0001"]; d.Status != db.DeviceActive || d.ApprovedAt == nil || !d.ApprovedAt.Equal(testNow) {
		t.Fatalf("after approve: %+v", d)
	}
	if err := h.run("revoke", "-device", "dev-0002"); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.store.families["dev-0002"]; ok || h.store.devices["dev-0002"].Status != db.DeviceRevoked || len(h.store.retired) != 2 {
		t.Fatalf("revoke left family or status: %+v, retired %d", h.store.devices["dev-0002"], len(h.store.retired))
	}
	if err := h.run("reject", "-device", "dev-0001"); err != nil || h.store.devices["dev-0001"].Status != db.DeviceRejected {
		t.Fatalf("reject: %v", err)
	}
	if err := h.run("approve", "-device", "dev-9999"); err == nil || !strings.Contains(err.Error(), "has not registered") {
		t.Fatalf("approve unknown: %v", err)
	}
}

func TestIssue(t *testing.T) {
	h := newHarness(t)
	h.env["ACCESS_TOKEN_TTL"] = "2h"
	h.env["REFRESH_IDLE_TTL"] = "240h"
	h.env["JWT_ISSUER"] = "test-iss"
	if err := h.run("issue", "-device", "dev-0003"); err != nil {
		t.Fatal(err)
	}
	var res deviceapi.TokenResponse
	if err := json.Unmarshal(h.stdout.Bytes(), &res); err != nil {
		t.Fatalf("stdout %q: %v", h.stdout.String(), err)
	}
	if res.DeviceID != "dev-0003" || res.TokenType != "Bearer" || res.ExpiresIn != 7200 ||
		!auth.ValidSecret(res.RefreshToken) || res.RefreshExpiresAt != testNow.Add(240*time.Hour).Format(time.RFC3339) {
		t.Fatalf("token pair = %+v", res)
	}
	c := h.verify(t, res.AccessToken)
	if c.Subject != "dev-0003" || c.Issuer != "test-iss" || !c.ExpiresAt.Equal(testNow.Add(2*time.Hour)) {
		t.Fatalf("claims = %+v", c)
	}
	if h.store.devices["dev-0003"].Status != db.DeviceActive {
		t.Fatal("issue did not activate the device")
	}
	fam := h.store.families["dev-0003"]
	if !bytes.Equal(fam.CurrentHash, auth.HashSecret(res.RefreshToken)) {
		t.Fatal("family does not hold the printed refresh token")
	}
	// A second issue replaces the family.
	if err := h.run("issue", "-device", "dev-0003"); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(h.store.families["dev-0003"].CurrentHash, fam.CurrentHash) || len(h.store.retired) != 1 {
		t.Fatal("second issue did not replace the family")
	}

	delete(h.env, "JWT_SIGNING_KEY_FILE")
	if err := h.run("issue", "-device", "dev-0003"); err == nil || !strings.Contains(err.Error(), "JWT_SIGNING_KEY_FILE") {
		t.Fatalf("missing key file: %v", err)
	}
}

func TestAccessToken(t *testing.T) {
	h := newHarness(t)
	h.store.devices["dev-0004"] = db.Device{DeviceID: "dev-0004", Status: db.DevicePending}
	h.store.devices["dev-0006"] = db.Device{DeviceID: "dev-0006", Status: db.DeviceRevoked}
	if err := h.run("access-token", "-device", "dev-0006"); !errors.Is(err, deviceapi.ErrNotActive) {
		t.Fatalf("revoked device: %v", err)
	}
	if h.store.devices["dev-0006"].Status != db.DeviceRevoked {
		t.Fatal("access-token changed a revoked device")
	}
	// A fresh device is created and activated.
	if err := h.run("access-token", "-device", "dev-0005"); err != nil {
		t.Fatalf("fresh device: %v", err)
	}
	if h.store.devices["dev-0005"].Status != db.DeviceActive {
		t.Fatalf("dev-0005 not activated: %+v", h.store.devices["dev-0005"])
	}
	h.stdout.Reset()
	if err := h.run("access-token", "-device", "dev-0004", "-ttl", "24h"); err != nil {
		t.Fatal(err)
	}
	if h.store.devices["dev-0004"].Status != db.DeviceActive {
		t.Fatal("pending device not activated")
	}
	tok := strings.TrimSpace(h.stdout.String())
	c := h.verify(t, tok)
	if c.Subject != "dev-0004" || c.Issuer != defaultIssuer || !c.ExpiresAt.Equal(testNow.Add(24*time.Hour)) {
		t.Fatalf("claims = %+v", c)
	}
	h.env["ACCESS_TOKEN_TTL"] = "nope"
	if err := h.run("access-token", "-device", "dev-0004"); err == nil || !strings.Contains(err.Error(), "ACCESS_TOKEN_TTL") {
		t.Fatalf("bad ACCESS_TOKEN_TTL: %v", err)
	}
}
