//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/client"
	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/deviceapi"
)

// devicesDB returns the device store on s's database.
func devicesDB(t *testing.T, s *Server) *db.Devices {
	t.Helper()
	pool, err := db.Open(context.Background(), s.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return db.NewDevices(pool)
}

// postJSON POSTs body to s's device API path and decodes the reply into out
// (when non-nil); it returns the status and the "error" member.
func postJSON(t *testing.T, s *Server, path string, body, out any) (int, string) {
	t.Helper()
	b, _ := json.Marshal(body)
	res, err := NewClient(t, H2).Post(s.URL()+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var raw json.RawMessage
	_ = json.NewDecoder(res.Body).Decode(&raw)
	var e struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &e)
	if out != nil && res.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s: decode %s: %v", path, raw, err)
		}
	}
	return res.StatusCode, e.Error
}

// uploadWith uploads a fresh file for dev over /rest with token and checks
// it is stored.
func uploadWith(t *testing.T, s *Server, dev, token string) {
	t.Helper()
	c := Endpoint(t, s, H2, "/rest/files/", dev)
	c.Token = token
	f := NewFile(t, dev, 3000)
	loc := c.MustCreate(f)
	c.SendFrom(loc, f, 0, 1024)
	AssertStored(t, s, "rest", loc, f)
}

// TestDeviceLifecycle walks one device through register → approve →
// collect → upload → refresh (current, then next) → reuse → revoke.
func TestDeviceLifecycle(t *testing.T) {
	s := StartServer(t, nil)
	devs := devicesDB(t, s)
	ctx := context.Background()
	dev := device()
	secret, _ := auth.GenerateSecret()
	reg := map[string]any{"device_id": dev, "poll_secret": secret, "lat": -1.23, "lon": 113.4, "firmware": "1.4.0"}

	if code, e := postJSON(t, s, "/v1/devices/register", reg, nil); code != http.StatusForbidden || e != "device_not_allowed" {
		t.Fatalf("register before batch import: %d %q", code, e)
	}
	if _, err := devs.ImportBatch(ctx, []string{dev}); err != nil {
		t.Fatal(err)
	}
	if code, _ := postJSON(t, s, "/v1/devices/register", reg, nil); code != http.StatusAccepted {
		t.Fatalf("first register: %d, want 202", code)
	}
	other, _ := auth.GenerateSecret()
	stolen := map[string]any{"device_id": dev, "poll_secret": other}
	if code, e := postJSON(t, s, "/v1/devices/register", stolen, nil); code != http.StatusConflict || e != "poll_secret_mismatch" {
		t.Fatalf("register with another poll secret: %d %q", code, e)
	}
	if err := devs.SetStatus(ctx, dev, db.DeviceActive, time.Now()); err != nil {
		t.Fatal(err)
	}

	// A lost 200 can be collected again with the same poll secret.
	var lost, p1 deviceapi.TokenResponse
	for _, out := range []*deviceapi.TokenResponse{&lost, &p1} {
		if code, e := postJSON(t, s, "/v1/devices/register", reg, out); code != http.StatusOK {
			t.Fatalf("collect: %d %q", code, e)
		}
	}
	if p1.DeviceID != dev || p1.TokenType != "Bearer" || p1.ExpiresIn != 3600 || !auth.ValidSecret(p1.RefreshToken) {
		t.Fatalf("token pair: %+v", p1)
	}
	uploadWith(t, s, dev, p1.AccessToken)

	refresh := func(tok string, out *deviceapi.TokenResponse) (int, string) {
		return postJSON(t, s, "/v1/devices/token/refresh", map[string]string{"refresh_token": tok}, out)
	}
	// Refreshing with current twice (lost response) is fine; using the
	// second result (next) retires p1's token.
	var p2, p3, p4 deviceapi.TokenResponse
	for _, c := range []struct {
		from *string
		out  *deviceapi.TokenResponse
	}{{&p1.RefreshToken, &p2}, {&p1.RefreshToken, &p3}, {&p3.RefreshToken, &p4}} {
		if code, e := refresh(*c.from, c.out); code != http.StatusOK {
			t.Fatalf("refresh: %d %q", code, e)
		}
	}
	uploadWith(t, s, dev, p4.AccessToken)
	for name, tok := range map[string]string{"retired current": p1.RefreshToken, "replaced next": p2.RefreshToken} {
		if code, e := refresh(tok, nil); code != http.StatusUnauthorized || e != "invalid_grant" {
			t.Fatalf("%s: %d %q, want 401 invalid_grant", name, code, e)
		}
	}
	// The pair was used: the poll secret no longer collects tokens.
	if code, e := postJSON(t, s, "/v1/devices/register", reg, nil); code != http.StatusConflict {
		t.Fatalf("register after first refresh: %d %q, want 409", code, e)
	}

	if err := devs.SetStatus(ctx, dev, db.DeviceRevoked, time.Now()); err != nil {
		t.Fatal(err)
	}
	if code, e := refresh(p4.RefreshToken, nil); code != http.StatusForbidden || e != "device_not_active" {
		t.Fatalf("refresh after revoke: %d %q", code, e)
	}
	// Stateless verification: the access token works until it expires.
	uploadWith(t, s, dev, p4.AccessToken)
}

// TestUploaderRefreshes runs the uploader client with a refresh token file
// against a server issuing 3 s access tokens: the second batch, started
// after 80% of expires_in, must refresh and rewrite the file.
func TestUploaderRefreshes(t *testing.T) {
	s := StartServer(t, map[string]string{"ACCESS_TOKEN_TTL": "3s"})
	dev := device()
	devs := devicesDB(t, s)
	if _, err := devs.ImportBatch(context.Background(), []string{dev}); err != nil {
		t.Fatal(err)
	}
	pair, err := deviceapi.Issue(context.Background(), deviceapi.IssueConfig{
		Store: devs, Issuer: mustIssuer(t, 3*time.Second),
		IdleTTL: time.Hour, MaxAge: 24 * time.Hour, Now: time.Now(),
	}, dev)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "token.json")
	b, _ := json.Marshal(pair)
	if err := os.WriteFile(file, b, 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := client.NewRefreshingSource(client.RefreshConfig{File: file, URL: s.URL(), TLS: TLSConfig()})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	ob, err := client.OpenOutbox(context.Background(), filepath.Join(dir, "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ob.Close() }()
	r, err := client.New(client.Config{
		Protocol: client.ProtoREST, URL: s.URL(), Device: dev, Priority: 2, Chunk: 1024,
		Retries: 3, BackoffMax: time.Second, Concurrency: 1, TLS: TLSConfig(), Tokens: src,
	}, ob)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	run := func(name string) {
		t.Helper()
		f := NewFile(t, dev, 5000)
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, f.Data, 0o600); err != nil {
			t.Fatal(err)
		}
		res, err := r.Run(context.Background(), []string{p})
		if err != nil || len(res) != 1 || res[0].Result != client.ResultOK {
			t.Fatalf("run %s: %+v, %v", name, res, err)
		}
	}
	run("a.jpg")
	time.Sleep(2600 * time.Millisecond) // > 80% of 3 s
	run("b.jpg")

	var saved deviceapi.TokenResponse
	b, _ = os.ReadFile(file)
	if err := json.Unmarshal(b, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.RefreshToken == pair.RefreshToken || saved.AccessToken == pair.AccessToken {
		t.Fatal("token file was not rewritten by a refresh")
	}
	if st, _ := os.Stat(file); st.Mode().Perm() != 0o600 {
		t.Fatalf("token file mode %v", st.Mode().Perm())
	}
}

// mustIssuer returns an Issuer with the harness signing key and ttl.
func mustIssuer(t *testing.T, ttl time.Duration) *auth.Issuer {
	t.Helper()
	iss, err := auth.NewIssuer(auth.IssuerConfig{Key: SigningKey, Issuer: JWTIssuer, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return iss
}
