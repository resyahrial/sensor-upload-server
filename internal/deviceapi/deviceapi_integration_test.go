//go:build integration

package deviceapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/resyahrial/sensor-upload-server/internal/db"
)

var testDevices *db.Devices

func TestMain(m *testing.M) {
	os.Exit(runWithPostgres(m))
}

func runWithPostgres(m *testing.M) int {
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "docker.io/library/postgres:16-alpine",
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
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres dsn:", err)
		return 1
	}
	openCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	pool, err := db.Open(openCtx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer pool.Close()
	if err := db.Migrate(openCtx, pool); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	testDevices = db.NewDevices(pool)
	return m.Run()
}

func TestSuitePostgres(t *testing.T) {
	runSuite(t, func(*testing.T) testStore { return testDevices })
}

// TestHTTPServerFlow drives the whole flow over a real HTTP server.
func TestHTTPServerFlow(t *testing.T) {
	e := newEnv(t, testDevices, 0)
	srv := httptest.NewServer(e.h)
	t.Cleanup(srv.Close)
	post := func(path string, body any) (*http.Response, []byte) {
		t.Helper()
		b, _ := json.Marshal(body)
		res, err := srv.Client().Post(srv.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(res.Body)
		return res, buf.Bytes()
	}
	dev, secret := e.newDevice(), newSecret(t)
	reg := map[string]any{"device_id": dev, "poll_secret": secret, "firmware": "2.0.0"}
	res, _ := post(PathRegister, reg)
	if res.StatusCode != http.StatusAccepted || res.Header.Get("Retry-After") != "3600" {
		t.Fatalf("register: %d Retry-After %q", res.StatusCode, res.Header.Get("Retry-After"))
	}
	e.setStatus(dev, db.DeviceActive)
	res, body := post(PathRegister, reg)
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("register after approve: %d %s", res.StatusCode, body)
	}
	var tr TokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		t.Fatal(err)
	}
	if sub, err := e.keys.Verifier.Verify(tr.AccessToken); err != nil || sub != dev {
		t.Fatalf("Verify = %q, %v", sub, err)
	}
	res, body = post(PathRefresh, RefreshRequest{RefreshToken: tr.RefreshToken})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("refresh: %d %s", res.StatusCode, body)
	}
	var tr2 TokenResponse
	if err := json.Unmarshal(body, &tr2); err != nil {
		t.Fatal(err)
	}
	if sub, err := e.keys.Verifier.Verify(tr2.AccessToken); err != nil || sub != dev {
		t.Fatalf("Verify refreshed = %q, %v", sub, err)
	}
	d, err := testDevices.Get(e.ctx(), dev)
	if err != nil {
		t.Fatal(err)
	}
	if d.PollSecretHash != nil || d.Firmware == nil || *d.Firmware != "2.0.0" {
		t.Fatalf("device after refresh = %+v", d)
	}
}

// TestConcurrentRefresh: parallel refreshes with the same token all
// succeed (it stays current) and leave exactly one live next.
func TestConcurrentRefresh(t *testing.T) {
	e := newEnv(t, testDevices, 0)
	dev, _, tr := e.active()
	const n = 8
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := range n {
		wg.Go(func() { codes[i] = e.refresh(tr.RefreshToken).Code })
	}
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusOK {
			t.Fatalf("refresh %d: status %d", i, c)
		}
	}
	e.tokens(e.refresh(tr.RefreshToken), dev)
}
