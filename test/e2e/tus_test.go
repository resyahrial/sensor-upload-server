//go:build integration

package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/client"
	"github.com/resyahrial/sensor-upload-server/internal/db"
)

// endpointCase is one tus endpoint over one HTTP version.
type endpointCase struct {
	name     string
	proto    string // H1, H2, H3
	base     string
	protocol string // DB protocol
	major    int    // expected response ProtoMajor
}

var endpoints = []endpointCase{
	{"rest-http1.1", H1, "/rest/files/", "rest", 1},
	{"rest-http2", H2, "/rest/files/", "rest", 2},
	{"rest-http3", H3, "/rest-http3/files/", "rest-http3", 3},
}

const chunk = 64 << 10

var deviceSeq = 0

// device returns a fresh device ID per call (rows never collide).
func device() string {
	deviceSeq++
	return fmt.Sprintf("e2e-%d-%d", time.Now().UnixNano()%1_000_000, deviceSeq)
}

func TestFullUpload(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, 3*chunk+123)
			res, body := c.Create(f, "")
			if res.StatusCode != http.StatusCreated {
				t.Fatalf("create: %d %q", res.StatusCode, body)
			}
			if res.ProtoMajor != ep.major {
				t.Errorf("served over HTTP/%d, want %d", res.ProtoMajor, ep.major)
			}
			loc := res.Header.Get("Location")
			if !strings.HasPrefix(loc, s.URL()+ep.base) {
				t.Errorf("Location = %q, want prefix %q", loc, s.URL()+ep.base)
			}
			last := c.SendFrom(loc, f, 0, chunk)
			if last.Header.Get("Upload-Offset") != strconv.Itoa(len(f.Data)) {
				t.Errorf("final Upload-Offset = %q", last.Header.Get("Upload-Offset"))
			}
			AssertStored(t, s, ep.protocol, loc, f)
			if off := c.Head(loc); off != len(f.Data) {
				t.Errorf("HEAD after completion = %d", off)
			}
		})
	}
}

func TestContractClient(t *testing.T) {
	s := StartServer(t, nil)
	for _, proto := range []string{client.ProtoREST, client.ProtoRESTHTTP3} {
		t.Run(proto, func(t *testing.T) {
			dev := device()
			f := NewFile(t, dev, 5*chunk+7)
			dir := t.TempDir()
			path := filepath.Join(dir, "sample.flac")
			if err := os.WriteFile(path, f.Data, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			ob, err := client.OpenOutbox(ctx, filepath.Join(dir, "outbox.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer ob.Close()
			r, err := client.New(client.Config{
				Protocol: proto, URL: s.URL(), Token: Token(t, dev), Device: dev, Priority: 2,
				Chunk: chunk, Retries: 3, BackoffMax: time.Second, TLS: TLSConfig(),
			}, ob)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			results, err := r.Run(ctx, []string{path})
			if err != nil {
				t.Fatal(err)
			}
			if got := results[0]; got.Result != client.ResultOK || got.BytesResent != 0 {
				t.Fatalf("result %+v", got)
			}
			var n int
			if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM uploads WHERE sha256 = $1 AND status = 'completed' AND protocol = $2`,
				f.SHA256, proto).Scan(&n); err != nil || n != 1 {
				t.Errorf("completed rows = %d (%v)", n, err)
			}
		})
	}
}

func TestOptionsAndExpires(t *testing.T) {
	s := StartServer(t, map[string]string{"INCOMPLETE_TTL": "48h"})
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			res, _ := c.MustDo(http.MethodOptions, c.Base, nil, nil)
			if res.StatusCode != http.StatusOK {
				t.Fatalf("OPTIONS: %d", res.StatusCode)
			}
			for h, want := range map[string]string{
				"Tus-Version":   "1.0.0",
				"Tus-Resumable": "1.0.0",
				"Tus-Extension": "creation,termination,expiration",
				"Tus-Max-Size":  strconv.Itoa(64 << 20),
			} {
				if got := res.Header.Get(h); got != want {
					t.Errorf("%s = %q, want %q", h, got, want)
				}
			}
			f := NewFile(t, dev, 1000)
			res, _ = c.Create(f, "")
			checkExpires(t, "POST", res, 48*time.Hour)
			loc := res.Header.Get("Location")
			res, _ = c.Patch(loc, 0, f.Data[:10])
			checkExpires(t, "PATCH", res, 48*time.Hour)
			res, _ = c.MustDo(http.MethodHead, loc, nil, nil)
			checkExpires(t, "HEAD", res, 48*time.Hour)
		})
	}
}

func checkExpires(t *testing.T, what string, res *http.Response, ttl time.Duration) {
	t.Helper()
	exp, err := http.ParseTime(res.Header.Get("Upload-Expires"))
	if err != nil {
		t.Errorf("%s: Upload-Expires %q: %v", what, res.Header.Get("Upload-Expires"), err)
		return
	}
	if d := time.Until(exp); d < ttl-time.Minute || d > ttl+time.Minute {
		t.Errorf("%s: Upload-Expires %v is %v away, want ~%v", what, exp, d, ttl)
	}
}

// inflight is a PATCH whose body is fed through a pipe.
type inflight struct {
	pw     *io.PipeWriter
	cancel context.CancelFunc
	done   chan inflightResult
}

type inflightResult struct {
	status int
	body   string
	err    error
}

// StartPatch begins a PATCH at off announcing total body bytes, sending
// the first part immediately.
func (c *Tus) StartPatch(url string, off int, data []byte, first int) *inflight {
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	p := &inflight{pw: pw, cancel: cancel, done: make(chan inflightResult, 1)}
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, pr)
		if err != nil {
			p.done <- inflightResult{err: err}
			return
		}
		req.ContentLength = int64(len(data))
		req.Header.Set("Tus-Resumable", "1.0.0")
		req.Header.Set("Authorization", "Bearer "+c.Token)
		for k, v := range patchHeaders(off) {
			req.Header.Set(k, v)
		}
		res, err := c.HC.Do(req)
		if err != nil {
			p.done <- inflightResult{err: err}
			return
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		p.done <- inflightResult{status: res.StatusCode, body: string(b)}
	}()
	go func() { _, _ = pw.Write(data[:first]) }()
	return p
}

func (p *inflight) wait(t *testing.T) inflightResult {
	t.Helper()
	select {
	case r := <-p.done:
		return r
	case <-time.After(30 * time.Second):
		t.Fatal("in-flight PATCH did not finish")
		return inflightResult{}
	}
}

func TestDisconnectResume(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, 4*chunk)
			loc := c.MustCreate(f)
			half := 2 * chunk
			p := c.StartPatch(loc, 0, f.Data, half)
			WaitStaged(t, s, loc, half)
			p.cancel() // client drops the connection mid-chunk
			_ = p.pw.CloseWithError(context.Canceled)
			if r := p.wait(t); r.err == nil {
				t.Logf("interrupted PATCH answered %d", r.status)
			}

			off := c.Head(loc)
			if off != half {
				t.Fatalf("HEAD after disconnect = %d, want %d", off, half)
			}
			c.SendFrom(loc, f, off, chunk)
			AssertStored(t, s, ep.protocol, loc, f)
			if u := Row(t, s, ep.protocol, loc); u.ResumeCount < 1 {
				t.Errorf("resume_count = %d", u.ResumeCount)
			}
		})
	}
}

// TestServerRestartResume stops the server during a PATCH (NFR-1),
// leaves a stale lock owned by this PID (as a crash would), restarts on
// the same port and resumes from the HEAD offset.
func TestServerRestartResume(t *testing.T) {
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			s := StartServer(t, nil)
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, 4*chunk)
			loc := c.MustCreate(f)
			c.SendFrom(loc, NewFileFrom(f, chunk), 0, chunk) // first chunk acked
			p := c.StartPatch(loc, chunk, f.Data[chunk:], chunk)
			WaitStaged(t, s, loc, 2*chunk)

			s.Stop()
			r := p.wait(t)
			_ = p.pw.Close()
			if r.err == nil && r.status != http.StatusServiceUnavailable {
				t.Errorf("PATCH during shutdown = %d %q, want 503", r.status, r.body)
			}
			id := refOf(loc)
			for _, name := range []string{id + ".lock", id + ".stop"} {
				if err := os.WriteFile(filepath.Join(s.TusDir(), name), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			s.Restart()

			c = Endpoint(t, s, ep.proto, ep.base, dev) // fresh connections
			off := c.Head(loc)
			if off != 2*chunk {
				t.Fatalf("HEAD after restart = %d, want %d", off, 2*chunk)
			}
			c.SendFrom(loc, f, off, chunk)
			AssertStored(t, s, ep.protocol, loc, f)
		})
	}
}

// NewFileFrom returns f truncated to n bytes (same metadata).
func NewFileFrom(f File, n int) File { f.Data = f.Data[:n]; return f }

func TestChecksumMismatch(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, 2*chunk)
			other := NewFile(t, dev, 10)
			res, _ := c.Create(f, other.SHA256)
			loc := res.Header.Get("Location")
			c.SendFrom(loc, NewFileFrom(f, chunk), 0, chunk)
			res, body := c.Patch(loc, chunk, f.Data[chunk:])
			if res.StatusCode != 460 {
				t.Fatalf("final PATCH: %d %q, want 460", res.StatusCode, body)
			}
			if u := Row(t, s, ep.protocol, loc); u.Status != db.StatusFailedChecksum {
				t.Errorf("status = %s", u.Status)
			}
			AssertNotStaged(t, s, loc)
			if _, err := os.Stat(filepath.Join(s.env["OBJSTORE_LOCAL_DIR"], filepath.FromSlash(other.FinalKey(t)))); !os.IsNotExist(err) {
				t.Errorf("object written for a corrupt upload (%v)", err)
			}
			if _, err := os.Stat(filepath.Join(s.env["OBJSTORE_LOCAL_DIR"], filepath.FromSlash(f.FinalKey(t)))); !os.IsNotExist(err) {
				t.Errorf("object written for a corrupt upload (%v)", err)
			}
			if off := c.Head(loc); off != -460 {
				t.Errorf("HEAD after mismatch = %d, want 460", -off)
			}
		})
	}
}

func TestDuplicate(t *testing.T) {
	s := StartServer(t, nil)
	dev := device()
	f := NewFile(t, dev, chunk)
	first := Endpoint(t, s, H2, "/rest/files/", dev)
	loc := first.MustCreate(f)
	first.SendFrom(loc, f, 0, chunk)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			res, body := c.Create(f, "")
			if res.StatusCode != 461 || body != "already exists" {
				t.Errorf("create duplicate: %d %q, want 461 \"already exists\"", res.StatusCode, body)
			}
		})
	}
	// Race: two in-progress uploads of the same file; the second to
	// finish gets 461 from the last PATCH.
	g := NewFile(t, dev, chunk)
	a := Endpoint(t, s, H2, "/rest/files/", dev)
	b := Endpoint(t, s, H3, "/rest-http3/files/", dev)
	la, lb := a.MustCreate(g), b.MustCreate(g)
	a.SendFrom(la, g, 0, chunk)
	res, body := b.Patch(lb, 0, g.Data)
	if res.StatusCode != 461 {
		t.Errorf("losing final PATCH: %d %q, want 461", res.StatusCode, body)
	}
	if u := Row(t, s, "rest-http3", lb); u.Status != db.StatusCancelled {
		t.Errorf("loser status = %s", u.Status)
	}
}

// TestShortCircuitGuard: the object store fails while the last PATCH is
// finalized. tusd itself would answer a retried PATCH (or the client's
// HEAD) as complete; the server must keep failing until finalization
// really succeeds.
func TestShortCircuitGuard(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		for _, retry := range []string{http.MethodPatch, http.MethodHead} {
			t.Run(ep.name+"/"+retry, func(t *testing.T) {
				dev := device()
				c := Endpoint(t, s, ep.proto, ep.base, dev)
				f := NewFile(t, dev, chunk)
				loc := c.MustCreate(f)
				s.Store.FailPuts(2)
				res, body := c.Patch(loc, 0, f.Data)
				if res.StatusCode != http.StatusInternalServerError || strings.Contains(body, "injected") {
					t.Fatalf("final PATCH with failing store: %d %q", res.StatusCode, body)
				}
				retryOnce := func() *http.Response {
					if retry == http.MethodHead {
						r, _ := c.MustDo(http.MethodHead, loc, nil, nil)
						return r
					}
					r, _ := c.Patch(loc, len(f.Data), nil)
					return r
				}
				if r := retryOnce(); r.StatusCode != http.StatusInternalServerError {
					t.Fatalf("retry while store fails: %d, want 500", r.StatusCode)
				}
				if u := Row(t, s, ep.protocol, loc); u.Status != db.StatusUploading {
					t.Fatalf("status after failures = %s", u.Status)
				}
				r := retryOnce()
				want := map[string]int{http.MethodHead: http.StatusOK, http.MethodPatch: http.StatusNoContent}[retry]
				if r.StatusCode != want || r.Header.Get("Upload-Offset") != strconv.Itoa(len(f.Data)) {
					t.Fatalf("retry after recovery: %d offset %q", r.StatusCode, r.Header.Get("Upload-Offset"))
				}
				AssertStored(t, s, ep.protocol, loc, f)
			})
		}
	}
}

// badToken is a rejected access token and the error code it must get:
// the REST {"error":code} body and the gRPC UNAUTHENTICATED message.
type badToken struct {
	token string
	code  string
}

// badTokens returns rejected tokens for dev.
func badTokens(t *testing.T, dev string) map[string]badToken {
	t.Helper()
	valid := Token(t, dev)
	otherKey, err := auth.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	other, err := auth.NewIssuer(auth.IssuerConfig{Key: otherKey, Issuer: JWTIssuer, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := other.Issue(dev)
	if err != nil {
		t.Fatal(err)
	}
	wrongIss, err := auth.NewIssuer(auth.IssuerConfig{Key: SigningKey, Issuer: "someone-else", TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	wrongIssTok, _, err := wrongIss.Issue(dev)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]badToken{
		"missing":            {"", "invalid_token"},
		"garbage":            {"not-a-jwt", "invalid_token"},
		"other-signing-key":  {foreign, "invalid_token"},
		"tampered-signature": {valid[:len(valid)-4] + "AAAA", "invalid_token"},
		"wrong-issuer":       {wrongIssTok, "invalid_token"},
		"hs256":              {"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln", "invalid_token"},
		"expired":            {ExpiredToken(t, dev), "token_expired"},
	}
}

func TestAuthFailures(t *testing.T) {
	s := StartServer(t, nil)
	dev := device()
	f := NewFile(t, dev, 100)
	owner := Endpoint(t, s, H2, "/rest/files/", dev)
	loc := owner.MustCreate(f)
	bad := badTokens(t, dev)
	for _, ep := range endpoints {
		for name, bt := range bad {
			t.Run(ep.name+"/"+name, func(t *testing.T) {
				c := Endpoint(t, s, ep.proto, ep.base, dev)
				c.Token = bt.token
				check := func(m string, res *http.Response, body string) {
					t.Helper()
					if res.StatusCode != http.StatusUnauthorized {
						t.Errorf("%s: %d", m, res.StatusCode)
					}
					if got := res.Header.Get("WWW-Authenticate"); got != `Bearer error="invalid_token", error_description="`+bt.code+`"` {
						t.Errorf("%s: WWW-Authenticate %q", m, got)
					}
					if m == http.MethodHead {
						return
					}
					if ct := res.Header.Get("Content-Type"); ct != "application/json" {
						t.Errorf("%s: Content-Type %q", m, ct)
					}
					if want := `{"error":"` + bt.code + `"}`; body != want {
						t.Errorf("%s: body %q, want %q", m, body, want)
					}
				}
				res, body := c.Create(f, "")
				check(http.MethodPost, res, body)
				target := c.Base + refOf(loc)
				for _, m := range []string{http.MethodHead, http.MethodPatch, http.MethodDelete, http.MethodOptions} {
					var data []byte
					if m == http.MethodPatch {
						data = f.Data
					}
					res, body := c.MustDo(m, target, data, patchHeaders(0))
					check(m, res, body)
				}
			})
		}
	}
	if off := owner.Head(loc); off != 0 {
		t.Errorf("owner HEAD = %d", off)
	}
}

func TestCrossDeviceAndBasePath(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			owner := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, 2*chunk)
			loc := owner.MustCreate(f)
			owner.SendFrom(loc, NewFileFrom(f, chunk), 0, chunk)

			other := Endpoint(t, s, ep.proto, ep.base, device())
			for _, m := range []string{http.MethodHead, http.MethodPatch, http.MethodDelete} {
				if res, _ := other.MustDo(m, loc, f.Data[chunk:], patchHeaders(chunk)); res.StatusCode != http.StatusNotFound {
					t.Errorf("other device %s: %d, want 404", m, res.StatusCode)
				}
			}
			// The other endpoint does not know this upload.
			wrong := map[string]endpointCase{"/rest/files/": endpoints[2], "/rest-http3/files/": endpoints[1]}[ep.base]
			oc := Endpoint(t, s, wrong.proto, wrong.base, dev)
			for _, m := range []string{http.MethodHead, http.MethodPatch, http.MethodDelete} {
				if res, _ := oc.MustDo(m, oc.Base+refOf(loc), f.Data[chunk:], patchHeaders(chunk)); res.StatusCode != http.StatusNotFound {
					t.Errorf("wrong base path %s: %d, want 404", m, res.StatusCode)
				}
			}
			// Untouched: the owner can still finish.
			if off := owner.Head(loc); off != chunk {
				t.Fatalf("owner HEAD = %d", off)
			}
			owner.SendFrom(loc, f, chunk, chunk)
			AssertStored(t, s, ep.protocol, loc, f)
		})
	}
}

func TestTCPRejectsHTTP3PathAndAdvertisesAltSvc(t *testing.T) {
	s := StartServer(t, nil)
	dev := device()
	for _, proto := range []string{H1, H2} {
		c := Endpoint(t, s, proto, "/rest-http3/files/", dev)
		f := NewFile(t, dev, 10)
		res, _ := c.Create(f, "")
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("%s POST /rest-http3/files/ on TCP: %d, want 404", proto, res.StatusCode)
		}
		if res.Header.Get("Alt-Svc") != `h3=":`+s.port+`"; ma=86400` {
			t.Errorf("%s Alt-Svc on 404 = %q", proto, res.Header.Get("Alt-Svc"))
		}
		rc := Endpoint(t, s, proto, "/rest/files/", dev)
		res, _ = rc.MustDo(http.MethodOptions, rc.Base, nil, nil)
		if !strings.HasPrefix(res.Header.Get("Alt-Svc"), `h3=":`+s.port+`"`) {
			t.Errorf("%s Alt-Svc on tus = %q", proto, res.Header.Get("Alt-Svc"))
		}
	}
	// HTTP/3 serves only /rest-http3/files/.
	h3 := Endpoint(t, s, H3, "/rest/files/", dev)
	if res, _ := h3.Create(NewFile(t, dev, 10), ""); res.StatusCode != http.StatusNotFound {
		t.Errorf("HTTP/3 POST /rest/files/: %d, want 404", res.StatusCode)
	}
}

func TestDeleteCancels(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, 2*chunk)
			loc := c.MustCreate(f)
			c.SendFrom(loc, NewFileFrom(f, chunk), 0, chunk)
			if res, body := c.MustDo(http.MethodDelete, loc, nil, nil); res.StatusCode != http.StatusNoContent {
				t.Fatalf("DELETE: %d %q", res.StatusCode, body)
			}
			if u := Row(t, s, ep.protocol, loc); u.Status != db.StatusCancelled {
				t.Errorf("status = %s", u.Status)
			}
			AssertNotStaged(t, s, loc)
			if off := c.Head(loc); off != -404 {
				t.Errorf("HEAD after DELETE = %d, want 404", -off)
			}
		})
	}
}

// TestConcurrentPatch documents tusd's lock hand-off: a second request on
// a locked upload does not get 423 (requirements §7); it asks the holder
// to release the lock, the first PATCH ends with 400
// ERR_UPLOAD_INTERRUPTED (its bytes are kept), and the second one then
// runs against the new offset (409 here, as its offset is stale).
func TestConcurrentPatch(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, 4*chunk)
			loc := c.MustCreate(f)
			p := c.StartPatch(loc, 0, f.Data, chunk)
			WaitStaged(t, s, loc, chunk)

			c2 := Endpoint(t, s, ep.proto, ep.base, dev)
			res, body := c2.Patch(loc, 0, f.Data[:chunk])
			if res.StatusCode != http.StatusConflict {
				t.Errorf("second PATCH: %d %q, want 409 after lock hand-off", res.StatusCode, body)
			}
			r := p.wait(t)
			_ = p.pw.Close()
			if r.err != nil || r.status != http.StatusBadRequest || !strings.Contains(r.body, "ERR_UPLOAD_INTERRUPTED") {
				t.Errorf("first PATCH: %d %q %v, want 400 ERR_UPLOAD_INTERRUPTED", r.status, r.body, r.err)
			}
			off := c.Head(loc)
			if off != chunk {
				t.Fatalf("HEAD = %d, want %d (interrupted bytes kept)", off, chunk)
			}
			c.SendFrom(loc, f, off, chunk)
			AssertStored(t, s, ep.protocol, loc, f)
		})
	}
}

func TestExpiredUpload(t *testing.T) {
	s := StartServer(t, nil)
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			dev := device()
			c := Endpoint(t, s, ep.proto, ep.base, dev)
			f := NewFile(t, dev, 2*chunk)
			loc := c.MustCreate(f)
			c.SendFrom(loc, NewFileFrom(f, chunk), 0, chunk)
			if err := s.Repo.SetStatus(context.Background(), ep.protocol, refOf(loc), db.StatusExpired); err != nil {
				t.Fatal(err)
			}
			if off := c.Head(loc); off != -http.StatusGone {
				t.Errorf("HEAD on expired: %d, want 410", -off)
			}
			if res, _ := c.Patch(loc, chunk, f.Data[chunk:]); res.StatusCode != http.StatusGone {
				t.Errorf("PATCH on expired: %d, want 410", res.StatusCode)
			}
			if res, _ := c.MustDo(http.MethodDelete, loc, nil, nil); res.StatusCode != http.StatusGone {
				t.Errorf("DELETE on expired: %d, want 410", res.StatusCode)
			}
		})
	}
}
