package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
)

const testChunk = 64 << 10

func writeFile(t *testing.T, dir, name string, size int) (string, []byte, string) {
	t.Helper()
	data := make([]byte, size)
	rng := rand.New(rand.NewChaCha8([32]byte{byte(size), byte(len(name))}))
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return p, data, hex.EncodeToString(sum[:])
}

func baseConfig(proto, url string) Config {
	return Config{
		Protocol: proto, URL: url, Token: "tok", Device: "dev-1", Priority: 2,
		Chunk: testChunk, Retries: 5, BackoffMax: 5 * time.Millisecond,
	}
}

// runOnce opens the outbox at state, runs cfg over paths and closes everything,
// like one process invocation.
func runOnce(t *testing.T, ctx context.Context, cfg Config, state string, paths ...string) []Result {
	t.Helper()
	ob, err := OpenOutbox(ctx, state)
	if err != nil {
		t.Fatalf("OpenOutbox: %v", err)
	}
	defer func() { _ = ob.Close() }()
	r, err := New(cfg, ob)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = r.Close() }()
	res, err := r.Run(ctx, paths)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return res
}

func outboxRow(t *testing.T, state, sha string) Row {
	t.Helper()
	ob, err := OpenOutbox(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ob.Close() }()
	row, err := ob.Get(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

// startTus serves f over TLS with HTTP/2 and returns the base URL and a
// trusting TLS config.
func startTus(t *testing.T, f *fakeTus) (string, *tls.Config) {
	t.Helper()
	ts := httptest.NewUnstartedServer(f)
	ts.EnableHTTP2 = true
	ts.Config.ErrorLog = nil
	ts.StartTLS()
	t.Cleanup(ts.Close)
	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	return ts.URL, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

func tusConfig(t *testing.T, f *fakeTus) Config {
	url, tlsCfg := startTus(t, f)
	cfg := baseConfig(ProtoREST, url)
	cfg.TLS = tlsCfg
	return cfg
}

func count(calls []string, prefix string) int {
	n := 0
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func expect(t *testing.T, res Result, want string) {
	t.Helper()
	if res.Result != want {
		t.Fatalf("result = %q (err %v), want %q", res.Result, res.Err, want)
	}
}

func TestTusHappyPathHTTP2(t *testing.T) {
	dir := t.TempDir()
	path, data, sha := writeFile(t, dir, "a.flac", 3*testChunk+123)
	f := newFakeTus("/rest/files/")
	cfg := tusConfig(t, f)
	var protos []int
	f.onPatch = func(_ int, _ *fakeUpload, _ http.ResponseWriter, r *http.Request) bool {
		protos = append(protos, r.ProtoMajor)
		return false
	}

	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "state.db"), path)[0]
	expect(t, res, ResultOK)
	if res.Attempts != 1 || res.Resumes != 0 || res.BytesSent != int64(len(data)) || res.BytesResent != 0 {
		t.Fatalf("counters = %+v", res)
	}
	if got, ok := f.stored(sha); !ok || !bytes.Equal(got, data) {
		t.Fatal("server does not hold the file")
	}
	if !slices.Equal(protos, []int{2, 2, 2, 2}) {
		t.Fatalf("PATCH protocols = %v, want HTTP/2 x4", protos)
	}
	if row := outboxRow(t, filepath.Join(dir, "state.db"), sha); row.Status != StatusDone || row.UploadRef == "" {
		t.Fatalf("outbox row = %+v", row)
	}

	// A second run finds the row done and sends nothing.
	res = runOnce(t, t.Context(), cfg, filepath.Join(dir, "state.db"), path)[0]
	expect(t, res, ResultOK)
	if n := count(f.callLog(), "POST"); n != 1 {
		t.Fatalf("POSTs = %d, want 1", n)
	}
}

func TestTusDropMidChunkResumesFromServerOffset(t *testing.T) {
	dir := t.TempDir()
	path, data, sha := writeFile(t, dir, "b.wav", 4*testChunk)
	f := newFakeTus("/rest/files/")
	f.onPatch = func(n int, u *fakeUpload, _ http.ResponseWriter, r *http.Request) bool {
		if n == 2 {
			abortMidBody(u, r)
		}
		return false
	}
	res := runOnce(t, t.Context(), tusConfig(t, f), filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	if res.Attempts != 2 || res.Resumes != 1 {
		t.Fatalf("attempts/resumes = %d/%d", res.Attempts, res.Resumes)
	}
	if res.BytesResent > testChunk || res.BytesSent != int64(len(data))+res.BytesResent {
		t.Fatalf("sent %d resent %d", res.BytesSent, res.BytesResent)
	}
	// The aborted PATCH is not logged (its handler panics): POST, PATCH,
	// then HEAD before the next PATCH.
	calls := f.callLog()
	if want := []string{"POST 201", "PATCH 204", "HEAD 200", "PATCH 204"}; !slices.Equal(calls[:4], want) {
		t.Fatalf("calls = %v, want prefix %v", calls, want)
	}
	if got, _ := f.stored(sha); !bytes.Equal(got, data) {
		t.Fatal("stored data differs")
	}
}

func TestTusConflictResyncsWithHead(t *testing.T) {
	dir := t.TempDir()
	path, _, _ := writeFile(t, dir, "c.jpg", 2*testChunk)
	f := newFakeTus("/rest/files/")
	f.onPatch = func(n int, _ *fakeUpload, w http.ResponseWriter, _ *http.Request) bool {
		if n == 2 {
			w.WriteHeader(http.StatusConflict)
			return true
		}
		return false
	}
	res := runOnce(t, t.Context(), tusConfig(t, f), filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	calls := f.callLog()
	if i := slices.Index(calls, "PATCH -1"); i < 0 || calls[i+1] != "HEAD 200" {
		t.Fatalf("409 not followed by HEAD: %v", calls)
	}
}

func TestTusAlreadyExistsIsDone(t *testing.T) {
	dir := t.TempDir()
	path, _, sha := writeFile(t, dir, "d.png", 1000)
	f := newFakeTus("/rest/files/")
	f.completed[sha] = true
	res := runOnce(t, t.Context(), tusConfig(t, f), filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultDuplicate)
	if res.Attempts != 1 || res.BytesSent != 0 {
		t.Fatalf("counters = %+v", res)
	}
}

func TestTusChecksumMismatchReuploads(t *testing.T) {
	dir := t.TempDir()
	path, data, _ := writeFile(t, dir, "e.ogg", 2*testChunk+7)
	f := newFakeTus("/rest/files/")
	f.corruptOnce = true
	res := runOnce(t, t.Context(), tusConfig(t, f), filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	if n := count(f.callLog(), "POST 201"); n != 2 {
		t.Fatalf("POSTs = %d, want 2", n)
	}
	if res.BytesSent != 2*int64(len(data)) || res.BytesResent != int64(len(data)) {
		t.Fatalf("sent %d resent %d", res.BytesSent, res.BytesResent)
	}
}

func TestTusPermanentFailure(t *testing.T) {
	dir := t.TempDir()
	path, _, sha := writeFile(t, dir, "f.mp4", 10)
	f := newFakeTus("/rest/files/")
	f.onCreate = func(w http.ResponseWriter, _ *http.Request) bool {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return true
	}
	state := filepath.Join(dir, "s.db")
	res := runOnce(t, t.Context(), tusConfig(t, f), state, path)[0]
	expect(t, res, ResultFailed)
	if res.Attempts != 1 || !errors.Is(res.Err, ErrPermanent) {
		t.Fatalf("attempts %d err %v", res.Attempts, res.Err)
	}
	if row := outboxRow(t, state, sha); row.Status != StatusFailed {
		t.Fatalf("status = %q", row.Status)
	}
}

func TestTusTransientRetriesThenExhausts(t *testing.T) {
	dir := t.TempDir()
	path, _, _ := writeFile(t, dir, "g.json", 100)
	f := newFakeTus("/rest/files/")
	fail := 2
	f.onPatch = func(n int, _ *fakeUpload, w http.ResponseWriter, _ *http.Request) bool {
		if n <= fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	}
	cfg := tusConfig(t, f)
	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	if res.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", res.Attempts)
	}

	path2, _, _ := writeFile(t, dir, "h.json", 101)
	fail = 1 << 30
	cfg.Retries = 2
	res = runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path2)[0]
	expect(t, res, ResultFailed)
	if res.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", res.Attempts)
	}
}

// TestTusKillAndResume simulates killing the client mid-chunk: the first run's
// context dies while a PATCH is in flight; a fresh run with the same SQLite
// state must HEAD the stored URL and re-send at most one chunk.
func TestTusKillAndResume(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "s.db")
	path, data, sha := writeFile(t, dir, "k.flac", 6*testChunk+11)
	f := newFakeTus("/rest/files/")
	cfg := tusConfig(t, f)

	ctx, kill := context.WithCancel(t.Context())
	f.onPatch = func(n int, u *fakeUpload, _ http.ResponseWriter, r *http.Request) bool {
		if n == 3 {
			kill()
			abortMidBody(u, r)
		}
		return false
	}
	res := runOnce(t, ctx, cfg, state, path)[0]
	expect(t, res, ResultInterrupted)
	if row := outboxRow(t, state, sha); row.Status != StatusUploading || row.UploadRef == "" {
		t.Fatalf("persisted row = %+v", row)
	}

	f.onPatch = nil
	res = runOnce(t, t.Context(), cfg, state, path)[0]
	expect(t, res, ResultOK)
	if res.BytesResent > testChunk {
		t.Fatalf("re-sent %d bytes, want <= one chunk (%d)", res.BytesResent, testChunk)
	}
	if res.Resumes != 1 || res.Attempts != 2 {
		t.Fatalf("resumes %d attempts %d", res.Resumes, res.Attempts)
	}
	calls := f.callLog()
	if n := count(calls, "POST"); n != 1 {
		t.Fatalf("POSTs = %d, want 1 (resume, not restart): %v", n, calls)
	}
	if got, _ := f.stored(sha); !bytes.Equal(got, data) {
		t.Fatal("stored data differs")
	}
}

func TestTusExpiredUploadStartsOver(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "s.db")
	path, _, _ := writeFile(t, dir, "x.wav", 3*testChunk)
	f := newFakeTus("/rest/files/")
	cfg := tusConfig(t, f)
	ctx, kill := context.WithCancel(t.Context())
	f.onPatch = func(n int, _ *fakeUpload, w http.ResponseWriter, _ *http.Request) bool {
		if n == 2 {
			kill()
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	}
	expect(t, runOnce(t, ctx, cfg, state, path)[0], ResultInterrupted)

	f.mu.Lock()
	f.uploads = map[string]*fakeUpload{} // server expired it
	f.onPatch = nil
	f.mu.Unlock()
	expect(t, runOnce(t, t.Context(), cfg, state, path)[0], ResultOK)
	calls := f.callLog()
	if !slices.Contains(calls, "HEAD 404") || count(calls, "POST 201") != 2 {
		t.Fatalf("calls = %v", calls)
	}
}

func TestTusDirConcurrency(t *testing.T) {
	dir := t.TempDir()
	f := newFakeTus("/rest/files/")
	cfg := tusConfig(t, f)
	cfg.Concurrency = 3
	var paths []string
	for i := range 5 {
		p, _, _ := writeFile(t, dir, string(rune('a'+i))+".jpg", 1000*(i+1))
		paths = append(paths, p)
	}
	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), paths...)
	for i, r := range res {
		expect(t, r, ResultOK)
		if abs, _ := filepath.Abs(paths[i]); r.File != abs {
			t.Fatalf("result %d is for %s", i, r.File)
		}
	}
}

// failingH3 fails every /rest-http3/ request at the connection level and
// forwards everything else.
type failingH3 struct{ next http.RoundTripper }

func (f failingH3) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.Contains(r.URL.Path, "/rest-http3/") {
		return nil, errors.New("quic handshake timeout")
	}
	return f.next.RoundTrip(r)
}

func TestHTTP3FallbackToRest(t *testing.T) {
	dir := t.TempDir()
	path, _, sha := writeFile(t, dir, "fb.flac", 2*testChunk)
	f := newFakeTus("/rest/files/")
	url, tlsCfg := startTus(t, f)
	cfg := baseConfig(ProtoRESTHTTP3, url)
	cfg.H3Fallback = true
	cfg.HTTPClient = &http.Client{Transport: failingH3{next: &http.Transport{TLSClientConfig: tlsCfg}}}
	state := filepath.Join(dir, "s.db")

	res := runOnce(t, t.Context(), cfg, state, path)[0]
	expect(t, res, ResultFallback)
	if res.Attempts != 4 {
		t.Fatalf("attempts = %d, want 3 QUIC failures + 1", res.Attempts)
	}
	if row := outboxRow(t, state, sha); row.Protocol != ProtoREST {
		t.Fatalf("outbox protocol = %q", row.Protocol)
	}

	// Without -h3-fallback the client keeps trying HTTP/3 and fails.
	path2, _, _ := writeFile(t, dir, "fb2.flac", 10)
	cfg.H3Fallback, cfg.Retries = false, 4
	expect(t, runOnce(t, t.Context(), cfg, state, path2)[0], ResultFailed)
}

func TestHTTP3RealTransport(t *testing.T) {
	dir := t.TempDir()
	path, data, sha := writeFile(t, dir, "h3.flac", 2*testChunk+5)
	f := newFakeTus("/rest-http3/files/")

	// Borrow httptest's self-signed certificate for the QUIC listener.
	ts := httptest.NewUnstartedServer(http.NotFoundHandler())
	ts.StartTLS()
	t.Cleanup(ts.Close)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http3.Server{Handler: f, TLSConfig: http3.ConfigureTLSConfig(ts.TLS.Clone())}
	go func() { _ = srv.Serve(pc) }()
	t.Cleanup(func() { _ = srv.Close() })

	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	cfg := baseConfig(ProtoRESTHTTP3, "https://"+pc.LocalAddr().String())
	cfg.TLS = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}

	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
	expect(t, res, ResultOK)
	if got, _ := f.stored(sha); !bytes.Equal(got, data) {
		t.Fatal("stored data differs")
	}
}

func grpcConfig(t *testing.T, f *fakeGRPC, proto string) Config {
	cfg := baseConfig(proto, "passthrough:///bufnet")
	cfg.GRPCDialOptions = startFakeGRPC(t, f)
	return cfg
}

func TestGRPCHappyPathHonoursMaxChunk(t *testing.T) {
	for _, proto := range []string{ProtoGRPC, ProtoGRPCNative} {
		t.Run(proto, func(t *testing.T) {
			dir := t.TempDir()
			path, data, sha := writeFile(t, dir, "g.flac", 5*testChunk/2)
			f := newFakeGRPC()
			f.maxChunk = testChunk / 2
			res := runOnce(t, t.Context(), grpcConfig(t, f, proto), filepath.Join(dir, "s.db"), path)[0]
			expect(t, res, ResultOK)
			if f.chunks != 5 {
				t.Fatalf("chunks = %d, want 5 (max_chunk_bytes honoured)", f.chunks)
			}
			if res.BytesSent != int64(len(data)) || res.Protocol != proto {
				t.Fatalf("result = %+v", res)
			}
			if got, _ := f.stored(sha); !bytes.Equal(got, data) {
				t.Fatal("stored data differs")
			}
		})
	}
}

func TestGRPCKillAndResume(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "s.db")
	path, data, sha := writeFile(t, dir, "k.flac", 6*testChunk+3)
	f := newFakeGRPC()
	cfg := grpcConfig(t, f, ProtoGRPC)

	ctx, kill := context.WithCancel(t.Context())
	f.onChunk = func(n int, _ *fakeGUpload, _ *uploadv1.Chunk) error {
		if n == 3 {
			kill()
			return status.Error(codes.Unavailable, "gone")
		}
		return nil
	}
	expect(t, runOnce(t, ctx, cfg, state, path)[0], ResultInterrupted)
	if row := outboxRow(t, state, sha); row.UploadRef == "" {
		t.Fatal("upload_id not persisted")
	}

	f.mu.Lock()
	f.onChunk = nil
	f.mu.Unlock()
	res := runOnce(t, t.Context(), cfg, state, path)[0]
	expect(t, res, ResultOK)
	// With the default window of one chunk, both the client's bytes_resent
	// and what the server received twice are bounded by one chunk (§15).
	if dup := f.received - int64(len(data)); dup > testChunk {
		t.Fatalf("server received %d duplicate bytes, want <= %d", dup, testChunk)
	}
	if res.BytesResent > testChunk {
		t.Fatalf("client re-sent %d bytes, want <= %d", res.BytesResent, testChunk)
	}
	t.Logf("client bytes_resent after kill: %d (chunk %d)", res.BytesResent, testChunk)
	if res.Resumes != 1 || f.inits != 2 {
		t.Fatalf("resumes %d inits %d", res.Resumes, f.inits)
	}
	if got, _ := f.stored(sha); !bytes.Equal(got, data) {
		t.Fatal("stored data differs")
	}
}

// TestGRPCWindowBoundsInFlight checks on the server side that the client
// never has more than -grpc-window chunks sent but unacked. The fake delays
// every ack, so a client that did not wait would run far ahead.
func TestGRPCWindowBoundsInFlight(t *testing.T) {
	for _, tc := range []struct{ window, ackEvery int }{{1, 1}, {4, 1}, {4, 4}} {
		t.Run(fmt.Sprintf("window%d_ackEvery%d", tc.window, tc.ackEvery), func(t *testing.T) {
			dir := t.TempDir()
			path, data, sha := writeFile(t, dir, "w.flac", 12*testChunk+5)
			f := newFakeGRPC()
			f.ackEvery, f.ackDelay = tc.ackEvery, 5*time.Millisecond
			cfg := grpcConfig(t, f, ProtoGRPC)
			cfg.GRPCWindow = tc.window
			res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
			expect(t, res, ResultOK)
			if res.Attempts != 1 || res.BytesResent != 0 || res.BytesSent != int64(len(data)) {
				t.Fatalf("result = %+v", res)
			}
			limit := int64(tc.window) * testChunk
			if f.maxUnacked > limit {
				t.Fatalf("server saw %d bytes unacked, window allows %d", f.maxUnacked, limit)
			}
			// window 4 must actually pipeline (more than one chunk ahead).
			if tc.window > 1 && f.maxUnacked <= testChunk {
				t.Fatalf("server saw at most %d bytes unacked; window %d did not pipeline", f.maxUnacked, tc.window)
			}
			if got, _ := f.stored(sha); !bytes.Equal(got, data) {
				t.Fatal("stored data differs")
			}
		})
	}
}

// TestGRPCWindowStallGuard: with -grpc-window below the server's ack
// interval the window never opens; the stall guard must end each stream with
// an error naming the flag, and the retry loop still makes progress.
func TestGRPCWindowStallGuard(t *testing.T) {
	dir := t.TempDir()
	path, data, sha := writeFile(t, dir, "s.flac", 6*testChunk)
	f := newFakeGRPC()
	f.ackEvery = 4
	cfg := grpcConfig(t, f, ProtoGRPC)
	cfg.GRPCWindow, cfg.GRPCAckTimeout = 2, 50*time.Millisecond

	// One attempt: the stall error itself.
	ob, err := OpenOutbox(t.Context(), filepath.Join(dir, "s1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ob.Close() }()
	r, err := New(cfg, ob)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	frow, err := r.enqueue(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = r.attempt(t.Context(), &frow)
	if err == nil || !strings.Contains(err.Error(), "-grpc-window") || errors.Is(err, ErrPermanent) {
		t.Fatalf("attempt error = %v, want transient stall error naming -grpc-window", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("stall guard took %v", d)
	}

	// Through the retry loop the upload still completes, a window per
	// stream (the fake commits chunks it never acked, like the server).
	cfg.Retries = 10
	res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s2.db"), path)[0]
	expect(t, res, ResultOK)
	// The direct attempt committed chunks 1–2; Init resumes there, the next
	// stream stalls after chunks 3–4, the last one completes with 5–6.
	if res.Attempts != 2 || res.Resumes != 2 {
		t.Fatalf("attempts %d resumes %d, want 2 and 2", res.Attempts, res.Resumes)
	}
	if got, _ := f.stored(sha); !bytes.Equal(got, data) {
		t.Fatal("stored data differs")
	}
}

func TestGRPCErrorHandling(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(f *fakeGRPC, sha string)
		want    string
		minInit int
	}{
		{"already exists", func(f *fakeGRPC, sha string) { f.completed[sha] = true }, ResultDuplicate, 1},
		{"data loss re-uploads", func(f *fakeGRPC, _ string) { f.corruptOnce = true }, ResultOK, 2},
		{"failed precondition re-syncs", func(f *fakeGRPC, _ string) {
			f.onChunk = func(n int, _ *fakeGUpload, _ *uploadv1.Chunk) error {
				if n == 2 {
					return status.Error(codes.FailedPrecondition, "expected offset 65536")
				}
				return nil
			}
		}, ResultOK, 2},
		{"not found re-inits", func(f *fakeGRPC, _ string) {
			f.onInit = func(n int) error {
				if n == 1 {
					return status.Error(codes.NotFound, "expired")
				}
				return nil
			}
		}, ResultOK, 2},
		{"unavailable backs off", func(f *fakeGRPC, _ string) {
			f.onInit = func(n int) error {
				if n <= 2 {
					return status.Error(codes.Unavailable, "down")
				}
				return nil
			}
		}, ResultOK, 3},
		{"invalid argument fails", func(f *fakeGRPC, _ string) {
			f.onInit = func(int) error { return status.Error(codes.InvalidArgument, "bad") }
		}, ResultFailed, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path, data, sha := writeFile(t, dir, "e.flac", 2*testChunk+1)
			f := newFakeGRPC()
			tc.setup(f, sha)
			res := runOnce(t, t.Context(), grpcConfig(t, f, ProtoGRPC), filepath.Join(dir, "s.db"), path)[0]
			expect(t, res, tc.want)
			if f.inits < tc.minInit {
				t.Fatalf("inits = %d, want >= %d", f.inits, tc.minInit)
			}
			if tc.want == ResultOK {
				if got, _ := f.stored(sha); !bytes.Equal(got, data) {
					t.Fatal("stored data differs")
				}
			}
		})
	}
}

// TestGRPCAllBytesCommitted covers Init returning committed_offset == size:
// the client must ask Status and only report done when it says complete.
func TestGRPCAllBytesCommitted(t *testing.T) {
	for _, complete := range []bool{false, true} {
		dir := t.TempDir()
		path, data, sha := writeFile(t, dir, "l.flac", 100)
		f := newFakeGRPC()
		f.skipDedup = true
		f.uploads["u9"] = &fakeGUpload{id: "u9", sha: sha, size: 100, data: data}
		f.bySHA[sha] = "u9"
		f.completed[sha] = complete
		cfg := grpcConfig(t, f, ProtoGRPC)
		cfg.Retries = 1
		res := runOnce(t, t.Context(), cfg, filepath.Join(dir, "s.db"), path)[0]
		want := ResultFailed
		if complete {
			want = ResultOK
		}
		expect(t, res, want)
		if res.BytesSent != 0 {
			t.Fatalf("sent %d bytes, want 0", res.BytesSent)
		}
	}
}

func TestReportFormat(t *testing.T) {
	var buf bytes.Buffer
	err := WriteReport(&buf, []Result{{
		Protocol: ProtoREST, File: "/x/a,b.flac", Size: 10, Chunk: 4, Duration: 1500 * time.Millisecond,
		Attempts: 2, Resumes: 1, BytesSent: 14, BytesResent: 4, Result: ResultOK,
	}})
	if err != nil {
		t.Fatal(err)
	}
	recs, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	wantHeader := []string{"protocol", "file", "size", "chunk", "duration_ms", "attempts", "resumes", "bytes_sent_total", "bytes_resent", "result"}
	if !slices.Equal(recs[0], wantHeader) {
		t.Fatalf("header = %v", recs[0])
	}
	if want := []string{"rest", "/x/a,b.flac", "10", "4", "1500", "2", "1", "14", "4", "ok"}; !slices.Equal(recs[1], want) {
		t.Fatalf("row = %v", recs[1])
	}
}

func TestBackoff(t *testing.T) {
	for step := range 20 {
		ceiling := min(time.Second<<step, 5*time.Minute)
		for range 50 {
			if d := backoff(step, 5*time.Minute); d < 0 || d > ceiling {
				t.Fatalf("backoff(%d) = %v, ceiling %v", step, d, ceiling)
			}
		}
	}
	if d := backoff(3, 0); d != 0 {
		t.Fatalf("zero cap: %v", d)
	}
}

func TestProgressCountsResent(t *testing.T) {
	p := &progress{hwm: 100}
	p.add(50, 100) // 50 resent, 50 new
	p.add(150, 10)
	p.add(0, 0)
	sent, resent, hwm, _ := p.snapshot()
	if sent != 110 || resent != 50 || hwm != 160 {
		t.Fatalf("sent %d resent %d hwm %d", sent, resent, hwm)
	}
}

func TestHelpers(t *testing.T) {
	for in, want := range map[string]string{
		"host:9090":               "host:9090",
		"https://host:8443":       "host:8443",
		"https://host":            "host:443",
		"passthrough:///bufnet":   "passthrough:///bufnet",
		"http://127.0.0.1:9090/x": "127.0.0.1:9090",
	} {
		if got, err := grpcTarget(in); err != nil || got != want {
			t.Errorf("grpcTarget(%q) = %q, %v", in, got, err)
		}
	}
	for in, want := range map[string]string{
		"a.JPG": "image/jpeg", "a.jpeg": "image/jpeg", "a.png": "image/png", "a.webp": "image/webp",
		"a.avif": "image/avif", "a.wav": "audio/wav", "a.flac": "audio/flac", "a.ogg": "audio/ogg",
		"a.opus": "audio/opus", "a.mp4": "video/mp4", "a.mov": "video/quicktime", "a.json": "application/json",
		"a.bin": "application/octet-stream",
	} {
		if got := contentType(in); got != want {
			t.Errorf("contentType(%q) = %q", in, got)
		}
	}
	for code, want := range map[int]error{404: ErrExpired, 410: ErrExpired, 409: ErrOffsetMismatch, 460: ErrChecksum,
		461: ErrAlreadyExists, 400: ErrPermanent, 401: ErrPermanent, 403: ErrPermanent, 413: ErrPermanent} {
		if err := statusErr("op", code); !errors.Is(err, want) {
			t.Errorf("statusErr(%d) = %v", code, err)
		}
	}
	if err := statusErr("op", 503); errors.Is(err, ErrPermanent) || err == nil {
		t.Errorf("503 = %v", err)
	}
}

func TestNewValidates(t *testing.T) {
	ok := baseConfig(ProtoREST, "https://h")
	bad := []func(c *Config){
		func(c *Config) { c.Protocol = "ftp" },
		func(c *Config) { c.URL = "" },
		func(c *Config) { c.Device = "" },
		func(c *Config) { c.Chunk = 0 },
		func(c *Config) { c.Priority = 5 },
	}
	for i, mut := range bad {
		c := ok
		mut(&c)
		if _, err := New(c, nil); err == nil {
			t.Errorf("case %d: want error", i)
		}
	}
	if _, err := New(ok, nil); err != nil {
		t.Fatal(err)
	}
}
