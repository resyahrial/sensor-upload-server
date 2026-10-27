//go:build remote

package remote

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/client"
	"github.com/resyahrial/sensor-upload-server/internal/fileapi"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

const (
	// fileSize is small enough for slow links, larger than a few chunks.
	fileSize  = 300<<10 + 123
	chunkSize = 64 << 10
	// reqTimeout bounds one request (contract §5.2: ~60 s per 256 KiB).
	reqTimeout = 60 * time.Second
)

// File is fresh random content with its sha256.
type File struct {
	Data   []byte
	SHA256 string
}

func newFile(t *testing.T, size int) File {
	t.Helper()
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return File{Data: b, SHA256: hex.EncodeToString(s[:])}
}

// randSHA returns a random 64-hex digest (no file has it).
func randSHA(t *testing.T) string { return newFile(t, 32).SHA256 }

// writeFile stores f in a temp dir as remote-<prefix>.flac.
func writeFile(t *testing.T, f File) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "remote-"+f.SHA256[:12]+".flac")
	if err := os.WriteFile(p, f.Data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*reqTimeout)
	t.Cleanup(cancel)
	return ctx
}

// --- HTTP ---------------------------------------------------------------

// httpClient speaks HTTP/2 (TCP) or HTTP/3 (QUIC) to REMOTE_URL.
func (e *env) httpClient(t *testing.T, h3 bool) *http.Client {
	t.Helper()
	if h3 {
		tr := &http3.Transport{TLSClientConfig: e.TLS.Clone()}
		t.Cleanup(func() { _ = tr.Close() })
		return &http.Client{Transport: tr, Timeout: reqTimeout}
	}
	tr := &http.Transport{
		TLSClientConfig:     e.TLS.Clone(),
		ForceAttemptHTTP2:   true,
		DialContext:         (&net.Dialer{Timeout: 15 * time.Second}).DialContext,
		TLSHandshakeTimeout: 15 * time.Second,
	}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: reqTimeout}
}

// lookup GETs /files/sha256/{sha} (no token) over HTTP/2.
func (e *env) lookup(t *testing.T, sha string) (int, fileapi.Response) {
	t.Helper()
	res, err := e.httpClient(t, false).Get(e.URL + fileapi.Prefix + sha)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if cc := res.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("lookup: Cache-Control %q, want no-store", cc)
	}
	var body fileapi.Response
	_ = json.Unmarshal(b, &body) // a 400 body has no fields of Response
	return res.StatusCode, body
}

// assertSaved checks that the server reports f as completed.
func (e *env) assertSaved(t *testing.T, f File) {
	t.Helper()
	code, body := e.lookup(t, f.SHA256)
	if code != http.StatusOK || body.Status != fileapi.StatusCompleted {
		t.Fatalf("lookup %s: %d %+v, want 200 completed", f.SHA256, code, body)
	}
	if body.SHA256 != f.SHA256 || body.Size != int64(len(f.Data)) {
		t.Errorf("lookup: %+v, want sha256 %s size %d", body, f.SHA256, len(f.Data))
	}
	if _, err := time.Parse(time.RFC3339, body.CompletedAt); err != nil {
		t.Errorf("lookup: completed_at %q: %v", body.CompletedAt, err)
	}
}

// tus is a raw tus client on one base path.
type tus struct {
	t     *testing.T
	hc    *http.Client
	base  string // https://host:port/rest/files/
	token string
}

func (e *env) tus(t *testing.T, h3 bool, token string) *tus {
	base := e.URL + "/rest/files/"
	if h3 {
		base = e.URL + "/rest-http3/files/"
	}
	return &tus{t: t, hc: e.httpClient(t, h3), base: base, token: token}
}

func (c *tus) do(ctx context.Context, method, url string, body []byte, hdr map[string]string) (*http.Response, string, error) {
	var r io.Reader = http.NoBody
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, r)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Tus-Resumable", "1.0.0")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
	return res, strings.TrimSpace(string(b)), nil
}

func (c *tus) mustDo(method, url string, body []byte, hdr map[string]string) (*http.Response, string) {
	c.t.Helper()
	res, b, err := c.do(ctxT(c.t), method, url, body, hdr)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, url, err)
	}
	return res, b
}

// meta is FR-1 metadata; zero fields get valid defaults.
type meta struct {
	Device, SHA256, Filename, ContentType, CapturedAt, Priority string
}

func (m meta) header() string {
	def := func(v, d string) string {
		if v == "" {
			return d
		}
		return v
	}
	pairs := [][2]string{
		{"device_id", m.Device},
		{"sha256", m.SHA256},
		{"filename", def(m.Filename, "remote-e2e.flac")},
		{"content_type", def(m.ContentType, "audio/flac")},
		{"captured_at", def(m.CapturedAt, time.Now().Format(time.RFC3339))},
		{"priority", def(m.Priority, "3")},
	}
	var out []string
	for _, p := range pairs {
		if p[1] == "-" { // "-" omits the key
			continue
		}
		out = append(out, p[0]+" "+base64.StdEncoding.EncodeToString([]byte(p[1])))
	}
	return strings.Join(out, ",")
}

// create POSTs an upload of size bytes.
func (c *tus) create(size int, m meta) (*http.Response, string) {
	c.t.Helper()
	return c.mustDo(http.MethodPost, c.base, nil, map[string]string{
		"Upload-Length":   strconv.Itoa(size),
		"Upload-Metadata": m.header(),
	})
}

// mustCreate creates an upload and returns its URL; it is DELETEd at the
// end of the test unless it completed (404 then, ignored).
func (c *tus) mustCreate(f File, device string) string {
	c.t.Helper()
	res, body := c.create(len(f.Data), meta{Device: device, SHA256: f.SHA256})
	if res.StatusCode != http.StatusCreated {
		c.t.Fatalf("create: %d %q", res.StatusCode, body)
	}
	loc, err := res.Location()
	if err != nil {
		c.t.Fatalf("create: Location: %v", err)
	}
	c.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)
		defer cancel()
		_, _, _ = c.do(ctx, http.MethodDelete, loc.String(), nil, nil)
	})
	return loc.String()
}

// head returns the server offset, or -status for a non-200.
func (c *tus) head(url string) int {
	c.t.Helper()
	res, _ := c.mustDo(http.MethodHead, url, nil, nil)
	if res.StatusCode != http.StatusOK {
		return -res.StatusCode
	}
	off, err := strconv.Atoi(res.Header.Get("Upload-Offset"))
	if err != nil {
		c.t.Fatalf("head: Upload-Offset %q", res.Header.Get("Upload-Offset"))
	}
	return off
}

func patchHeaders(off int) map[string]string {
	return map[string]string{"Upload-Offset": strconv.Itoa(off), "Content-Type": "application/offset+octet-stream"}
}

// sendFrom PATCHes f from off to the end, re-syncing with HEAD on 409.
func (c *tus) sendFrom(url string, f File, off int) {
	c.t.Helper()
	for off < len(f.Data) {
		end := min(off+chunkSize, len(f.Data))
		res, body := c.mustDo(http.MethodPatch, url, f.Data[off:end], patchHeaders(off))
		switch res.StatusCode {
		case http.StatusNoContent:
			next, err := strconv.Atoi(res.Header.Get("Upload-Offset"))
			if err != nil || next <= off {
				c.t.Fatalf("patch at %d: Upload-Offset %q", off, res.Header.Get("Upload-Offset"))
			}
			off = next
		case http.StatusConflict:
			if off = c.head(url); off < 0 {
				c.t.Fatalf("head after 409: %d", -off)
			}
		default:
			c.t.Fatalf("patch at %d: %d %q", off, res.StatusCode, body)
		}
	}
}

// --- gRPC ---------------------------------------------------------------

type grpcConn struct {
	t     *testing.T
	c     uploadv1.UploadServiceClient
	token string
}

// grpc dials the /grpc prefix on REMOTE_URL, or the native listener with
// bare method paths.
func (e *env) grpc(t *testing.T, native bool, token string) *grpcConn {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(e.TLS.Clone()))}
	var target string
	if native {
		if e.GRPCNative == "" {
			t.Skip("REMOTE_GRPC_NATIVE not set")
		}
		target = e.GRPCNative
	} else {
		target = strings.TrimPrefix(e.URL, "https://")
		opts = append(opts, grpcprefix.DialOptions("/grpc")...)
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &grpcConn{t: t, c: uploadv1.NewUploadServiceClient(conn), token: token}
}

func (g *grpcConn) ctx(parent context.Context) context.Context {
	if g.token == "" {
		return parent
	}
	return metadata.AppendToOutgoingContext(parent, "authorization", "Bearer "+g.token)
}

func initReq(f File, device string) *uploadv1.InitRequest {
	return &uploadv1.InitRequest{
		DeviceId: device, Sha256: f.SHA256, Size: int64(len(f.Data)),
		Filename: "remote-e2e.flac", ContentType: "audio/flac",
		CapturedAt: time.Now().Format(time.RFC3339), Priority: 3,
	}
}

func (g *grpcConn) init(req *uploadv1.InitRequest) (*uploadv1.InitResponse, error) {
	return g.c.Init(g.ctx(ctxT(g.t)), req)
}

// mustInit calls Init and cancels the upload at the end of the test unless
// it completed.
func (g *grpcConn) mustInit(req *uploadv1.InitRequest) *uploadv1.InitResponse {
	g.t.Helper()
	res, err := g.init(req)
	if err != nil {
		g.t.Fatalf("init: %v", err)
	}
	g.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)
		defer cancel()
		_, _ = g.c.Cancel(g.ctx(ctx), &uploadv1.CancelRequest{UploadId: res.GetUploadId()})
	})
	return res
}

// upload streams f from off; with stopAfter > 0 it stops (abandons the
// stream) once an ack at or beyond stopAfter arrives. It returns the last
// ack.
func (g *grpcConn) upload(id string, f File, off, stopAfter int64) *uploadv1.Ack {
	g.t.Helper()
	ctx, cancel := context.WithCancel(g.ctx(ctxT(g.t)))
	defer cancel()
	stream, err := g.c.Upload(ctx)
	if err != nil {
		g.t.Fatalf("upload: %v", err)
	}
	var last *uploadv1.Ack
	for off < int64(len(f.Data)) {
		end := min(off+chunkSize, int64(len(f.Data)))
		if err := stream.Send(&uploadv1.Chunk{UploadId: id, Offset: off, Data: f.Data[off:end]}); err != nil {
			break // the error arrives through Recv
		}
		ack, err := stream.Recv() // window 1: one chunk in flight (GRPC_ACK_EVERY_N=1)
		if err != nil {
			g.t.Fatalf("upload: ack after offset %d: %v", off, err)
		}
		last, off = ack, ack.GetCommittedOffset()
		if ack.GetComplete() || (stopAfter > 0 && off >= stopAfter) {
			return last
		}
	}
	_ = stream.CloseSend()
	for last == nil || !last.GetComplete() {
		ack, err := stream.Recv()
		if err != nil {
			g.t.Fatalf("upload: final ack: %v", err)
		}
		last = ack
	}
	return last
}

func wantCode(t *testing.T, what string, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Errorf("%s: code %v (%v), want %v", what, got, err, want)
	}
}

// wantStatus is wantCode that also checks the status message.
func wantStatus(t *testing.T, what string, err error, want codes.Code, msg string) {
	t.Helper()
	if st := status.Convert(err); st.Code() != want || st.Message() != msg {
		t.Errorf("%s: %v %q, want %v %q", what, st.Code(), st.Message(), want, msg)
	}
}

// --- contract client -----------------------------------------------------

// runClient uploads path with internal/client (outbox, resume, backoff) and
// returns its result.
func (e *env) runClient(t *testing.T, protocol, path string) client.Result {
	t.Helper()
	url := e.URL
	if protocol == client.ProtoGRPCNative {
		if e.GRPCNative == "" {
			t.Skip("REMOTE_GRPC_NATIVE not set")
		}
		url = e.GRPCNative
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ob, err := client.OpenOutbox(ctx, filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ob.Close() }()
	r, err := client.New(client.Config{
		Protocol: protocol, URL: url, Token: e.Token(t), Device: e.Device,
		Priority: 3, Chunk: chunkSize, Retries: 3, BackoffMax: 5 * time.Second, TLS: e.TLS.Clone(),
	}, ob)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	res, err := r.Run(ctx, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatalf("results: %d", len(res))
	}
	return res[0]
}

func (e *env) String() string {
	return fmt.Sprintf("url=%s native=%q admin=%q device=%s", e.URL, e.GRPCNative, e.AdminURL, e.Device)
}
