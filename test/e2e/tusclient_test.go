//go:build integration

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/db"
	"github.com/resyahrial/sensor-upload-server/internal/meta"
)

// File is a random test file with its tus metadata.
type File struct {
	Data   []byte
	SHA256 string
	Device string
}

// NewFile returns size random bytes for device.
func NewFile(t *testing.T, device string, size int) File {
	t.Helper()
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return File{Data: b, SHA256: hex.EncodeToString(s[:]), Device: device}
}

// Metadata encodes the Upload-Metadata header; sha overrides the digest
// when non-empty.
func (f File) Metadata(sha string) string {
	if sha == "" {
		sha = f.SHA256
	}
	pairs := [][2]string{
		{"device_id", f.Device}, {"sha256", sha}, {"filename", "e2e.flac"},
		{"content_type", "audio/flac"}, {"captured_at", "2026-10-21T05:30:00+07:00"}, {"priority", "2"},
	}
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p[0] + " " + base64.StdEncoding.EncodeToString([]byte(p[1]))
	}
	return strings.Join(out, ",")
}

// FinalKey is the object key the finalizer stores the file under.
func (f File) FinalKey(t *testing.T) string {
	t.Helper()
	ts, err := meta.ParseCapturedAt("2026-10-21T05:30:00+07:00")
	if err != nil {
		t.Fatal(err)
	}
	return meta.FinalKey(meta.Metadata{DeviceID: f.Device, SHA256: f.SHA256, ContentType: "audio/flac", CapturedAt: ts})
}

// Tus is a raw tus client for one endpoint.
type Tus struct {
	t     *testing.T
	HC    *http.Client
	Base  string // https://host:port/rest/files/
	Token string
}

// Do sends a tus request with auth and Tus-Resumable.
func (c *Tus) Do(ctx context.Context, method, url string, body io.Reader, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if b, ok := body.(*bytes.Reader); ok {
		req.ContentLength = int64(b.Len())
	}
	req.Header.Set("Tus-Resumable", "1.0.0")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	return c.HC.Do(req)
}

// MustDo is Do that fails the test on a transport error and reads the body.
func (c *Tus) MustDo(method, url string, body []byte, hdr map[string]string) (*http.Response, string) {
	c.t.Helper()
	var r io.Reader = http.NoBody
	if body != nil {
		r = bytes.NewReader(body)
	}
	res, err := c.Do(context.Background(), method, url, r, hdr)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

// Create POSTs the file and returns the response.
func (c *Tus) Create(f File, sha string) (*http.Response, string) {
	c.t.Helper()
	return c.MustDo(http.MethodPost, c.Base, nil, map[string]string{
		"Upload-Length":   strconv.Itoa(len(f.Data)),
		"Upload-Metadata": f.Metadata(sha),
	})
}

// MustCreate creates an upload and returns its URL.
func (c *Tus) MustCreate(f File) string {
	c.t.Helper()
	res, body := c.Create(f, "")
	if res.StatusCode != http.StatusCreated {
		c.t.Fatalf("create: %d %q", res.StatusCode, body)
	}
	return res.Header.Get("Location")
}

// Patch sends one chunk at off.
func (c *Tus) Patch(url string, off int, chunk []byte) (*http.Response, string) {
	c.t.Helper()
	return c.MustDo(http.MethodPatch, url, chunk, patchHeaders(off))
}

func patchHeaders(off int) map[string]string {
	return map[string]string{"Upload-Offset": strconv.Itoa(off), "Content-Type": "application/offset+octet-stream"}
}

// Head returns the server offset (or -status on a non-200).
func (c *Tus) Head(url string) int {
	c.t.Helper()
	res, _ := c.MustDo(http.MethodHead, url, nil, nil)
	if res.StatusCode != http.StatusOK {
		return -res.StatusCode
	}
	off, err := strconv.Atoi(res.Header.Get("Upload-Offset"))
	if err != nil {
		c.t.Fatalf("head: bad Upload-Offset %q", res.Header.Get("Upload-Offset"))
	}
	return off
}

// SendFrom uploads f from off to the end in chunks, expecting 204s. It
// returns the last response.
func (c *Tus) SendFrom(url string, f File, off, chunk int) *http.Response {
	c.t.Helper()
	var res *http.Response
	for off < len(f.Data) {
		end := min(off+chunk, len(f.Data))
		var body string
		res, body = c.Patch(url, off, f.Data[off:end])
		if res.StatusCode != http.StatusNoContent {
			c.t.Fatalf("patch at %d: %d %q", off, res.StatusCode, body)
		}
		next, _ := strconv.Atoi(res.Header.Get("Upload-Offset"))
		if next != end {
			c.t.Fatalf("patch at %d: Upload-Offset %d, want %d", off, next, end)
		}
		off = end
	}
	return res
}

// Endpoint returns a raw tus client for base path on s over proto.
func Endpoint(t *testing.T, s *Server, proto, base, device string) *Tus {
	return &Tus{t: t, HC: NewClient(t, proto), Base: s.URL() + base, Token: Token(t, device)}
}

// refOf returns the upload_ref of an upload URL (the tus ID without an
// s3store "+<multipart id>" suffix).
func refOf(url string) string {
	ref, _, _ := strings.Cut(url[strings.LastIndex(url, "/")+1:], "+")
	return ref
}

// Row loads the DB row of an upload URL.
func Row(t *testing.T, s *Server, protocol, url string) db.Upload {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := s.Repo.Get(ctx, protocol, refOf(url))
	if err != nil {
		t.Fatalf("load row: %v", err)
	}
	return u
}

// AssertStored checks that f is completed in the DB and in the local
// object store.
func AssertStored(t *testing.T, s *Server, protocol, url string, f File) {
	t.Helper()
	u := Row(t, s, protocol, url)
	if u.Status != db.StatusCompleted || u.CommittedBytes != int64(len(f.Data)) {
		t.Fatalf("row status=%s committed=%d", u.Status, u.CommittedBytes)
	}
	if u.StorageKey != f.FinalKey(t) {
		t.Errorf("storage_key = %q, want %q", u.StorageKey, f.FinalKey(t))
	}
	if s.Store != nil {
		got, err := os.ReadFile(filepath.Join(s.env["OBJSTORE_LOCAL_DIR"], filepath.FromSlash(u.StorageKey)))
		if err != nil || !bytes.Equal(got, f.Data) {
			t.Errorf("stored object differs (err %v, %d bytes)", err, len(got))
		}
	}
	AssertNotStaged(t, s, url)
}

// AssertNotStaged checks that the tus staging files are gone.
func AssertNotStaged(t *testing.T, s *Server, url string) {
	t.Helper()
	for _, name := range []string{refOf(url), refOf(url) + ".info"} {
		if _, err := os.Stat(filepath.Join(s.TusDir(), name)); !os.IsNotExist(err) {
			t.Errorf("staging file %s still present (%v)", name, err)
		}
	}
}

// WaitStaged waits until the staged file of url has at least n bytes.
func WaitStaged(t *testing.T, s *Server, url string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fi, err := os.Stat(filepath.Join(s.TusDir(), refOf(url))); err == nil && fi.Size() >= int64(n) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("staged file of %s never reached %d bytes", url, n)
}
