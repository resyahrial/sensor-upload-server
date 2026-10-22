//go:build integration

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/fileapi"
)

// lookup GETs /files/sha256/{sha} without a token.
func lookup(t *testing.T, hc *http.Client, base, sha string) (*http.Response, fileapi.Response) {
	t.Helper()
	res, err := hc.Get(base + "/files/sha256/" + sha)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	var body fileapi.Response
	if err := json.Unmarshal(b, &body); err != nil {
		t.Fatalf("body %q: %v", b, err)
	}
	return res, body
}

// TestFileLookup uploads a file over tus and checks that the public lookup
// reports it as completed only afterwards, over HTTP/1.1, HTTP/2 and HTTP/3.
func TestFileLookup(t *testing.T) {
	s := StartServer(t, nil)
	dev := device()
	c := Endpoint(t, s, H2, "/rest/files/", dev)
	f := NewFile(t, dev, 2*chunk+7)

	for _, proto := range []string{H1, H2, H3} {
		if res, body := lookup(t, NewClient(t, proto), s.URL(), f.SHA256); res.StatusCode != http.StatusNotFound || body.Status != fileapi.StatusNotFound {
			t.Fatalf("%s before upload: %d %+v", proto, res.StatusCode, body)
		}
	}
	loc := c.MustCreate(f)
	if res, body := c.Patch(loc, 0, f.Data[:chunk]); res.StatusCode != http.StatusNoContent {
		t.Fatalf("first patch: %d %q", res.StatusCode, body)
	}
	// In-progress uploads are never disclosed.
	if res, body := lookup(t, NewClient(t, H2), s.URL(), f.SHA256); res.StatusCode != http.StatusNotFound {
		t.Fatalf("in progress: %d %+v", res.StatusCode, body)
	}
	start := time.Now().Add(-time.Second).Truncate(time.Second)
	c.SendFrom(loc, f, chunk, chunk)
	AssertStored(t, s, "rest", loc, f)

	majors := map[string]int{H1: 1, H2: 2, H3: 3}
	for _, proto := range []string{H1, H2, H3} {
		res, body := lookup(t, NewClient(t, proto), s.URL(), f.SHA256)
		if res.StatusCode != http.StatusOK || body.Status != fileapi.StatusCompleted ||
			body.Size != int64(len(f.Data)) || body.SHA256 != f.SHA256 {
			t.Fatalf("%s after upload: %d %+v", proto, res.StatusCode, body)
		}
		if res.ProtoMajor != majors[proto] {
			t.Errorf("%s: served over HTTP/%d", proto, res.ProtoMajor)
		}
		if at, err := time.Parse(time.RFC3339, body.CompletedAt); err != nil || at.Before(start) {
			t.Errorf("%s: completed_at %q (%v)", proto, body.CompletedAt, err)
		}
		if cc := res.Header.Get("Cache-Control"); cc != "no-store" {
			t.Errorf("%s: Cache-Control %q", proto, cc)
		}
	}
	res, err := NewClient(t, H2).Get(s.URL() + "/files/sha256/NOT-HEX")
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid sha: %d", res.StatusCode)
	}
}
