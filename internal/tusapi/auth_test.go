package tusapi

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

// keyFixture serves the tus endpoints behind a real auth.Verifier on an
// in-memory key store.
type keyFixture struct {
	*fixture
	keys *auth.MemoryStore
}

func newKeyFixture(t *testing.T) *keyFixture {
	t.Helper()
	keys := auth.NewMemoryStore()
	v, err := auth.NewVerifier(keys, 0)
	if err != nil {
		t.Fatal(err)
	}
	return &keyFixture{fixture: newFixtureWith(t, v), keys: keys}
}

// as sends a request with "Authorization: Bearer <tok>" (none if tok is "").
func (f *keyFixture) as(method, path, tok string, body []byte, hdr map[string]string) *http.Response {
	f.t.Helper()
	h := map[string]string{}
	for k, v := range hdr {
		h[k] = v
	}
	if tok != "" {
		h["Authorization"] = "Bearer " + tok
	}
	return f.do(method, path, "", body, h)
}

func (f *keyFixture) createAs(tok, device string, data []byte) *http.Response {
	f.t.Helper()
	return f.as(http.MethodPost, BasePathREST, tok, nil, map[string]string{
		"Upload-Length": strconv.Itoa(len(data)), "Upload-Metadata": metadata(device, shaHex(data)),
	})
}

func mustIssue(t *testing.T, keys *auth.MemoryStore, device string) string {
	t.Helper()
	tok, err := keys.Issue(device)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestDeviceKeyAuth(t *testing.T) {
	f := newKeyFixture(t)
	const dev = "cam-0001"
	tok := mustIssue(t, f.keys, dev)
	_, key, _ := auth.ParseToken(tok)
	wrongKey := auth.FormatToken(dev, strings.Repeat("0", auth.KeyHexLen))
	unknown := auth.FormatToken("cam-9999", key)

	data := []byte("hello device keys")
	res := f.createAs(tok, dev, data)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("valid key: %d %s", res.StatusCode, readBody(t, res))
	}
	loc := res.Header.Get("Location")

	for name, bad := range map[string]string{
		"missing": "", "wrong key": wrongKey, "unknown device": unknown,
		"no underscore": dev + key, "non-hex key": dev + "_" + strings.Repeat("z", auth.KeyHexLen),
	} {
		t.Run(name, func(t *testing.T) {
			for _, m := range []string{http.MethodPost, http.MethodHead, http.MethodPatch, http.MethodDelete, http.MethodOptions} {
				target := loc
				if m == http.MethodPost || m == http.MethodOptions {
					target = BasePathREST
				}
				res := f.as(m, target, bad, nil, map[string]string{"Upload-Length": "1", "Upload-Offset": "0",
					"Content-Type": "application/offset+octet-stream"})
				if res.StatusCode != http.StatusUnauthorized {
					t.Errorf("%s: %d, want 401", m, res.StatusCode)
				}
				if m != http.MethodHead {
					if body := readBody(t, res); body != "unauthenticated\n" {
						t.Errorf("%s body %q: must be the same generic body for every case", m, body)
					}
				}
			}
		})
	}
	if strings.Contains(f.logs.String(), key) {
		t.Fatal("log contains the key")
	}
	if !strings.Contains(f.logs.String(), "wrong key") || !strings.Contains(f.logs.String(), "unknown device") {
		t.Errorf("log lacks the failure detail:\n%s", f.logs.String())
	}

	t.Run("metadata device mismatch", func(t *testing.T) {
		other := mustIssue(t, f.keys, "cam-0002")
		if res := f.createAs(other, dev, data); res.StatusCode != http.StatusForbidden {
			t.Fatalf("%d, want 403", res.StatusCode)
		}
	})
	t.Run("other device's upload", func(t *testing.T) {
		other, _ := f.keys.Token("cam-0002")
		if res := f.as(http.MethodHead, loc, other, nil, nil); res.StatusCode != http.StatusNotFound {
			t.Fatalf("HEAD %d, want 404", res.StatusCode)
		}
	})
}

func TestDeviceKeyRotation(t *testing.T) {
	f := newKeyFixture(t)
	const dev = "cam-0001"
	old := mustIssue(t, f.keys, dev)
	data := []byte("rotate me")
	res := f.createAs(old, dev, data)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d", res.StatusCode)
	}
	loc := res.Header.Get("Location")

	rotated := mustIssue(t, f.keys, dev)
	if res := f.as(http.MethodHead, loc, old, nil, nil); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old key after rotation: %d, want 401", res.StatusCode)
	}
	// The upload survives rotation: the new key resumes it.
	if res := f.as(http.MethodHead, loc, rotated, nil, nil); res.StatusCode != http.StatusOK {
		t.Fatalf("new key HEAD: %d", res.StatusCode)
	}
	res = f.as(http.MethodPatch, loc, rotated, data, map[string]string{
		"Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("new key PATCH: %d %s", res.StatusCode, readBody(t, res))
	}

	f.keys.Delete(dev)
	if res := f.as(http.MethodHead, loc, rotated, nil, nil); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("deleted key: %d, want 401", res.StatusCode)
	}
}

func TestDeviceKeyStoreDown(t *testing.T) {
	f := newKeyFixture(t)
	tok := mustIssue(t, f.keys, "cam-0001")
	f.keys.SetErr(errors.New("dial tcp: connection refused"))
	res := f.createAs(tok, "cam-0001", []byte("x"))
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("store down: %d, want 503", res.StatusCode)
	}
	if body := readBody(t, res); body != "authentication temporarily unavailable\n" {
		t.Errorf("body %q", body)
	}
	if res := f.as(http.MethodHead, BasePathREST+"0190f0a0-0000-7000-8000-000000000000", tok, nil, nil); res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("HEAD with store down: %d, want 503", res.StatusCode)
	}
	// A malformed token is still 401: it never reaches the store.
	if res := f.as(http.MethodPost, BasePathREST, "junk", nil, nil); res.StatusCode != http.StatusUnauthorized {
		t.Errorf("malformed with store down: %d, want 401", res.StatusCode)
	}
	f.keys.SetErr(nil)
	if res := f.createAs(tok, "cam-0001", []byte("x")); res.StatusCode != http.StatusCreated {
		t.Fatalf("store back: %d", res.StatusCode)
	}
}
