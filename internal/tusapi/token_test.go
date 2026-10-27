package tusapi

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
	"github.com/resyahrial/sensor-upload-server/internal/auth/authtest"
)

// authAs sends a request with the given Authorization header value (none
// if authz is "").
func (f *fixture) authAs(method, path, authz string, hdr map[string]string) *http.Response {
	f.t.Helper()
	h := map[string]string{"Upload-Length": "1", "Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"}
	for k, v := range hdr {
		h[k] = v
	}
	if authz != "" {
		h["Authorization"] = authz
	}
	return f.do(method, path, "", nil, h)
}

func TestTokenAuth(t *testing.T) {
	f := newFixture(t)
	const dev = "cam-0001"
	data := []byte("hello access tokens")
	loc := f.create(BasePathREST, dev, data)

	otherKey := authtest.New(t)
	otherIss, err := auth.NewIssuer(auth.IssuerConfig{Key: f.keys.Key, Issuer: "someone-else", TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	wrongIss, _, err := otherIss.Issue(dev)
	if err != nil {
		t.Fatal(err)
	}
	valid := f.token(dev)
	expired := f.keys.Expired(t, dev)
	cases := []struct {
		name, authz, code string
	}{
		{"missing", "", "invalid_token"},
		{"not bearer", "Basic " + valid, "invalid_token"},
		{"garbage", "Bearer not-a-jwt", "invalid_token"},
		{"other signing key", otherKey.Bearer(t, dev), "invalid_token"},
		{"tampered signature", "Bearer " + valid[:len(valid)-4] + "AAAA", "invalid_token"},
		{"wrong issuer", "Bearer " + wrongIss, "invalid_token"},
		{"expired", "Bearer " + expired, "token_expired"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, m := range []string{http.MethodPost, http.MethodHead, http.MethodPatch, http.MethodDelete, http.MethodOptions} {
				target := loc
				if m == http.MethodPost || m == http.MethodOptions {
					target = BasePathREST
				}
				res := f.authAs(m, target, tc.authz, nil)
				if res.StatusCode != http.StatusUnauthorized {
					t.Errorf("%s: %d, want 401", m, res.StatusCode)
				}
				if got := res.Header.Get("Content-Type"); got != "application/json" {
					t.Errorf("%s: Content-Type %q", m, got)
				}
				if got := res.Header.Get("WWW-Authenticate"); got != `Bearer error="invalid_token", error_description="`+tc.code+`"` {
					t.Errorf("%s: WWW-Authenticate %q", m, got)
				}
				want := `{"error":"` + tc.code + `"}`
				if m == http.MethodHead {
					want = ""
				}
				if body := readBody(t, res); body != want {
					t.Errorf("%s: body %q, want %q", m, body, want)
				}
			}
		})
	}
	logs := f.logs.String()
	for _, tok := range []string{valid, expired, wrongIss} {
		if strings.Contains(logs, tok) {
			t.Fatal("log contains a token")
		}
	}
	if !strings.Contains(logs, "token expired") || !strings.Contains(logs, "missing bearer token") {
		t.Errorf("log lacks the failure detail:\n%s", logs)
	}

	t.Run("metadata device mismatch", func(t *testing.T) {
		res := f.authAs(http.MethodPost, BasePathREST, "Bearer "+f.token("cam-0002"), map[string]string{
			"Upload-Length": strconv.Itoa(len(data)), "Upload-Metadata": metadata(dev, shaHex(data)),
		})
		if res.StatusCode != http.StatusForbidden {
			t.Fatalf("%d, want 403", res.StatusCode)
		}
		if got := res.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type %q", got)
		}
		if body := readBody(t, res); body != `{"error":"device_not_active"}` {
			t.Errorf("body %q", body)
		}
	})
	t.Run("other device's upload", func(t *testing.T) {
		// 404, not 403: another device's upload is not revealed.
		if res := f.do(http.MethodHead, loc, "cam-0002", nil, nil); res.StatusCode != http.StatusNotFound {
			t.Fatalf("HEAD %d, want 404", res.StatusCode)
		}
	})
	t.Run("new token resumes", func(t *testing.T) {
		// Tokens are stateless: a freshly minted token for the same device
		// continues the upload started with another one.
		res := f.patchWith(loc, f.keys.Bearer(t, dev), data)
		if res.StatusCode != http.StatusNoContent {
			t.Fatalf("PATCH %d %s", res.StatusCode, readBody(t, res))
		}
	})
}

// patchWith sends the whole of data at offset 0 with Authorization authz.
func (f *fixture) patchWith(loc, authz string, data []byte) *http.Response {
	f.t.Helper()
	req, err := http.NewRequest(http.MethodPatch, loc, strings.NewReader(string(data)))
	if err != nil {
		f.t.Fatal(err)
	}
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Authorization", authz)
	req.Header.Set("Upload-Offset", "0")
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	res, err := f.srv.Client().Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { res.Body.Close() })
	return res
}
