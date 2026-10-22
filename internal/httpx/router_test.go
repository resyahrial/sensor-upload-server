package httpx

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouter(t *testing.T) {
	echo := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, name+" "+r.URL.Path)
		})
	}
	tests := []struct {
		name        string
		grpc        bool
		method      string
		path        string
		contentType string
		status      int
		body        string
	}{
		{"rest", false, http.MethodPost, "/rest/files/", "", 200, "rest /rest/files/"},
		{"rest-http3 is 404 on TCP", false, http.MethodPost, "/rest-http3/files/", "", 404, ""},
		{"rest-http3 subpath", false, http.MethodHead, "/rest-http3/files/abc", "", 404, ""},
		{"unknown", false, http.MethodGet, "/", "", 404, ""},
		{"grpc not mounted", false, http.MethodPost, "/grpc/upload.v1.UploadService/Init", "application/grpc", 404, ""},
		{"grpc", true, http.MethodPost, "/grpc/upload.v1.UploadService/Init", "application/grpc+proto", 200, "grpc /upload.v1.UploadService/Init"},
		{"grpc path without grpc content type", true, http.MethodPost, "/grpc/upload.v1.UploadService/Init", "text/plain", 404, ""},
		{"files", false, http.MethodGet, "/files/sha256/abc", "", 200, "files /files/sha256/abc"},
		{"files root is not rest", false, http.MethodGet, "/files/", "", 404, ""},
		{"rest files not lookup", false, http.MethodGet, "/rest/files/sha256/abc", "", 200, "rest /rest/files/sha256/abc"},
		{"files with grpc content type", true, http.MethodPost, "/files/sha256/abc", "application/grpc", 200, "files /files/sha256/abc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := RouterConfig{REST: echo("rest"), Files: echo("files"), AltSvc: AltSvcH3("8443")}
			if tc.grpc {
				cfg.GRPC = echo("grpc")
			}
			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()
			NewRouter(cfg).ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Errorf("status = %d, want %d", rec.Code, tc.status)
			}
			if tc.body != "" && rec.Body.String() != tc.body {
				t.Errorf("body = %q, want %q", rec.Body.String(), tc.body)
			}
			if got := rec.Header().Get("Alt-Svc"); got != `h3=":8443"; ma=86400` {
				t.Errorf("Alt-Svc = %q", got)
			}
		})
	}
}
