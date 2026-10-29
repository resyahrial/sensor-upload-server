package httpx

import (
	"net/http"
	"strings"
)

// Public route prefixes.
const (
	PrefixREST      = "/rest/files/"
	PrefixRESTHTTP3 = "/rest-http3/"
	PrefixGRPC      = "/grpc/"
	// PrefixFiles is the public, unauthenticated file lookup
	// (GET /files/sha256/{sha256}, internal/fileapi).
	PrefixFiles = "/files/sha256/"
	// PathFiles is the public listing of completed uploads (GET /files,
	// internal/fileapi).
	PathFiles = "/files"
	// PrefixDevices is device registration and token refresh
	// (POST /v1/devices/register, POST /v1/devices/token/refresh,
	// internal/deviceapi).
	PrefixDevices = "/v1/devices/"
	// PathReady is the public readiness probe (same as the admin /readyz).
	PathReady = "/readyz"
)

// RouterConfig configures the public TLS TCP router.
type RouterConfig struct {
	// REST serves tus on /rest/files/ (mounted as-is; it strips its own
	// prefix).
	REST http.Handler
	// GRPC, when non-nil, receives requests whose path starts with /grpc/
	// and whose Content-Type starts with application/grpc, with the "/grpc"
	// prefix stripped (so grpc.Server.ServeHTTP sees
	// /upload.v1.UploadService/...). nil = gRPC not mounted (404).
	GRPC http.Handler
	// Files, when non-nil, serves /files/sha256/ (mounted as-is).
	Files http.Handler
	// Devices, when non-nil, serves /v1/devices/ (mounted as-is).
	Devices http.Handler
	// FileList, when non-nil, serves GET /files (the completed upload
	// listing, internal/fileapi).
	FileList http.Handler
	// Ready, when non-nil, serves GET /readyz.
	Ready http.Handler
	// AltSvc is the Alt-Svc header value advertising HTTP/3, e.g.
	// `h3=":8443"`; empty = no header.
	AltSvc string
}

// NewRouter returns the TCP listener's handler: /rest/files/ → tus,
// /rest-http3/* → 404 (that endpoint is HTTP/3 only, so misrouted clients
// fail loudly), /grpc/ → gRPC (if configured), /files/sha256/ → file
// lookup (if configured), GET /files → upload listing and GET /readyz →
// readiness (if configured), everything else 404. Every
// response carries Alt-Svc.
func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()
	if cfg.REST != nil {
		mux.Handle(PrefixREST, cfg.REST)
	}
	mux.Handle(PrefixRESTHTTP3, http.NotFoundHandler())
	if cfg.Files != nil {
		mux.Handle(PrefixFiles, cfg.Files)
	}
	if cfg.Devices != nil {
		mux.Handle(PrefixDevices, cfg.Devices)
	}
	if cfg.FileList != nil {
		mux.Handle("GET "+PathFiles, cfg.FileList)
	}
	if cfg.Ready != nil {
		mux.Handle("GET "+PathReady, cfg.Ready)
	}
	var grpc http.Handler
	if cfg.GRPC != nil {
		grpc = http.StripPrefix(strings.TrimSuffix(PrefixGRPC, "/"), cfg.GRPC)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.AltSvc != "" {
			w.Header().Set("Alt-Svc", cfg.AltSvc)
		}
		if grpc != nil && strings.HasPrefix(r.URL.Path, PrefixGRPC) &&
			strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpc.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// AltSvcH3 returns the Alt-Svc value advertising h3 on port.
func AltSvcH3(port string) string { return `h3=":` + port + `"; ma=86400` }
