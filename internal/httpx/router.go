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
	// AltSvc is the Alt-Svc header value advertising HTTP/3, e.g.
	// `h3=":8443"`; empty = no header.
	AltSvc string
}

// NewRouter returns the TCP listener's handler: /rest/files/ → tus,
// /rest-http3/* → 404 (that endpoint is HTTP/3 only, so misrouted clients
// fail loudly), /grpc/ → gRPC (if configured), everything else 404. Every
// response carries Alt-Svc.
func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()
	if cfg.REST != nil {
		mux.Handle(PrefixREST, cfg.REST)
	}
	mux.Handle(PrefixRESTHTTP3, http.NotFoundHandler())
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
