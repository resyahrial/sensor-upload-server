package grpcapi

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	uploadv1 "github.com/resyahrial/sensor-upload-server/gen/upload/v1"
	"github.com/resyahrial/sensor-upload-server/internal/grpcprefix"
)

// Prefix is the path prefix of the gRPC endpoint on the TLS listener.
const Prefix = "/grpc"

// nativeStreamWindow is the HTTP/2 stream flow-control window of the native
// listener (the minimum grpc-go accepts is 64 KiB).
const nativeStreamWindow = 64 << 10

// msgOverhead is added to MaxChunkBytes for the receive limit (FR-6).
const msgOverhead = 64 << 10

// Native listener limits per connection: concurrent streams (grpc-go's
// default is unlimited) and the received header list size (default 16 MiB).
const (
	maxConcurrentStreams = 100
	maxHeaderListSize    = 16 << 10
)

// NewServer returns a grpc.Server with the FR-6 keepalive, enforcement and
// message-size settings and the auth interceptors, with svc registered both
// bare and under the /grpc prefix. The same server can be used for the
// native listener (grpc.Server.Serve) and, through PrefixHandler, for the TLS
// listener. On the TLS listener wrap it in LimitReadAhead. Extra options
// (e.g. credentials) are appended.
func NewServer(svc *Service, opts ...grpc.ServerOption) (*grpc.Server, error) {
	base := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
		grpc.MaxRecvMsgSize(svc.cfg.MaxChunkBytes + msgOverhead),
		grpc.ChainUnaryInterceptor(svc.UnaryInterceptor),
		grpc.ChainStreamInterceptor(svc.StreamInterceptor),
		// Releases read-ahead budget on the ServeHTTP path (LimitReadAhead).
		grpc.StatsHandler(readAheadStats{}),
		// Native listener: a fixed 64 KiB stream window instead of BDP
		// estimation. BDP grows the window (up to 16 MiB) from how fast
		// the network delivers, not how fast the service commits, so with
		// pipelining clients the transport buffered ~0.8 MiB per stream
		// (NFR-3). 64 KiB covers the bandwidth-delay product of every
		// netem profile; larger messages still flow (grpc-go widens the
		// window for a message being read).
		grpc.StaticStreamWindowSize(nativeStreamWindow),
		// Native listener only: on the TLS listener net/http's HTTP/2
		// server applies its own limits.
		grpc.MaxConcurrentStreams(maxConcurrentStreams),
		grpc.MaxHeaderListSize(maxHeaderListSize),
	}
	gs := grpc.NewServer(append(base, opts...)...)
	uploadv1.RegisterUploadServiceServer(gs, svc)
	if err := grpcprefix.RegisterWithPrefix(gs, &uploadv1.UploadService_ServiceDesc, svc, Prefix); err != nil {
		return nil, fmt.Errorf("grpcapi: %w", err)
	}
	return gs, nil
}

// PrefixHandler routes HTTP/2 requests whose path starts with "/grpc/" and
// whose Content-Type is application/grpc* to gs (a grpc.Server, normally
// wrapped in LimitReadAhead) with "/grpc" stripped; everything else goes to
// next.
func PrefixHandler(next, gs http.Handler) http.Handler {
	grpcH := http.StripPrefix(Prefix, gs)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 &&
			strings.HasPrefix(r.URL.Path, Prefix+"/") &&
			strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcH.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
