// Package grpcprefix serves and calls gRPC services under an extra HTTP/2 path
// prefix such as "/grpc".
//
// The client contract puts every method under "/grpc/upload.v1.UploadService/…".
// grpc-go splits the request path at its last "/" into service and method, so a
// copy of the generated ServiceDesc renamed to "grpc/upload.v1.UploadService"
// is dispatched for the prefixed path on a native grpc.Server. Clients add the
// prefix with the interceptors returned by DialOptions.
package grpcprefix

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc"
)

// normalize turns "/grpc", "grpc" or "/grpc/" into "grpc".
func normalize(prefix string) string {
	return strings.Trim(prefix, "/")
}

// Desc returns a shallow copy of desc whose ServiceName is prefixed, so the
// wire path becomes "/{prefix}/{service}/{method}". Handlers are shared.
func Desc(desc *grpc.ServiceDesc, prefix string) *grpc.ServiceDesc {
	d := *desc
	d.ServiceName = normalize(prefix) + "/" + desc.ServiceName
	return &d
}

// RegisterWithPrefix registers impl on s a second time under the prefixed
// copy of desc (see Desc). It does not register the bare service; call the
// generated Register…Server for that. Unlike grpc.Server.RegisterService, which
// exits the process on a duplicate name, it returns an error.
func RegisterWithPrefix(s *grpc.Server, desc *grpc.ServiceDesc, impl any, prefix string) error {
	if normalize(prefix) == "" {
		return errors.New("grpcprefix: empty prefix")
	}
	d := Desc(desc, prefix)
	if _, ok := s.GetServiceInfo()[d.ServiceName]; ok {
		return fmt.Errorf("grpcprefix: service %q already registered", d.ServiceName)
	}
	s.RegisterService(d, impl)
	return nil
}

// DialOptions returns the client interceptors from the contract (§5.4) that
// prepend prefix (e.g. "/grpc") to every unary and streaming method path.
func DialOptions(prefix string) []grpc.DialOption {
	p := "/" + normalize(prefix)
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, p+method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc,
			cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(ctx, desc, cc, p+method, opts...)
		}),
	}
}
