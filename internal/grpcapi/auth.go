package grpcapi

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/resyahrial/sensor-upload-server/internal/auth"
)

type deviceKey struct{}

// deviceFrom returns the device_id the auth interceptor stored in ctx.
func deviceFrom(ctx context.Context) string {
	d, _ := ctx.Value(deviceKey{}).(string)
	return d
}

// Auth failures; the messages are the same codes as the REST JSON bodies.
var (
	errUnauthenticated = status.Error(codes.Unauthenticated, "invalid_token")
	// errTokenExpired: the device refreshes its access token and retries.
	errTokenExpired = status.Error(codes.Unauthenticated, "token_expired")
	errForbidden    = status.Error(codes.PermissionDenied, "device_not_active")
)

// authenticate rejects calls during shutdown and verifies the bearer token
// in the "authorization" metadata (once per call, i.e. once per stream),
// returning ctx carrying the device_id.
// It applies to every method, so it never inspects the method name (which
// differs between the bare and the /grpc-prefixed registration).
func (s *Service) authenticate(ctx context.Context) (context.Context, error) {
	if s.shuttingDown() {
		return nil, errUnavailable
	}
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) != 1 {
		return nil, errUnauthenticated
	}
	device, err := s.verifier.VerifyHeader(ctx, vals[0])
	if err != nil {
		s.log.Info("authentication failed", "error", err.Error())
		if errors.Is(err, auth.ErrTokenExpired) {
			return nil, errTokenExpired
		}
		return nil, errUnauthenticated
	}
	return context.WithValue(ctx, deviceKey{}, device), nil
}

// UnaryInterceptor authenticates unary calls.
func (s *Service) UnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamInterceptor authenticates streaming calls.
func (s *Service) StreamInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, err := s.authenticate(ss.Context())
	if err != nil {
		return err
	}
	return handler(srv, &authedStream{ServerStream: ss, ctx: ctx})
}

// authedStream overrides the stream context with the authenticated one.
type authedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (a *authedStream) Context() context.Context { return a.ctx }
