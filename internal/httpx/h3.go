package httpx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// H3Config configures an H3Server.
type H3Config struct {
	Name string
	// Addr is the UDP address to listen on, e.g. ":8443".
	Addr    string
	Handler http.Handler
	// TLS must hold the certificate; the config is cloned and forced to
	// TLS 1.3 (QUIC) with ALPN h3.
	TLS             *tls.Config
	MaxIdleTimeout  time.Duration // QUIC_IDLE_TIMEOUT
	KeepAlivePeriod time.Duration // QUIC_KEEPALIVE
	// ShutdownCause is set as the cancellation cause of every in-flight
	// request context when Shutdown starts. Defaults to ErrShuttingDown.
	ShutdownCause error
}

// H3Server is an HTTP/3 server bound to a UDP socket.
type H3Server struct {
	name     string
	srv      *http3.Server
	conn     net.PacketConn
	shutdown context.CancelCauseFunc
}

// ListenH3 binds cfg.Addr (UDP) and returns a server ready to Serve.
func ListenH3(cfg H3Config) (*H3Server, error) {
	if cfg.TLS == nil {
		return nil, fmt.Errorf("%s: TLS config is required", cfg.Name)
	}
	cause := cfg.ShutdownCause
	if cause == nil {
		cause = ErrShuttingDown
	}
	shutdownCtx, shutdown := context.WithCancelCause(context.Background())
	tlsCfg := cfg.TLS.Clone()
	tlsCfg.MinVersion = tls.VersionTLS13
	srv := &http3.Server{
		Handler:   withShutdownCause(shutdownCtx, cfg.Handler),
		TLSConfig: http3.ConfigureTLSConfig(tlsCfg),
		QUICConfig: &quic.Config{
			MaxIdleTimeout:  cfg.MaxIdleTimeout,
			KeepAlivePeriod: cfg.KeepAlivePeriod,
		},
		IdleTimeout: IdleTimeout,
	}
	conn, err := net.ListenPacket("udp", cfg.Addr)
	if err != nil {
		shutdown(nil)
		return nil, fmt.Errorf("listen %s on udp %s: %w", cfg.Name, cfg.Addr, err)
	}
	return &H3Server{
		name: cfg.Name, srv: srv, conn: conn,
		shutdown: func(error) { shutdown(cause) },
	}, nil
}

// withShutdownCause cancels each request's context with the shutdown
// cause once shutdownCtx is done (http3.Server has no BaseContext).
func withShutdownCause(shutdownCtx context.Context, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancelCause(r.Context())
		defer cancel(nil)
		stop := context.AfterFunc(shutdownCtx, func() { cancel(context.Cause(shutdownCtx)) })
		defer stop()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Addr returns the bound UDP address.
func (s *H3Server) Addr() net.Addr { return s.conn.LocalAddr() }

// Serve accepts QUIC connections until Shutdown.
func (s *H3Server) Serve() error {
	if err := s.srv.Serve(s.conn); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: %w", s.name, err)
	}
	return nil
}

// Shutdown interrupts in-flight requests with the shutdown cause, sends
// GOAWAY and waits for connections to finish until ctx is done, then
// closes everything.
func (s *H3Server) Shutdown(ctx context.Context) error {
	s.shutdown(nil)
	err := s.srv.Shutdown(ctx)
	if cerr := s.conn.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) && err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("%s: shutdown: %w", s.name, err)
	}
	return nil
}

// Close closes the server and its socket without draining.
func (s *H3Server) Close() error {
	s.shutdown(nil)
	err := s.srv.Close()
	if cerr := s.conn.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) && err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("%s: close: %w", s.name, err)
	}
	return nil
}
