// Package httpx provides the HTTP listeners, the admin (metrics/health)
// handler and the graceful-shutdown lifecycle shared by all servers.
package httpx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// NFR-4 timeouts for the public listener.
const (
	ReadHeaderTimeout = 15 * time.Second
	IdleTimeout       = 120 * time.Second
)

// MaxReceiveBufferPerStream is the HTTP/2 receive window (and buffer) per
// request on the TLS listener, instead of net/http's 1 MiB default. A
// client that sends faster than the handler reads (a pipelining gRPC
// client against a slow commit, or a tus PATCH) fills it, so it is the
// per-upload memory bound: 1 MiB x 200 in-progress uploads was most of the
// server's live heap in the NFR-3 load test. 256 KiB still exceeds the
// bandwidth-delay product of every netem profile (at most ~64 KB).
const MaxReceiveBufferPerStream = 256 << 10

// ErrShuttingDown is the default cause attached to request contexts once a
// server starts shutting down.
var ErrShuttingDown = errors.New("server shutting down")

// Service is a long-running server managed by Run.
type Service interface {
	// Serve blocks until the service stops. It returns nil after a
	// successful Shutdown.
	Serve() error
	// Shutdown stops accepting work and waits for in-flight work until ctx
	// is done.
	Shutdown(ctx context.Context) error
}

// ServerConfig configures an HTTPServer.
type ServerConfig struct {
	// Name identifies the server in logs.
	Name string
	// Addr is the TCP address to listen on, e.g. ":8443".
	Addr    string
	Handler http.Handler
	// TLS enables HTTPS (HTTP/1.1 and HTTP/2) when non-nil. The config is
	// cloned and forced to TLS 1.2 or newer.
	TLS *tls.Config
	// ShutdownCause is set as the cancellation cause of every request
	// context when Shutdown starts (tusd needs handler.ErrServerShutdown
	// here to answer 503). Defaults to ErrShuttingDown.
	ShutdownCause error
}

// HTTPServer is an http.Server bound to a listener.
type HTTPServer struct {
	name string
	srv  *http.Server
	ln   net.Listener
}

// Listen binds cfg.Addr immediately (so port conflicts fail at startup)
// and returns a server ready to Serve.
func Listen(cfg ServerConfig) (*HTTPServer, error) {
	cause := cfg.ShutdownCause
	if cause == nil {
		cause = ErrShuttingDown
	}
	baseCtx, cancel := context.WithCancelCause(context.Background())

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	srv := &http.Server{
		Handler:           cfg.Handler,
		ReadHeaderTimeout: ReadHeaderTimeout,
		IdleTimeout:       IdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		Protocols:         protocols,
		HTTP2:             &http.HTTP2Config{MaxReceiveBufferPerStream: MaxReceiveBufferPerStream},
	}
	srv.RegisterOnShutdown(func() { cancel(cause) })

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		cancel(nil)
		return nil, fmt.Errorf("listen %s on %s: %w", cfg.Name, cfg.Addr, err)
	}
	if cfg.TLS != nil {
		tlsCfg := cfg.TLS.Clone()
		if tlsCfg.MinVersion < tls.VersionTLS12 {
			tlsCfg.MinVersion = tls.VersionTLS12
		}
		tlsCfg.NextProtos = []string{"h2", "http/1.1"}
		srv.TLSConfig = tlsCfg
		protocols.SetHTTP2(true)
		ln = tls.NewListener(ln, tlsCfg)
	}
	return &HTTPServer{name: cfg.Name, srv: srv, ln: ln}, nil
}

// Addr returns the bound address.
func (s *HTTPServer) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until Shutdown.
func (s *HTTPServer) Serve() error {
	if err := s.srv.Serve(s.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: %w", s.name, err)
	}
	return nil
}

// Shutdown drains in-flight requests until ctx is done, then force-closes
// the remaining connections.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	if err := s.srv.Shutdown(ctx); err != nil {
		_ = s.srv.Close() // best effort; the drain error is what matters
		return fmt.Errorf("%s: shutdown: %w", s.name, err)
	}
	return nil
}

// Close closes the listener and all connections without draining (for a
// server that failed to start).
func (s *HTTPServer) Close() error {
	_ = s.srv.Close() // not serving yet: only the listener is open
	if err := s.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("%s: close: %w", s.name, err)
	}
	return nil
}

// Run serves all services until ctx is cancelled (e.g. SIGTERM) or one of
// them fails, then shuts them down in the given order within timeout.
// Put the public listeners first so the admin server keeps answering
// health checks while they drain.
func Run(ctx context.Context, log *slog.Logger, timeout time.Duration, svcs ...Service) error {
	errc := make(chan error, len(svcs))
	for _, s := range svcs {
		go func() { errc <- s.Serve() }()
	}

	var first error
	pending := len(svcs)
	select {
	case <-ctx.Done():
		log.Info("shutdown requested", "timeout", timeout.String())
	case err := <-errc:
		pending--
		if err != nil {
			first = fmt.Errorf("server failed: %w", err)
			log.Error("server failed, shutting down", "error", err)
		}
	}

	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	errs := []error{first}
	for _, s := range svcs {
		if err := s.Shutdown(sctx); err != nil {
			errs = append(errs, err)
		}
	}
	for ; pending > 0; pending-- {
		if err := <-errc; err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
