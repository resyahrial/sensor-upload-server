package app

import (
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc"
)

// nativeGRPC serves the gRPC server on its own TLS listener
// (GRPC_NATIVE_ADDR). It implements httpx.Service and io.Closer.
type nativeGRPC struct {
	gs *grpc.Server
	ln net.Listener
}

func listenNativeGRPC(addr string, gs *grpc.Server) (*nativeGRPC, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen grpc-native on %s: %w", addr, err)
	}
	return &nativeGRPC{gs: gs, ln: ln}, nil
}

// Addr returns the bound address.
func (n *nativeGRPC) Addr() net.Addr { return n.ln.Addr() }

// Serve accepts connections until Shutdown.
func (n *nativeGRPC) Serve() error {
	if err := n.gs.Serve(n.ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("grpc-native: %w", err)
	}
	return nil
}

// Shutdown stops accepting, sends GOAWAY and waits for the open streams
// until ctx is done, then closes the remaining connections.
func (n *nativeGRPC) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		n.gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		n.gs.Stop()
		<-done
		return fmt.Errorf("grpc-native: shutdown: %w", ctx.Err())
	}
}

// Close releases the listener of a server that never served.
func (n *nativeGRPC) Close() error {
	if err := n.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("grpc-native: close: %w", err)
	}
	return nil
}
