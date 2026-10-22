package httpx

import (
	"context"
	"sync"
)

// ShutdownHook is a Service without a listener: Serve blocks until Shutdown,
// and Shutdown runs fn once. Put it first in Run's list to run a step (e.g.
// telling the gRPC service to end its streams) before the listeners stop.
type ShutdownHook struct {
	fn   func(ctx context.Context) error
	once sync.Once
	stop chan struct{}
	err  error
}

// NewShutdownHook returns a ShutdownHook running fn.
func NewShutdownHook(fn func(ctx context.Context) error) *ShutdownHook {
	return &ShutdownHook{fn: fn, stop: make(chan struct{})}
}

// Serve blocks until Shutdown has been called.
func (h *ShutdownHook) Serve() error {
	<-h.stop
	return nil
}

// Shutdown runs fn (only the first time) and unblocks Serve.
func (h *ShutdownHook) Shutdown(ctx context.Context) error {
	h.once.Do(func() {
		h.err = h.fn(ctx)
		close(h.stop)
	})
	return h.err
}
