package httpx

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// recorder collects the order in which services are shut down.
type recorder struct {
	mu    sync.Mutex
	order []string
}

func (r *recorder) add(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, name)
}

// orderService records when its Shutdown runs.
type orderService struct {
	rec  *recorder
	stop chan struct{}
	once sync.Once
}

func (o *orderService) Serve() error { <-o.stop; return nil }

func (o *orderService) Shutdown(context.Context) error {
	o.rec.add("svc")
	o.once.Do(func() { close(o.stop) })
	return nil
}

func TestShutdownHookRunsFirstAndOnce(t *testing.T) {
	rec := &recorder{}
	boom := errors.New("drain timed out")
	hook := NewShutdownHook(func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("hook ctx has no deadline")
		}
		rec.add("hook")
		return boom
	})
	svc := &orderService{rec: rec, stop: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, discardLogger(), time.Second, hook, svc); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the hook's error", err)
	}
	if err := hook.Shutdown(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("second Shutdown = %v", err)
	}
	if want := []string{"hook", "svc"}; !slices.Equal(rec.order, want) {
		t.Fatalf("order = %v, want %v", rec.order, want)
	}
}
