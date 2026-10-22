package grpcapi

import "sync"

// registry tracks the uploads that are in use by a stream (or by a Cancel or
// finalize call) in this process, so only one holder exists per upload id.
type registry struct {
	mu     sync.Mutex
	active map[string]struct{}
}

func newRegistry() *registry {
	return &registry{active: make(map[string]struct{})}
}

// acquire claims id. It returns ok=false if id is already held; otherwise the
// caller must call release exactly once.
func (r *registry) acquire(id string) (release func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, held := r.active[id]; held {
		return nil, false
	}
	r.active[id] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.active, id)
			r.mu.Unlock()
		})
	}, true
}

func (r *registry) held(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.active[id]
	return ok
}

// size returns the number of held uploads.
func (r *registry) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.active)
}
