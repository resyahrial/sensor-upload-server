package tusapi

import (
	"context"
	"sync"
)

// useRegistry coordinates tus requests with the expiry cleaner per
// upload_ref. Requests share an upload (tusd's own lock hand-off decides
// between them); the cleaner gets it exclusively, and only when no request
// uses it, so it never interrupts an in-flight PATCH. Requests arriving
// while the cleaner holds an upload wait for it to finish.
type useRegistry struct {
	mu       sync.Mutex
	inUse    map[string]int
	cleaning map[string]chan struct{} // closed when the cleaner releases
}

func newUseRegistry() *useRegistry {
	return &useRegistry{inUse: map[string]int{}, cleaning: map[string]chan struct{}{}}
}

// enter marks ref as used by a request, waiting while the cleaner holds
// it. The returned leave must be called once the request is done.
func (r *useRegistry) enter(ctx context.Context, ref string) (leave func(), err error) {
	for {
		r.mu.Lock()
		ch, busy := r.cleaning[ref]
		if !busy {
			r.inUse[ref]++
			r.mu.Unlock()
			return sync.OnceFunc(func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				if r.inUse[ref]--; r.inUse[ref] <= 0 {
					delete(r.inUse, ref)
				}
			}), nil
		}
		r.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// tryExclusive takes ref for the cleaner if no request uses it.
func (r *useRegistry) tryExclusive(ref string) (release func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, busy := r.cleaning[ref]; busy || r.inUse[ref] > 0 {
		return nil, false
	}
	ch := make(chan struct{})
	r.cleaning[ref] = ch
	return sync.OnceFunc(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.cleaning, ref)
		close(ch)
	}), true
}

// CleanupStaging is the expiry cleaner's view of the tus staging
// (cleanup.TusStaging). One value serves both tus protocols: refs are
// UUIDs, unique across them.
type CleanupStaging struct{ st *Staging }

// ForCleanup returns the cleaner adapter for s.
func (s *Staging) ForCleanup() CleanupStaging { return CleanupStaging{st: s} }

// TryLock takes ref exclusively without waiting and without tusd's lock
// hand-off; ok=false if any tus request is using the upload.
func (c CleanupStaging) TryLock(_ context.Context, ref string) (func(), bool, error) {
	release, ok := c.st.use.tryExclusive(ref)
	return release, ok, nil
}

// Delete removes the staged upload of ref (nil if already gone). With
// s3store staging the tus ID also carries the multipart ID, which is not
// stored, so only filestore data can be deleted here; s3 needs a bucket
// lifecycle rule for abandoned multipart uploads.
func (c CleanupStaging) Delete(ctx context.Context, ref string) error {
	if !c.st.idIsRef {
		return nil
	}
	return c.st.input("", ref, ref).Delete(ctx)
}
