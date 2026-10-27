package deviceapi

import (
	"sync"
	"time"
)

// limiter is an in-memory token bucket per key (client IP). Buckets that
// have refilled completely are dropped by a sweep at most once per
// rateLimiterSweepEvery, so the map only holds recently active clients.
type limiter struct {
	rate  float64 // tokens per second
	burst float64
	now   func() time.Time

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

type bucket struct {
	tokens float64
	at     time.Time
}

func newLimiter(perSecond, burst float64, now func() time.Time) *limiter {
	return &limiter{rate: perSecond, burst: burst, now: now, buckets: make(map[string]*bucket)}
}

// allow takes a token for key; if none is left it reports how long until
// one is.
func (l *limiter) allow(key string) (bool, time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastSweep) >= rateLimiterSweepEvery {
		for k, b := range l.buckets {
			if l.refill(b, now) >= l.burst {
				delete(l.buckets, k)
			}
		}
		l.lastSweep = now
	}
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, at: now}
		l.buckets[key] = b
	}
	b.tokens = l.refill(b, now)
	b.at = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
	return false, min(wait, rateLimiterMaxRetryWait)
}

// refill returns b's tokens at now (capped at burst).
func (l *limiter) refill(b *bucket, now time.Time) float64 {
	elapsed := now.Sub(b.at).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	return min(l.burst, b.tokens+elapsed*l.rate)
}
