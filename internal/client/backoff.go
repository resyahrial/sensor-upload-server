package client

import (
	"math/rand/v2"
	"time"
)

// backoffBase is the first retry delay from contract §5.1.
const backoffBase = time.Second

// backoff returns the delay before retry number step (0-based): a uniformly
// random duration in [0, min(max, 1s·2^step)] ("full jitter").
func backoff(step int, maxDelay time.Duration) time.Duration {
	ceiling := backoffBase
	for i := 0; i < step && ceiling < maxDelay; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, maxDelay)
	if ceiling <= 0 {
		return 0
	}
	return rand.N(ceiling + 1)
}
