package retrystrategy

import (
	"math"
	"math/rand/v2"
	"time"
)

func NewExponentialBackoffWithJitterRetryStrategy(base time.Duration, max time.Duration) RetryStrategy {
	return &exponentialBackoffWithJitterRetryStrategy{
		baseDelay: base,
		maxDelay:  max,
	}
}

type exponentialBackoffWithJitterRetryStrategy struct {
	baseDelay time.Duration
	maxDelay  time.Duration
}

func (b *exponentialBackoffWithJitterRetryStrategy) WaitFor(attempt int64) time.Duration {
	exp := float64(b.baseDelay) * math.Pow(2, float64(attempt))
	delay := time.Duration(exp)
	if delay > b.maxDelay {
		delay = b.maxDelay
	}
	halfDelay := delay / 2
	jitter := time.Duration(halfDelay) + time.Duration(rand.Int64N(int64(delay)))
	return jitter
}
