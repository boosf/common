package retrystrategy

import "time"

type RetryStrategy interface {
	WaitFor(attempt int64) time.Duration
}
