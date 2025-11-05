package clock

import "time"

type Client interface {
	UnixNow() int64
	FromNow(time.Duration) time.Time
}
