package clock

import "time"

type Client interface {
	UnixNow() int64
	TimeFromNow(time.Duration) time.Time
}
