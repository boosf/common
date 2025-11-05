package clock

import "time"

func New() Client {
	return &client{}
}

type client struct {
}

func (c *client) UnixNow() int64 {
	return time.Now().Unix()
}

func (c *client) FromNow(duration time.Duration) time.Time {
	unixTime := c.UnixNow() + int64(duration.Seconds())
	return time.Unix(unixTime, 0)
}
