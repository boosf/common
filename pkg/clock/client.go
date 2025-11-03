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
