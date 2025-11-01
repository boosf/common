package clock

import "time"

func New() Client {
	return &client{}
}

type client struct {
}

func (c *client) Now() int64 {
	return time.Now().Unix()
}
