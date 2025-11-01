package clock

import "time"

func New() Clock {
	return &clock{}
}

type clock struct {
}

func (c *clock) Now() int64 {
	return time.Now().Unix()
}
