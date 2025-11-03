package clock

type Client interface {
	UnixNow() int64
}
