package clock

type Client interface {
	Now() int64
}
