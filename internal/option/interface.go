package option

type Option[T any] interface {
	Apply(option T) T
}
