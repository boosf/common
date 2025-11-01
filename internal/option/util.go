package option

func ApplyOptions[T Option[T]](base T, options []T) T {
	for _, option := range options {
		base = base.Apply(option)
	}
	return base
}
