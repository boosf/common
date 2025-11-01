package clock

type StubClock struct {
	now int64
}

func (s *StubClock) Now() int64 {
	return s.now
}
