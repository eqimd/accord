package common

type Set[T comparable] map[T]struct{}

func (s Set[T]) Add(t T) {
	s[t] = struct{}{}
}

func (s Set[T]) Remove(t T) {
	delete(s, t)
}

func (s Set[T]) Exist(t T) bool {
	_, exist := s[t]

	return exist
}

func (s Set[T]) Union(other Set[T]) {
	for t := range other {
		s[t] = struct{}{}
	}
}
