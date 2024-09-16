package storage

import (
	"maps"
	"sync"
)

type Storage struct {
	mu sync.Mutex
	m  map[string]string
}

func NewStorage() *Storage {
	return &Storage{
		m: map[string]string{},
	}
}

func (s *Storage) Get(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.m[key]
}

func (s *Storage) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[key] = value
}

func (s *Storage) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	cloned := maps.Clone(s.m)
	return cloned
}
