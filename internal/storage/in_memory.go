package storage

import (
	"maps"
	"sync"
)

type InMemory struct {
	mu sync.RWMutex
	m  map[string]string
}

func NewInMemory() *InMemory {
	return &InMemory{
		m: map[string]string{},
	}
}

func (s *InMemory) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.m[key], nil
}

func (s *InMemory) GetBatch(keys []string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = s.m[k]
	}

	return vals, nil
}

func (s *InMemory) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[key] = value

	return nil
}

func (s *InMemory) SetBatch(kv map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range kv {
		s.m[k] = v
	}

	return nil
}

func (s *InMemory) Snapshot() (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cloned := maps.Clone(s.m)

	return cloned, nil
}
