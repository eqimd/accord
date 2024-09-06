package main

import (
	"context"
	"log"
	"time"
)

type Service struct {
	name    string
	timeout time.Duration
	ctx     context.Context
	cancel  context.CancelFunc
}

func New(name string, timeout time.Duration) *Service {
	return &Service{name: name, timeout: timeout}
}

// this service has exited.  Start is not thread safe, do not call from multiple goroutines.
func (s *Service) Start() <-chan error {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	errc := make(chan error)
	go func() {
		defer close(errc)
		if err := s.run(); err != nil {
			errc <- err
		}
	}()
	return errc
}

func (s *Service) Stop() {
	s.cancel()
}

func (s *Service) run() error {
	log.Printf("service %s started", s.name)

	<-s.ctx.Done()
	log.Printf("service %s stopped", s.name)

	return nil
}
