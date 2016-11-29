// Package service provides a service to manage peers within the connection
// space.
package service

import (
	"fmt"
	"sync"
	"time"

	servicespec "github.com/the-anna-project/spec/service"
)

// New creates a new peer service.
func New() servicespec.PeerService {
	return &service{
		// Dependencies.
		serviceCollection: nil,

		// Settings.
		closer:       make(chan struct{}, 1),
		metadata:     map[string]string{},
		shutdownOnce: sync.Once{},
	}
}

type service struct {
	// Dependencies.
	serviceCollection servicespec.ServiceCollection

	// Settings.
	closer       chan struct{}
	metadata     map[string]string
	shutdownOnce sync.Once
}

func (s *service) Boot() {
	id, err := s.Service().ID().New()
	if err != nil {
		panic(err)
	}
	s.metadata = map[string]string{
		"id":   id,
		"name": "peer",
		"type": "service",
	}
}

func (s *service) Create(peer string) error {
	s.Service().Log().Line("func", "Create")

	key := fmt.Sprintf("peer:%s", peer)

	seconds := fmt.Sprintf("%d", time.Now().Unix())
	val := map[string]string{
		"created": seconds,
	}

	err := s.Service().Storage().Peer().SetStringMap(key, val)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Delete(peer string) error {
	s.Service().Log().Line("func", "Delete")

	key := fmt.Sprintf("peer:%s", peer)

	err := s.Service().Storage().Peer().Remove(key)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Search(peer string) (map[string]string, error) {
	s.Service().Log().Line("func", "Search")

	key := fmt.Sprintf("peer:%s", peer)

	result, err := s.Service().Storage().Peer().GetStringMap(key)
	if err != nil {
		return nil, maskAny(err)
	}

	if len(result) == 0 {
		return nil, maskAny(peerNotFoundError)
	}

	return result, nil
}

func (s *service) Metadata() map[string]string {
	return s.metadata
}

func (s *service) Service() servicespec.ServiceCollection {
	return s.serviceCollection
}

func (s *service) SetServiceCollection(sc servicespec.ServiceCollection) {
	s.serviceCollection = sc
}

func (s *service) Shutdown() {
	s.Service().Log().Line("func", "Shutdown")

	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}
