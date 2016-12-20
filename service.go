// Package peer implements Service to manage peers inside the neural network.
package peer

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/the-anna-project/connection"
	"github.com/the-anna-project/id"
	"github.com/the-anna-project/index"
	"github.com/the-anna-project/position"
	storagecollection "github.com/the-anna-project/storage/collection"
	"github.com/the-anna-project/worker"
)

const (
	// KindBehaviour represents the kind of the visible behaviour peer.
	KindBehaviour = "behaviour"
	// KindInformation represents the kind of the visible information peer.
	KindInformation = "information"
	// KindPosition repesents the kind of the hidden position peer. Position peers
	// are managed implicitly internally. Each behaviour and each information peer
	// is automatically connected to a position peer. There is no position peer
	// implementation. The usage of KindPosition outside of this package is only
	// for kind veriffication purposes.
	KindPosition = "position"
	// NamespaceID represents the namespace used to scope keys of internal
	// indizes, where IDs are mapped to values.
	NamespaceID = "id"
	// NamespaceValue represents the namespace used to scope keys of internal
	// indizes, where values are mapped to IDs.
	NamespaceValue = "value"
)

// ConfigService represents the configuration used to create a new peer service.
type ServiceConfig struct {
	// Dependencies.
	ConnectionService connection.Service
	IDService         id.Service
	IndexService      index.Service
	PositionService   position.Service
	StorageCollection *storagecollection.Collection
	WorkerService     worker.Service

	// Settings.
	Kind string
}

// DefaultConfig provides a default configuration to create a new peer service
// by best effort.
func DefaultServiceConfig() ServiceConfig {
	var err error

	var connectionService connection.Service
	{
		connectionConfig := connection.DefaultServiceConfig()
		connectionService, err = connection.NewService(connectionConfig)
		if err != nil {
			panic(err)
		}
	}

	var idService id.Service
	{
		idConfig := id.DefaultServiceConfig()
		idService, err = id.NewService(idConfig)
		if err != nil {
			panic(err)
		}
	}

	var indexService index.Service
	{
		indexConfig := index.DefaultServiceConfig()
		indexService, err = index.NewService(indexConfig)
		if err != nil {
			panic(err)
		}
	}

	var positionService position.Service
	{
		positionConfig := position.DefaultServiceConfig()
		positionService, err = position.NewService(positionConfig)
		if err != nil {
			panic(err)
		}
	}

	var storageCollection *storagecollection.Collection
	{
		storageConfig := storagecollection.DefaultConfig()
		storageCollection, err = storagecollection.New(storageConfig)
		if err != nil {
			panic(err)
		}
	}

	var workerService worker.Service
	{
		workerConfig := worker.DefaultServiceConfig()
		workerService, err = worker.NewService(workerConfig)
		if err != nil {
			panic(err)
		}
	}

	config := ServiceConfig{
		// Dependencies.
		ConnectionService: connectionService,
		IDService:         idService,
		IndexService:      indexService,
		PositionService:   positionService,
		StorageCollection: storageCollection,
		WorkerService:     workerService,

		// Settings.
		Kind: "",
	}

	return config
}

// NewService creates a new peer service.
func NewService(config ServiceConfig) (Service, error) {
	// Dependencies.
	if config.ConnectionService == nil {
		return nil, maskAnyf(invalidConfigError, "connection service must not be empty")
	}
	if config.IDService == nil {
		return nil, maskAnyf(invalidConfigError, "id service must not be empty")
	}
	if config.IndexService == nil {
		return nil, maskAnyf(invalidConfigError, "index service must not be empty")
	}
	if config.PositionService == nil {
		return nil, maskAnyf(invalidConfigError, "position service must not be empty")
	}
	if config.StorageCollection == nil {
		return nil, maskAnyf(invalidConfigError, "storage collection must not be empty")
	}
	if config.WorkerService == nil {
		return nil, maskAnyf(invalidConfigError, "worker service must not be empty")
	}

	// Settings.
	if config.Kind == "" {
		return nil, maskAnyf(invalidConfigError, "kind must not be empty")
	}
	if config.Kind != KindBehaviour && config.Kind != KindInformation {
		return nil, maskAnyf(invalidConfigError, "kind must either be %s or %s", KindBehaviour, KindInformation)
	}

	newService := &service{
		// Dependencies.
		connection: config.ConnectionService,
		id:         config.IDService,
		index:      config.IndexService,
		position:   config.PositionService,
		storage:    config.StorageCollection,
		worker:     config.WorkerService,

		// Internals.
		bootOnce:     sync.Once{},
		closer:       make(chan struct{}, 1),
		shutdownOnce: sync.Once{},

		// Settings.
		kind: config.Kind,
	}

	return newService, nil
}

type service struct {
	// Dependencies.
	connection connection.Service
	id         id.Service
	index      index.Service
	position   position.Service
	storage    *storagecollection.Collection
	worker     worker.Service

	// Internals.
	bootOnce     sync.Once
	closer       chan struct{}
	shutdownOnce sync.Once

	// Settings.
	kind string
}

func (s *service) Boot() {
	s.bootOnce.Do(func() {
		// Service specific boot logic goes here.
	})
}

func (s *service) Create(peerAValue string) (Peer, error) {
	// Define the function wide global peer variables, where peerA is the peer
	// requested to be created, and peerB is its associated position peer.
	var peerA Peer
	var peerB Peer

	var err error

	// In case the peer requested to be created already exists, we don't need to
	// do anything but return it.
	peerA, err = s.Search(peerAValue)
	if IsNotFound(err) {
		// In case the peer does not exist, we can go ahead to create it.
	} else if err != nil {
		return nil, maskAny(err)
	}
	if peerA != nil {
		// In case we found a peer, we return it. That way we are idempotent.
		return peerA, nil
	}

	// Create the peer IDs.
	peerAID, err := s.id.New()
	if err != nil {
		return nil, maskAny(err)
	}
	peerBID, err := s.id.New()
	if err != nil {
		return nil, maskAny(err)
	}

	createActions := []func(canceler <-chan struct{}) error{
		// Create the actual peer.
		func(canceler <-chan struct{}) error {
			peerConfig := DefaultConfig()
			peerConfig.Created = time.Now()
			peerConfig.ID = peerAID
			peerConfig.Kind = s.Kind()
			peerConfig.Value = peerAValue
			newPeer, err := New(peerConfig)
			if err != nil {
				return maskAny(err)
			}
			peerA = newPeer
			b, err := json.Marshal(newPeer)
			if err != nil {
				return maskAny(err)
			}
			err = s.storage.Peer.Set(peerAID, string(b))
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Create the actual peer's position peer. Note that a peer's position is a
		// peer itself.
		func(canceler <-chan struct{}) error {
			// In case the peer does not exist, we can go ahead to create it.
			peerBValue, err := s.position.Default()
			if err != nil {
				return maskAny(err)
			}

			peerB, err = s.Search(peerBValue)
			if IsNotFound(err) {
				// In case the peer requested to be created already exists, we don't
				// need to do anything.
			} else if err != nil {
				return maskAny(err)
			}
			if peerB != nil {
				peerBID = peerB.ID()
				return nil
			}

			peerConfig := DefaultConfig()
			peerConfig.Created = time.Now()
			peerConfig.ID = peerBID
			peerConfig.Kind = KindPosition
			peerConfig.Value = peerBValue
			newPeer, err := New(peerConfig)
			if err != nil {
				return maskAny(err)
			}
			peerB = newPeer
			b, err := json.Marshal(newPeer)
			if err != nil {
				return maskAny(err)
			}
			err = s.storage.Peer.Set(peerBID, string(b))
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
	}

	// Execute the list of create actions asynchronously.
	executeConfig := s.worker.ExecuteConfig()
	executeConfig.Actions = createActions
	executeConfig.Canceler = s.closer
	executeConfig.NumWorkers = len(createActions)
	err = s.worker.Execute(executeConfig)
	if err != nil {
		return nil, maskAny(err)
	}

	indexActions := []func(canceler <-chan struct{}) error{
		// Create the index mapping between ID and value of peer A inside the
		// namespace identified by the peer's kind. Here peer A is the peer
		// requested to be created.
		func(canceler <-chan struct{}) error {
			err := s.index.Create(NamespaceID, peerA.Kind(), peerA.Kind(), peerA.ID(), peerA.Value())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Create the index mapping between ID and value of peer B inside the
		// namespace identified by the peer's kind. Here peer B is the peer
		// requested to be created.
		func(canceler <-chan struct{}) error {
			err := s.index.Create(NamespaceID, peerB.Kind(), peerB.Kind(), peerB.ID(), peerB.Value())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Create the index mapping between value and ID of peer A inside the
		// namespace identified by the peer's kind. Here peer A is the peer
		// requested to be created.
		func(canceler <-chan struct{}) error {
			err := s.index.Create(NamespaceValue, peerA.Kind(), peerA.Kind(), peerA.Value(), peerA.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Create the index mapping between value and ID of peer B inside the
		// namespace identified by the peer's kind. Here peer B is the peer
		// requested to be created.
		func(canceler <-chan struct{}) error {
			err := s.index.Create(NamespaceValue, peerB.Kind(), peerB.Kind(), peerB.Value(), peerB.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Create the connection between IDs of peer A and peer B. Here peer A is
		// the peer requested to be created, and peer B is its associated position
		// peer. This is a connection in one single direction.
		func(canceler <-chan struct{}) error {
			_, err := s.connection.Create(peerA.Kind(), peerB.Kind(), peerA.ID(), peerB.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Create the connection between IDs of peer B and peer A. Here peer A is
		// the peer requested to be created, and peer B is its associated position
		// peer. This is a connection in one single direction.
		func(canceler <-chan struct{}) error {
			_, err := s.connection.Create(peerB.Kind(), peerA.Kind(), peerB.ID(), peerA.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
	}

	// Execute the list of index actions asynchronously.
	executeConfig = s.worker.ExecuteConfig()
	executeConfig.Actions = indexActions
	executeConfig.Canceler = s.closer
	executeConfig.NumWorkers = len(indexActions)
	err = s.worker.Execute(executeConfig)
	if err != nil {
		return nil, maskAny(err)
	}

	return peerA, nil
}

func (s *service) Delete(peerAValue string) error {
	// Define the function wide global peer variables, where peerA is the peer
	// requested to be deleted, and peerB is its associated position peer. Further
	// we track the list of peers that are connected with peerA and peerB.
	// Connections associated with these peers have to be removed as well.
	var peerA Peer
	var peerB Peer
	var peerAPeers []string
	var peerBPeers []string

	// Check if the peer exists, to make sure we actually have to do something.
	ok, err := s.Exists(peerAValue)
	if err != nil {
		return maskAny(err)
	}
	if !ok {
		return nil
	}

	searchActions := []func(canceler <-chan struct{}) error{
		// Search for all the actual peer's associated peers.
		func(canceler <-chan struct{}) error {
			namespaces := [][]string{
				[]string{peerA.Kind(), KindBehaviour},
				[]string{peerA.Kind(), KindInformation},
				[]string{peerA.Kind(), KindPosition},
				[]string{KindBehaviour, peerA.Kind()},
				[]string{KindInformation, peerA.Kind()},
				[]string{KindPosition, peerA.Kind()},
			}
			for _, ns := range namespaces {
				peers, err := s.connection.SearchPeers(ns[0], ns[1], peerA.ID())
				if err != nil {
					return maskAny(err)
				}
				peerAPeers = append(peerAPeers, peers...)
			}

			return nil
		},
		// Search for the actual peer's position and all its associated peers.
		func(canceler <-chan struct{}) error {
			peerB, err = s.Position(peerAValue)
			if err != nil {
				return maskAny(err)
			}

			namespaces := [][]string{
				[]string{peerB.Kind(), KindBehaviour},
				[]string{peerB.Kind(), KindInformation},
				[]string{KindBehaviour, peerB.Kind()},
				[]string{KindInformation, peerB.Kind()},
			}
			for _, ns := range namespaces {
				peers, err := s.connection.SearchPeers(ns[0], ns[1], peerB.ID())
				if err != nil {
					return maskAny(err)
				}
				peerBPeers = append(peerBPeers, peers...)
			}

			return nil
		},
	}

	// Execute the list of search actions asynchronously.
	executeConfig := s.worker.ExecuteConfig()
	executeConfig.Actions = searchActions
	executeConfig.Canceler = s.closer
	executeConfig.NumWorkers = len(searchActions)
	err = s.worker.Execute(executeConfig)
	if err != nil {
		return maskAny(err)
	}

	deleteActions := []func(canceler <-chan struct{}) error{
		// Delete the actual peer.
		func(canceler <-chan struct{}) error {
			err := s.storage.Peer.Remove(peerA.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Delete the actual peer's position if not referenced by other peers.
		func(canceler <-chan struct{}) error {
			if len(peerBPeers) > 1 {
				// In case the list of peers connected with the position peer  contains
				// more than one peer, we stop here, because then other peers than the
				// actual peer are still connected to this position. Other referenced
				// peers must keep their position as it is. So we leave the position
				// here untouched.
				return nil
			}

			err := s.storage.Peer.Remove(peerB.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Delete the connection between the actual peer and its associated
		// position.
		func(canceler <-chan struct{}) error {
			err := s.connection.Delete(peerA.Kind(), peerB.Kind(), peerA.ID(), peerB.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Delete the connection between the peer's position and the actual peer.
		func(canceler <-chan struct{}) error {
			err := s.connection.Delete(peerB.Kind(), peerA.Kind(), peerB.ID(), peerA.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Delete the connections between the actual peer and the peers it is
		// connected to. This has to be done to not keep orphaned connections to
		// peers that do not exist anymore.
		func(canceler <-chan struct{}) error {
			namespaces := [][]string{
				[]string{peerA.Kind(), KindBehaviour},
				[]string{peerA.Kind(), KindInformation},
				[]string{KindBehaviour, peerA.Kind()},
				[]string{KindInformation, peerA.Kind()},
			}

			for _, peerID := range peerAPeers {
				for _, ns := range namespaces {
					err := s.connection.Delete(ns[0], ns[1], peerA.ID(), peerID)
					if err != nil {
						return maskAny(err)
					}
				}
			}

			return nil
		},
		// Delete the index mapping between the ID and value of peer A. Here peer A
		// is the position peer of the peer actually requested to be deleted.
		func(canceler <-chan struct{}) error {
			err := s.index.Delete(NamespaceID, peerA.Kind(), peerA.Kind(), peerA.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Delete the index mapping between the ID and value of peer B. Here peer B
		// is the position peer of the peer actually requested to be deleted.
		func(canceler <-chan struct{}) error {
			err := s.index.Delete(NamespaceID, peerB.Kind(), peerB.Kind(), peerB.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Delete the index mapping between the value and ID of peer A. Here peer A
		// is the position peer of the peer actually requested to be deleted.
		func(canceler <-chan struct{}) error {
			err := s.index.Delete(NamespaceValue, peerA.Kind(), peerA.Kind(), peerA.Value())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		// Delete the index mapping between the value and ID of peer B. Here peer B
		// is the position peer of the peer actually requested to be deleted.
		func(canceler <-chan struct{}) error {
			err := s.index.Delete(NamespaceValue, peerB.Kind(), peerB.Kind(), peerB.Value())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
	}

	executeConfig = s.worker.ExecuteConfig()
	executeConfig.Actions = deleteActions
	executeConfig.Canceler = s.closer
	executeConfig.NumWorkers = len(deleteActions)
	err = s.worker.Execute(executeConfig)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Exists(peerValue string) (bool, error) {
	_, err := s.Search(peerValue)
	if IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, maskAny(err)
	}

	return true, nil
}

func (s *service) Kind() string {
	return s.kind
}

func (s *service) Position(peerAValue string) (Peer, error) {
	peerA, err := s.Search(peerAValue)
	if err != nil {
		return nil, maskAny(err)
	}

	peerAPeers, err := s.connection.SearchPeers(peerA.Kind(), KindPosition, peerA.ID())
	if err != nil {
		return nil, maskAny(err)
	}
	if len(peerAPeers) != 1 {
		return nil, maskAnyf(invalidPositionError, "peer %s must have 1 position", peerAValue)
	}

	// The following code fetches the position peer by its ID. Note that we cannot
	// use Service.Search, since there are only peer service implementations for
	// behaviour and information peers. Therefore we would not find any position
	// peer using Service.Search.

	result, err := s.storage.Peer.Get(peerAPeers[0])
	if err != nil {
		return nil, maskAny(err)
	}

	peerB, err := New(DefaultConfig())
	if err != nil {
		return nil, maskAny(err)
	}
	err = json.Unmarshal([]byte(result), peerB)
	if err != nil {
		return nil, maskAny(err)
	}

	return peerB, nil
}

func (s *service) Search(peerValue string) (Peer, error) {
	peerID, err := s.index.Search(NamespaceValue, s.Kind(), s.Kind(), peerValue)
	if index.IsNotFound(err) {
		return nil, maskAnyf(notFoundError, peerValue)
	} else if err != nil {
		return nil, maskAny(err)
	}

	result, err := s.storage.Peer.Get(peerID)
	if err != nil {
		return nil, maskAny(err)
	}

	newPeer, err := New(DefaultConfig())
	if err != nil {
		return nil, maskAny(err)
	}
	err = json.Unmarshal([]byte(result), newPeer)
	if err != nil {
		return nil, maskAny(err)
	}

	return newPeer, nil
}

func (s *service) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}
