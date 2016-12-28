package peer

import (
	"encoding/json"
	"sync"

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
	var peerAID string
	var peerBID string

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

	// Create the actual new peer and its new position peer and execute the list
	// of actions asynchronously.
	{
		actions := []func(canceler <-chan struct{}) error{
			// Create the actual peer. This will be a peer having a kind equal to the
			// kind of the executing service. That means, e.g. an behaviour peer
			// service implementation creates behaviour peers.
			func(canceler <-chan struct{}) error {
				peerAID, err = s.id.New()
				if err != nil {
					return maskAny(err)
				}

				peerConfig := DefaultConfig()
				peerConfig.ID = peerAID
				peerConfig.Kind = s.Kind()
				peerConfig.Value = peerAValue
				peer, err := New(peerConfig)
				if err != nil {
					return maskAny(err)
				}
				peerA = peer

				b, err := json.Marshal(peer)
				if err != nil {
					return maskAny(err)
				}
				err = s.storage.Peer.Set(peerAID, string(b))
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
			// Create the actual peer's position peer. Note that a peer's position
			// peer is a usual peer itself, but having KindPosition. It represents the
			// position of the actual peer within the connection space of the neural
			// network.
			func(canceler <-chan struct{}) error {
				peerBID, err = s.id.New()
				if err != nil {
					return maskAny(err)
				}

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
				peerConfig.ID = peerBID
				peerConfig.Kind = KindPosition
				peerConfig.Value = peerBValue
				peer, err := New(peerConfig)
				if err != nil {
					return maskAny(err)
				}
				peerB = peer

				b, err := json.Marshal(peerB)
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

		executeConfig := s.worker.ExecuteConfig()
		executeConfig.Actions = actions
		executeConfig.Canceler = s.closer
		executeConfig.NumWorkers = len(actions)
		err = s.worker.Execute(executeConfig)
		if err != nil {
			return nil, maskAny(err)
		}
	}

	// Connect the actual new peer with its new position peer and execute the list
	// of actions asynchronously. This is used to connect the two given peers.
	{
		var actions []func(canceler <-chan struct{}) error
		actions = append(actions, s.newIndexActions(peerA, peerB)...)
		actions = append(actions, s.newConnectActions(peerA, peerB)...)

		executeConfig := s.worker.ExecuteConfig()
		executeConfig.Actions = actions
		executeConfig.Canceler = s.closer
		executeConfig.NumWorkers = len(actions)
		err = s.worker.Execute(executeConfig)
		if err != nil {
			return nil, maskAny(err)
		}
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

	var err error

	// In case the peer requested to be deleted does not exist, we don't need to
	// do anything but return without throwing an error to be idempotent. In case
	// a deprecated peer is requested to be deleted we do not want to deal with
	// the deprecated error. That is why we have to use the private Service.search
	// method, because the public Service.Search throws the deprecated error and
	// does not return the deprecated peer.
	peerA, err = s.search(peerAValue)
	if IsNotFound(err) {
		return nil
	} else if err != nil {
		return maskAny(err)
	}
	peerB, err = s.Position(peerAValue)
	if err != nil {
		return maskAny(err)
	}

	allNamespaces := s.allNamespaces(peerA)

	// Search for the actual peer with all its references and execute the list of
	// actions asynchronously. This collects all necessary information to clean up
	// the underlying storage in the next step.
	{
		actions := []func(canceler <-chan struct{}) error{
			// Search for all the actual peer's associated peers.
			func(canceler <-chan struct{}) error {
				peerAPeers, err = s.searchAllPeers(peerA)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
			// Search for the actual peer's position and all its associated peers.
			func(canceler <-chan struct{}) error {
				peerBPeers, err = s.searchAllPeers(peerA)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
		}

		executeConfig := s.worker.ExecuteConfig()
		executeConfig.Actions = actions
		executeConfig.Canceler = s.closer
		executeConfig.NumWorkers = len(actions)
		err = s.worker.Execute(executeConfig)
		if err != nil {
			return maskAny(err)
		}
	}

	// Delete the actual peer with all its references and execute the list of
	// actions asynchronously. This cleans up the underlying storage.
	{
		actions := []func(canceler <-chan struct{}) error{
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
			// Delete the connections between the actual peer and the peers it is
			// connected to. This has to be done to not keep orphaned connections to
			// peers that do not exist anymore.
			func(canceler <-chan struct{}) error {
				for _, peerID := range peerAPeers {
					for _, ns := range allNamespaces {
						err := s.connection.Delete(ns[0], ns[1], peerA.ID(), peerID)
						if err != nil {
							return maskAny(err)
						}
					}
				}

				return nil
			},
			// Delete the connections between the peers the actual peer is connected
			// to and the actual. This has to be done to not keep orphaned connections
			// to peers that do not exist anymore.
			func(canceler <-chan struct{}) error {
				for _, peerID := range peerAPeers {
					for _, ns := range allNamespaces {
						err := s.connection.Delete(ns[0], ns[1], peerID, peerA.ID())
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

		executeConfig := s.worker.ExecuteConfig()
		executeConfig.Actions = actions
		executeConfig.Canceler = s.closer
		executeConfig.NumWorkers = len(actions)
		err = s.worker.Execute(executeConfig)
		if err != nil {
			return maskAny(err)
		}
	}

	return nil
}

func (s *service) Exists(peerValue string) (bool, error) {
	_, err := s.Search(peerValue)
	if IsNotFound(err) {
		return false, nil
	} else if IsDeprecated(err) {
		return true, nil
	} else if err != nil {
		return false, maskAny(err)
	}

	return true, nil
}

func (s *service) Kind() string {
	return s.kind
}

func (s *service) Mutate(peerAValue, newPeerAValue, newPeerBValue string) (Peer, Peer, error) {
	// Search for the actual peer which is requested to be mutated. Note that we
	// here might return a not found error or a deprecated error. If we find a
	// valid peer, this peer will be prepared to only act as proxy to its mutated
	// peers until it is removed at some point.
	peerA, err := s.Search(peerAValue)
	if err != nil {
		return nil, nil, maskAny(err)
	}

	// Define the function wide global peer variables, where newPeerA is the first
	// peer resulting out of the mutation, and newPeerB is the second peer
	// resulting out of the mutation.
	var newPeerA Peer
	var newPeerB Peer

	// Create the two new peers resulting out of the mutation and execute the list
	// of actions asynchronously. This is used to connect the two given peers.
	{
		actions := []func(canceler <-chan struct{}) error{
			func(canceler <-chan struct{}) error {
				newPeerA, err = s.Create(newPeerAValue)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
			func(canceler <-chan struct{}) error {
				newPeerB, err = s.Create(newPeerBValue)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
		}

		executeConfig := s.worker.ExecuteConfig()
		executeConfig.Actions = actions
		executeConfig.Canceler = s.closer
		executeConfig.NumWorkers = len(actions)
		err = s.worker.Execute(executeConfig)
		if err != nil {
			return nil, nil, maskAny(err)
		}
	}

	// Process the mutation asynchronously.
	{
		actions := []func(canceler <-chan struct{}) error{
			// Connect the actual peer with the first new peer and execute the list of
			// actions asynchronously. This is used to connect the two given peers.
			func(canceler <-chan struct{}) error {
				var actions []func(canceler <-chan struct{}) error
				actions = append(actions, s.newIndexActions(peerA, newPeerA)...)
				actions = append(actions, s.newConnectActions(peerA, newPeerA)...)

				executeConfig := s.worker.ExecuteConfig()
				executeConfig.Actions = actions
				executeConfig.Canceler = s.closer
				executeConfig.NumWorkers = len(actions)
				err = s.worker.Execute(executeConfig)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
			// Connect the first new peer with the second new peer and execute the
			// list of actions asynchronously. This is used to connect the two given
			// peers.
			func(canceler <-chan struct{}) error {
				var actions []func(canceler <-chan struct{}) error
				actions = append(actions, s.newIndexActions(newPeerA, newPeerB)...)
				actions = append(actions, s.newConnectActions(newPeerA, newPeerB)...)

				executeConfig := s.worker.ExecuteConfig()
				executeConfig.Actions = actions
				executeConfig.Canceler = s.closer
				executeConfig.NumWorkers = len(actions)
				err = s.worker.Execute(executeConfig)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
			// Lookup the connections of the peer which is requested to be mutated and
			// apply them to the first peer resulting out of the mutation.
			func(canceler <-chan struct{}) error {
				err = s.movePeers(peerA, newPeerA)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
			// Lookup the position of the peer which is requested to be mutated and
			// apply it to the first peer resulting out of the mutation.
			func(canceler <-chan struct{}) error {
				err = s.movePosition(peerA, newPeerA)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
			// Lookup the position of the peer which is requested to be mutated and
			// apply it to the second peer resulting out of the mutation.
			func(canceler <-chan struct{}) error {
				err = s.movePosition(peerA, newPeerB)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
			// Mark the the peer which is requested to be mutated as deprecated and
			// erase its actua value.
			func(canceler <-chan struct{}) error {
				err = s.markDeprecated(peerA)
				if err != nil {
					return maskAny(err)
				}

				return nil
			},
		}

		executeConfig := s.worker.ExecuteConfig()
		executeConfig.Actions = actions
		executeConfig.Canceler = s.closer
		executeConfig.NumWorkers = len(actions)
		err = s.worker.Execute(executeConfig)
		if err != nil {
			return nil, nil, maskAny(err)
		}
	}

	return newPeerA, newPeerB, nil
}

func (s *service) Position(peerAValue string) (Peer, error) {
	// We lookup the peer we want to find its position for. In case a deprecated
	// peer is lookued up, we do not want to deal with the deprecated error. That
	// is why we have to use the private Service.search method, because the public
	// Service.Search throws the deprecated error and does not return the
	// deprecated peer.
	peerA, err := s.search(peerAValue)
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

func (s *service) Random() (Peer, error) {
	var count int

	for {
		// Make sure we only try finding a random peer to a certain extend.
		count++
		if count > 10 {
			return nil, maskAnyf(notFoundError, "no valid random peer exists")
		}

		// Look for a random key within the peer storage. We store peers using their
		// plain peer ID. That is why we can easily use the storage's Random method.
		result, err := s.storage.Peer.GetRandom()
		if err != nil {
			return nil, maskAny(err)
		}

		// Use the random key to fetch the associated peer.
		peer, err := s.SearchByID(result)
		if err != nil {
			return nil, maskAny(err)
		}

		// We do not want to return deprecated peers, so we continue to find a valid
		// peer for the requestor.
		if peer.Deprecated() {
			continue
		}

		// We found a valid peer, so we go with it.
		return peer, nil
	}
}

func (s *service) Search(peerValue string) (Peer, error) {
	peer, err := s.search(peerValue)
	if err != nil {
		return nil, maskAny(err)
	}

	if peer.Deprecated() {
		return nil, maskAnyf(deprecatedError, peerValue)
	}

	return peer, nil
}

func (s *service) SearchByID(peerID string) (Peer, error) {
	peerValue, err := s.index.Search(NamespaceID, s.Kind(), s.Kind(), peerID)
	if index.IsNotFound(err) {
		return nil, maskAnyf(notFoundError, peerValue)
	} else if err != nil {
		return nil, maskAny(err)
	}

	peer, err := s.Search(peerValue)
	if err != nil {
		return nil, maskAny(err)
	}

	return peer, nil
}

func (s *service) SearchPath(peerValues ...string) ([]Peer, error) {
	var peers []Peer

	for i, v := range peerValues {
		peer, err := s.Search(v)
		if err != nil {
			return nil, maskAny(err)
		}
		peers = append(peers, peer)

		if i == 0 {
			continue
		}

		peerA := peers[i-1]
		peerB := peers[i]

		ok, err := s.connection.Exists(peerA.Kind(), peerB.Kind(), peerA.ID(), peerB.ID())
		if err != nil {
			return nil, maskAny(err)
		}
		if !ok {
			return nil, maskAnyf(notFoundError, "%#v", peerValues)
		}
	}

	return peers, nil
}

func (s *service) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}

// allNamespaces returns a list of all combinations of all possible namespaces.
// This includes the position namespace and all reverse combinations.
func (s *service) allNamespaces(peer Peer) [][]string {
	return [][]string{
		[]string{peer.Kind(), KindBehaviour},
		[]string{peer.Kind(), KindInformation},
		[]string{peer.Kind(), KindPosition},
		[]string{KindBehaviour, peer.Kind()},
		[]string{KindInformation, peer.Kind()},
		[]string{KindPosition, peer.Kind()},
	}
}

// markDeprecated marks the given peer as being deprecated and erases its value.
func (s *service) markDeprecated(peer Peer) error {
	// Mark the given peer as being deprecated and erase its value.
	peer.SetDeprecated(true)
	peer.SetValue("")

	// Store the updated peer.
	b, err := json.Marshal(peer)
	if err != nil {
		return maskAny(err)
	}
	err = s.storage.Peer.Set(peer.ID(), string(b))
	if err != nil {
		return maskAny(err)
	}

	return nil
}

// movePeers applies the connections of peerA to peerB.
func (s *service) movePeers(peerA, peerB Peer) error {
	namespaces := [][]string{
		[]string{peerA.Kind(), KindBehaviour},
		[]string{peerA.Kind(), KindInformation},
	}

	for _, ns := range namespaces {
		peers, err := s.connection.SearchPeers(ns[0], ns[1], peerA.ID())
		if connection.IsNotFound(err) {
			// We lookup all possible combinations of namespaces. There might be
			// no peers for some namespace combinations. So we can savely ignore
			// the error and go ahead looking for the next.
			continue
		} else if err != nil {
			return maskAny(err)
		}

		for _, peerID := range peers {
			_, err := s.connection.Create(ns[0], ns[1], peerB.ID(), peerID)
			if err != nil {
				return maskAny(err)
			}
		}
		for _, peerID := range peers {
			_, err := s.connection.Create(ns[1], ns[0], peerID, peerB.ID())
			if err != nil {
				return maskAny(err)
			}
		}
	}

	return nil
}

// movePosition applies the position value of the position peer of peerA to the
// position peer of peerB.
func (s *service) movePosition(peerA, peerB Peer) error {
	// Fetch the position peer of the peer requested to be used to update the
	// position of the other peer.
	positionA, err := s.Position(peerA.Value())
	if err != nil {
		return maskAny(err)
	}

	// Fetch the position peer of the peer requested to get its position updated.
	positionB, err := s.Position(peerB.Value())
	if err != nil {
		return maskAny(err)
	}

	// Update the position peer value.
	positionB.SetValue(positionA.Value())

	// Store the updated position peer.
	b, err := json.Marshal(positionB)
	if err != nil {
		return maskAny(err)
	}
	err = s.storage.Peer.Set(positionB.ID(), string(b))
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) newConnectActions(peerA, peerB Peer) []func(canceler <-chan struct{}) error {
	return []func(canceler <-chan struct{}) error{
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
}

func (s *service) newIndexActions(peerA, peerB Peer) []func(canceler <-chan struct{}) error {
	return []func(canceler <-chan struct{}) error{
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
	}
}

// search looks up the peer associated to the given peer value.
func (s *service) search(peerValue string) (Peer, error) {
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

	peer, err := New(DefaultConfig())
	if err != nil {
		return nil, maskAny(err)
	}
	err = json.Unmarshal([]byte(result), peer)
	if err != nil {
		return nil, maskAny(err)
	}

	return peer, nil
}

// searchAllPeers looks up all connected peers of the given peers. Therefore all
// namespaces are searched.
func (s *service) searchAllPeers(peer Peer) ([]string, error) {
	var allPeers []string

	for _, ns := range s.allNamespaces(peer) {
		peers, err := s.connection.SearchPeers(ns[0], ns[1], peer.ID())
		if connection.IsNotFound(err) {
			// We lookup all possible combinations of namespaces. There might be
			// no peers for some namespace combinations. So we can savely ignore
			// the error and go ahead looking for the next.
			continue
		} else if err != nil {
			return nil, maskAny(err)
		}
		allPeers = append(allPeers, peers...)
	}

	return allPeers, nil
}
