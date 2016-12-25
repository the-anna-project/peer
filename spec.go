package peer

import (
	"encoding/json"
	"time"
)

// Peer represents a single peer within the connection space of the neural
// network.
type Peer interface {
	// Created returns the creation time of the current peer.
	Created() time.Time
	// Deprecated returns the deprecation state of the current peer. See
	// Service.Mutate and Service.Search for more information.
	Deprecated() bool
	// ID returns the ID of the current peer. This ID is used to store the peer
	// under the hood of the peer service. In case of a behaviour peer, this ID is
	// also referenced as the behaviour ID, an ID for the specific implementation
	// of a CLG. In case of a information peer, this ID is also referenced as the
	// information ID, an ID for some specific, resusable feature.
	ID() string
	json.Marshaler
	json.Unmarshaler
	// Kind returns the kind of the peer. This must be either KindBehaviour or
	// KindInformation.
	Kind() string
	// SetDeprecated sets the deprecated field of the current peer to the given
	// value.
	SetDeprecated(deprecated bool)
	// SetValue sets the value field of the current peer to the given value.
	SetValue(value string)
	// Value returns the actual peer value. In case of a behaviour peer, this
	// value is also referenced as behaviour name, or CLG name. In case of an
	// information peer, this value is also referenced as some specific, resusable
	// feature.
	Value() string
}

// Service represents a peer service providing certain operations for peers
// within the connection space. The inner working of the peer concept are quite
// complex on the implementation level. There are three different kinds of peers
// interacting with each other. Each peer has its own functional and semantic
// meaning. Different peer kinds may or may not be connected, depending on their
// kind and meaning. These are the kinds of peers being implemented.
//
//     behaviour
//
//         The behaviour peers are managed by the behaviour peer service
//         implementation. Subject of these peers is to reference implemented
//         behaviours. Note that behaviour is implemented in form of CLG
//         services. See https://github.com/the-anna-project/clg.Service.
//
//     information
//
//         The information peers are managed by the information peer service
//         implementation. Subject of these peers is to reference all kind of
//         information. Note that information is provided in form of input,
//         which is received via input services. See
//         https://github.com/the-anna-project/input.Service. Further
//         information is calculated by the neural networks.
//
//     position
//
//         The position peers are managed implicitly. Subject of these peers is
//         is to represent the position of behaviour and information peers
//         within the connection space. That means, each behaviour and each
//         information peer has automatically an position peer connected to
//         itself. Note that connection peers are never connected to each other.
//
type Service interface {
	// Boot initializes and starts the whole service like booting a machine. The
	// call to Boot blocks until the service is completely initialized, so you
	// might want to call it in a separate goroutine.
	Boot()
	// Create creates a new peer for the given peer value. Additionally Create
	// ensures that all necessary references and connections related to a new peer
	// are constructued.
	Create(peerAValue string) (Peer, error)
	// Delete removes the peer identified by the given peer value. Additionally
	// Delete ensures that all necessary references and connections related to the
	// identified peer are cleaned up.
	Delete(peerAValue string) error
	// Exists checks whether the peer identified by the given peer value exists.
	// Exists returns also true in case the peer being looked up is deprecated.
	Exists(peerAValue string) (bool, error)
	// Kind returns the kind of the current service implementation. This must be
	// either KindBehaviour or KindInformation.
	Kind() string
	// Mutate implements an evolutional primitive. The purpose of Mutate is to
	// provide functionality of improving peers and their connections within the
	// connection space of the neural network.
	//
	// Mutate looks up the peer identified by the first given peer value. This
	// peer is the target intended to be mutated. The mutation process "splits"
	// the target peer into two. Therefore two new peers are created, having the
	// same kind as the target peer. The two new peers get the second and third
	// provided peer values applied. The target peer is marked as deprecated. The
	// target peer furthermore only acts as proxy to the two new peers.
	// Connections related to the three peers described here are updated
	// accordingly to guaranty the neural network operable.
	//
	// Mutate may never be used in combination with peers of kind KindBehaviour.
	Mutate(peerAValue, newPeerAValue, newPeerBValue string) (Peer, Peer, error)
	// Position looks up the position of the peer identified by the given peer
	// value. The peer returned by Position is always a position peer. The call to
	// Peer.Value of a position peer must always return the position of the
	// initially referenced peer. This behaviour applies to all kind of peers,
	// whether they are of the kind KindBehaviour or KindInformation.
	Position(peerAValue string) (Peer, error)
	// Search looks up the peer identified by the given peer value. Therefore the
	// given peer value is used to resolve the actual peer ID. The mapping between
	// peer values and peer IDs is managemed by the index service. Note that
	// peers, regardless of their kind, are always stored and looked up by their
	// ID.
	//
	// Search may return an deprecate error in case the given peer value
	// references a deprecated peer. In that case, clients using the management
	// primitives of the peer service are adviced to handle the deprecate error by
	// reevaluating the connections of the associated peers using the connection
	// service accordingly.
	Search(peerAValue string) (Peer, error)
	// SearchByID is like Search, with the exception that it uses a peer's ID for
	// the initial index lookup.
	SearchByID(peerAID string) (Peer, error)
	// Shutdown ends all processes of the service like shutting down a machine.
	// The call to Shutdown blocks until the service is completely shut down, so
	// you might want to call it in a separate goroutine.
	Shutdown()
}
