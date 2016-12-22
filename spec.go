package peer

import (
	"encoding/json"
	"time"
)

type Peer interface {
	Created() time.Time
	ID() string
	json.Marshaler
	json.Unmarshaler
	Kind() string
	Value() string
}

// Service represents a peer service providing certain CRUD operations for peers
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
//         within the connection space.
//
type Service interface {
	Boot()
	Create(peerAValue string) (Peer, error)
	Delete(peerAValue string) error
	Exists(peerAValue string) (bool, error)
	Kind() string
	Position(peerAValue string) (Peer, error)
	Search(peerAValue string) (Peer, error)
	SearchByID(peerAID string) (Peer, error)
	Shutdown()
}
