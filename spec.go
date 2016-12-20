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

type Service interface {
	Boot()
	Create(peerAValue string) (Peer, error)
	Delete(peerAValue string) error
	Exists(peerAValue string) (bool, error)
	Kind() string
	Position(peerAValue string) (Peer, error)
	Search(peerAValue string) (Peer, error)
	Shutdown()
}
