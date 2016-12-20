package peer

import (
	"time"
)

// Config represents the configuration used to create a new peer.
type Config struct {
	// Settings.
	Created time.Time
	ID      string
	Kind    string
	Value   string
}

// DefaultConfig provides a default configuration to create a new peer by best
// effort.
func DefaultConfig() Config {
	return Config{
		// Settings.
		Created: time.Now(),
		ID:      "",
		Kind:    "",
		Value:   "",
	}
}

// New creates a new configured peer.
func New(config Config) (Peer, error) {
	// Dependencies.
	if config.Created.IsZero() {
		return nil, maskAnyf(invalidConfigError, "created must not be empty")
	}
	if config.ID == "" {
		return nil, maskAnyf(invalidConfigError, "ID must not be empty")
	}
	if config.Kind == "" {
		return nil, maskAnyf(invalidConfigError, "kind must not be empty")
	}
	if config.Value == "" {
		return nil, maskAnyf(invalidConfigError, "value must not be empty")
	}

	newPeer := &peer{
		// Settings.
		created: config.Created,
		id:      config.ID,
		kind:    config.Kind,
		value:   config.Value,
	}

	return newPeer, nil
}

type peer struct {
	// Settings.
	created time.Time
	id      string
	kind    string
	value   string
}

func (p *peer) Created() time.Time {
	return p.created
}

func (p *peer) ID() string {
	return p.id
}

func (p *peer) Kind() string {
	return p.kind
}

func (p *peer) Value() string {
	return p.value
}
