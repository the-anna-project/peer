package peer

import (
	"encoding/json"
)

func (p *peer) MarshalJSON() ([]byte, error) {
	type PeerClone peer

	b, err := json.Marshal(&struct {
		*PeerClone
	}{
		PeerClone: (*PeerClone)(p),
	})
	if err != nil {
		return nil, maskAny(err)
	}

	return b, nil
}

func (p *peer) UnmarshalJSON(b []byte) error {
	type PeerClone peer

	aux := &struct {
		*PeerClone
	}{
		PeerClone: (*PeerClone)(p),
	}
	err := json.Unmarshal(b, &aux)
	if err != nil {
		return maskAny(err)
	}

	return nil
}
