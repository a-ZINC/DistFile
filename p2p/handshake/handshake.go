package handshake

import "github.com/a-ZINC/DistFile/p2p"

func NOPHandshake(p2p.Peer) error {
	// No-operation handshake
	return nil
}