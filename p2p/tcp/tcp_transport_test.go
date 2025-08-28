package tcp

import (
	"testing"

	"github.com/a-ZINC/DistFile/p2p/handshake"
)

func TestTCPTransport(t *testing.T) {
	cfg := Config{
		ListenerAddr: ":8080",
		HandShake:    handshake.NOPHandshake,
	}
	transport := NewTCPTransport(cfg)

	err := transport.ListenAndAccept()
	if err != nil {
		t.Fatal("Failed to start TCP transport:", err)
	}
}
