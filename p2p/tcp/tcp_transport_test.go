package tcp

import (
	"testing"
)

func TestTCPTransport(t *testing.T) {
	listenerAddr := ":8080"
	transport := NewTCPTransport(listenerAddr)

	err := transport.ListenAndAccept()
	if err != nil {
		t.Fatal("Failed to start TCP transport:", err)
	}
}
