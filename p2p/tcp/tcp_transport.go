package tcp

import (
	"log"
	"net"
	"sync"

	"github.com/a-ZINC/DistFile/p2p"
	"github.com/a-ZINC/DistFile/p2p/handshake"
)

type TCPPeer struct {
	conn    net.Conn
	inbound bool
}

type TCPTransport struct {
	listenerAddr string
	listener     net.Listener
	handShake    func(p2p.Peer) error

	mu    sync.RWMutex
	peers map[net.Addr]p2p.Peer
}

func NewTCPPeer(conn net.Conn, inbound bool) *TCPPeer {
	return &TCPPeer{
		conn:    conn,
		inbound: inbound,
	}
}

func NewTCPTransport(listenerAddr string) *TCPTransport {
	return &TCPTransport{
		listenerAddr: listenerAddr,
		handShake:    handshake.NOPHandshake,
	}
}

func (t *TCPTransport) ListenAndAccept() error {
	listener, err := net.Listen("tcp", t.listenerAddr)
	if err != nil {
		return err
	}
	t.listener = listener
	t.peers = make(map[net.Addr]p2p.Peer)

	go t.handleAccept()
	return nil
}

func (t *TCPTransport) handleAccept() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go t.handleConnection(conn)
	}
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer t.handleConnClose(conn)
	peer := NewTCPPeer(conn, true)
	log.Printf("Peer: %v", peer)
	if err := t.handShake(peer); err != nil {
	}
	t.AddPeer(peer)

	// Handle the incoming connection
}

func (t *TCPTransport) handleConnClose(conn net.Conn) {
	log.Printf("Closing peer: %v", t.peers[conn.RemoteAddr()])
	if _, ok := t.peers[conn.RemoteAddr()]; ok {
		t.mu.Lock()
		delete(t.peers, conn.RemoteAddr())
		t.mu.Unlock()
	}
	log.Printf("Closed peer: %v", t.peers[conn.RemoteAddr()])
	// Close the connection
	conn.Close()
}

func (t *TCPTransport) AddPeer(peer *TCPPeer) {
	t.mu.Lock()
	t.peers[peer.conn.RemoteAddr()] = peer
	t.mu.Unlock()
	log.Printf("Added Peer: %v", t.peers[peer.conn.RemoteAddr()])
}
