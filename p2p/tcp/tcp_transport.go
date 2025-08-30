package tcp

import (
	"log"
	"net"
	"sync"

	"github.com/a-ZINC/DistFile/p2p"
	"github.com/a-ZINC/DistFile/p2p/decoder"
	"github.com/a-ZINC/DistFile/p2p/message"
)

type TCPPeer struct {
	conn    net.Conn
	inbound bool
}

type Config struct {
	ListenerAddr string
	HandShake    func(p2p.Peer) error
	MSGChan      chan *message.Message
	OnPeer      func(p2p.Peer) error
}

type TCPTransport struct {
	Config
	listener net.Listener

	mu    sync.RWMutex
	peers map[net.Addr]p2p.Peer
}

func NewTCPPeer(conn net.Conn, inbound bool) *TCPPeer {
	return &TCPPeer{
		conn:    conn,
		inbound: inbound,
	}
}

func NewTCPTransport(cfg Config) *TCPTransport {
	return &TCPTransport{
		Config: cfg,
		mu:    sync.RWMutex{},
	}
}

func (t *TCPTransport) ListenAndAccept() error {
	listener, err := net.Listen("tcp", t.Config.ListenerAddr)
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
	defer t.close(conn)
	peer := NewTCPPeer(conn, true)
	log.Printf("Peer: %v", peer)
	if err := t.Config.HandShake(peer); err != nil {
	}
	if t.OnPeer != nil {
		if err := t.OnPeer(peer); err != nil {
			return
		}
	}
	t.AddPeer(peer)
	for {
		message := &message.Message{}
		err := decoder.Decode(conn, message); 
		if err != nil {
			log.Printf("Error decoding message: %v", err)
			continue
		}
		t.MSGChan <- message
	}
}

func (t *TCPTransport) Close() {
	t.listener.Close()
}

func (t *TCPTransport) close(conn net.Conn) {
	log.Printf("Closing peer: %v", t.peers[conn.RemoteAddr()])
	if peer, ok := t.peers[conn.RemoteAddr()]; ok {
		t.mu.Lock()
		delete(t.peers, conn.RemoteAddr())
		t.mu.Unlock()
		if err := peer.Close(); err != nil {
			log.Printf("Error closing conn: %v", err)
		}
	}
	log.Printf("Closed peer: %v", t.peers[conn.RemoteAddr()])
}

func (t *TCPTransport) AddPeer(peer *TCPPeer) {
	t.mu.Lock()
	t.peers[peer.conn.RemoteAddr()] = peer
	t.mu.Unlock()
	log.Printf("Added Peer: %v", peer)
}

func (p *TCPPeer) Close() error {
	if err := p.conn.Close(); err != nil {
		return err
	}
	return nil
}
