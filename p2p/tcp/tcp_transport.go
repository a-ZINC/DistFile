package tcp

import (
	"errors"
	"log"
	"net"
	"sync"

	"github.com/a-ZINC/DistFile/p2p"
	"github.com/a-ZINC/DistFile/p2p/decoder"
	"github.com/a-ZINC/DistFile/p2p/message"
)

type TCPPeer struct {
	net.Conn
	inbound bool
}

type Config struct {
	ListenerAddr string
	HandShake    func(p2p.Peer) error
	MSGChan      chan *message.Message
	OnPeer       func(p2p.Peer) error
}

type TCPTransport struct {
	Config
	Listener net.Listener

	Mu    sync.RWMutex
	Peers map[string]p2p.Peer

	Wg sync.WaitGroup
}

func NewTCPPeer(conn net.Conn, inbound bool) *TCPPeer {
	return &TCPPeer{
		Conn:    conn,
		inbound: inbound,
	}
}

func NewTCPTransport(cfg Config) *TCPTransport {
	return &TCPTransport{
		Config: cfg,
		Mu:     sync.RWMutex{},
		Wg:     sync.WaitGroup{},
	}
}

func (t *TCPTransport) ListenAndAccept() error {
	listener, err := net.Listen("tcp", t.Config.ListenerAddr)
	if err != nil {
		return err
	}
	log.Printf("Listening on %s", t.Config.ListenerAddr)
	t.Listener = listener
	t.Peers = make(map[string]p2p.Peer)

	go t.handleAccept()
	return nil
}

func (t *TCPTransport) handleAccept() {
	for {
		conn, err := t.Listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go t.handleConnection(conn, true)
	}
}

func (t *TCPTransport) handleConnection(conn net.Conn, inbound bool) {
	defer t.close(conn)
	peer := NewTCPPeer(conn, inbound)

	if err := t.Config.HandShake(peer); err != nil {
	}
	if t.OnPeer != nil {
		if err := t.OnPeer(peer); err != nil {
			log.Printf("OnPeer error: %v", err)
			return
		}
	}
	for {
		message := &message.Message{}
		err := decoder.Decode(conn, message)
		if err != nil {
			log.Printf("Error decoding message: %v", err)
			continue
		}
		if message.IsStream {
			log.Printf("Received stream message: %v", message)
			t.Wg.Add(1)
			t.Wg.Wait()
			log.Printf("Stream message processed: %v", message)
			continue
		}
		t.Config.MSGChan <- message
		log.Printf("Normal messages released: %v", message)
	}
}

func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	t.handleConnection(conn, false)
	return nil
}

func (t *TCPTransport) Close() {
	t.Listener.Close()
}

func (t *TCPTransport) close(conn net.Conn) {
	if peer, ok := t.Peers[conn.RemoteAddr().String()]; ok {
		t.Mu.Lock()
		delete(t.Peers, conn.RemoteAddr().String())
		t.Mu.Unlock()
		if err := peer.Close(); err != nil {
			log.Printf("Error closing conn: %v", err)
		}
	}
	log.Printf("Closed peer: %v", t.Peers[conn.RemoteAddr().String()])
}

/*
----------------------------------------------------------------------------------------------------------------------
--------------------------------------------------- TCPPeer ----------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
*/

func (p *TCPPeer) IsInbound() bool {
	return p.inbound
}
