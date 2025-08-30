package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/a-ZINC/DistFile/p2p"
	"github.com/a-ZINC/DistFile/p2p/tcp"
	"github.com/a-ZINC/DistFile/store"
)

type ServerCfg struct {
	addr              string
	root              string
	pathTransformFunc func(string, string) *store.Path
	transport         *tcp.TCPTransport
}

type Server struct {
	cfg   ServerCfg
	store *store.Store

	quitCh chan os.Signal
	nodes  []string
}

func NewServer(cfg ServerCfg, nodes ...string) *Server {
	store := store.NewStore(store.StoreOpts{
		DefaultRoot:       cfg.root,
		PathTransformFunc: cfg.pathTransformFunc,
	})
	return &Server{
		cfg:    cfg,
		store:  store,
		quitCh: make(chan os.Signal, 1),
		nodes:  nodes,
	}
}

func (s *Server) Stop() {
	close(s.quitCh)
}

func (s *Server) Start() error {
	defer s.Stop()
	if err := s.cfg.transport.ListenAndAccept(); err != nil {
		return err
	}
	s.StartNodes()
	signal.Notify(s.quitCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case msg := <-s.cfg.transport.MSGChan:
			log.Printf("Received message: %v", msg.Payload)
		case <-s.quitCh:
			s.cfg.transport.Close()
			log.Printf("Server shutting down")
			return nil
		}
	}
}

func (s *Server) OnPeer(peer p2p.Peer) error {
	s.cfg.transport.Mu.Lock()
	s.cfg.transport.Peers[peer.RemoteAddr()] = peer
	log.Printf("New peer connected: %v, Inbound: %v", peer.RemoteAddr(), peer.IsInbound())
	s.cfg.transport.Mu.Unlock()
	return nil
}

func (s *Server) StartNodes() error {
	for _, node := range s.nodes {
		log.Printf("Connecting to node: %s", node)
		err := s.cfg.transport.Dial(node)
		if err != nil {
			continue
		}
	}
	return nil
}
