package main

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/a-ZINC/DistFile/p2p"
	"github.com/a-ZINC/DistFile/p2p/message"
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

type Payload struct {
	Key   string
	Value []byte
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
			log.Printf("Received message1: %v", msg)
			s.handleMessage(msg)
		case <-s.quitCh:
			s.cfg.transport.Close()
			log.Printf("Server shutting down")
			return nil
		}
	}
}

func (s *Server) handleMessage(msg *message.Message) {
	log.Printf("Received message: %v", msg)

	switch payload := msg.Payload.(type) {
		case *Payload:
			log.Printf("Storing key: %s, value size: %+v bytes", payload.Key, payload)
		default:
			log.Printf("Unknown message payload type: %T", payload)
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

func (s *Server) Broadcast(msg *message.Message) {
	var peers []io.Writer
	for _, peer := range s.cfg.transport.Peers {
		log.Printf("Adding peer %v to broadcast list", peer.RemoteAddr())
		peers = append(peers, peer)
	}

	writer := io.MultiWriter(peers...)
	if err := gob.NewEncoder(writer).Encode(msg); err != nil {
		log.Printf("Failed to broadcast message: %v", err)
	}
	log.Printf("Broadcasted message to %+v peers", peers)
	log.Printf("Broadcasted message: %d", len(peers))
}

func (s *Server) SaveData(key string, r io.Reader) error {

	buff := new(bytes.Buffer)
	teeReader := io.TeeReader(r, buff)
	if err := s.store.Write(key, teeReader); err != nil {
		return err
	}

	payload := &Payload{
		Key:   key,
		Value: buff.Bytes(),
	}
	message := &message.Message{
		From:    s.cfg.addr,
		Payload: payload,
	}
	log.Printf("Broadcasting message: %v", message)
	s.Broadcast(message)
	return nil
}
