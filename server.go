package main

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
			s.handleMessage(msg)
		case <-s.quitCh:
			s.cfg.transport.Close()
			log.Printf("Server shutting down")
			return nil
		}
	}
}

func (s *Server) handleMessage(msg *message.Message) {
	log.Printf("Handling message: %v", msg)
	switch msg.Type {
	case message.BroadcastMsg:
		 s.handleMessageStoredFile(msg)
	default:
		log.Printf("Unknown message payload type: %T", msg)
	}
}

func (s *Server) handleMessageStoredFile(msg *message.Message) {
	log.Printf("Received stored file message: %v", msg)
	peer, ok := s.cfg.transport.Peers[s.cfg.addr]
	log.Printf("peer: %v, server: %v", s.cfg.transport.Peers, s.cfg.addr)
	if !ok {
		log.Printf("Unknown peer: %v", msg.From)
		return
	}
	if _, err := io.CopyN(io.Discard, peer, msg.Size); err != nil {
		log.Printf("Error discarding message from %v: %v", msg.From, err)
		return
	}

	s.cfg.transport.Wg.Done()
	log.Printf("Handling stored file message: %v", msg)
}

func (s *Server) OnPeer(peer p2p.Peer) error {
	s.cfg.transport.Mu.Lock()
	s.cfg.transport.Peers[peer.RemoteAddr().String()] = peer
	log.Printf("New peer connected remote: %v, curr: %v,  Inbound: %v", peer.RemoteAddr(), peer.LocalAddr(), peer.IsInbound())
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

func (s *Server) Broadcast(msg *message.Message, r io.Reader) error {
	var peers []io.Writer
	for _, peer := range s.cfg.transport.Peers {
		peers = append(peers, peer)
	}

	writer := io.MultiWriter(peers...)
	if err := gob.NewEncoder(writer).Encode(msg); err != nil {
		log.Printf("Failed to broadcast message: %v", err)
		return err
	}
	return nil
}

func (s *Server) SaveData(key string, r io.Reader) error {

	fileBuffer := new(bytes.Buffer)
	teeReader := io.TeeReader(io.LimitReader(r, 10), fileBuffer)
	size, err := s.store.Write(key, teeReader)
	if err != nil {
		return err
	}

	message := &message.Message{
		Type: message.BroadcastMsg,
		BroadcastPayload: message.BroadcastPayload{
			From: s.cfg.addr,
			Size: size,
		},
	}

	log.Printf("Broadcasting message: %v", message)
	err = s.Broadcast(message, r)
	if err != nil {
		return err
	}

	time.Sleep(3 * time.Second)

	err = s.StreamToPeers(r)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) StreamToPeers(r io.Reader) error {
	var peers []io.Writer
	for _, p := range s.cfg.transport.Peers {
		peers = append(peers, p)
	}
	log.Printf("Streaming data to %+v peers", peers)
	writer := io.MultiWriter(peers...)
	if _, err := io.Copy(writer, r); err != nil {
		log.Printf("Failed to stream data to peers: %v", err)
		return err
	}
	log.Printf("Successfully streamed data to peers")
	s.cfg.transport.Wg.Done()
	return nil
}
