package main

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
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
	s.cfg.transport.Close()
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
			os.Exit(0)
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
	peer, ok := s.cfg.transport.Peers[msg.From]
	log.Printf("peer: %v, server: %v", s.cfg.transport.Peers, s.cfg.addr)
	if !ok {
		log.Printf("Peer not found for address: %s", msg.From)
		return
	}
	if _, err := s.store.Write(msg.Key, io.LimitReader(peer, msg.Size)); err != nil {
		log.Printf("Failed to store file from peer %v: %v", peer.RemoteAddr(), err)
		return
	}
	log.Printf("Successfully stored file from peer %v with key %s", peer.RemoteAddr(), msg.Key)
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

func (s *Server) Broadcast(size int64, key string) error {
	var wg sync.WaitGroup
	wg.Add(len(s.cfg.transport.Peers))
	defer wg.Wait()
	for _, peer := range s.cfg.transport.Peers {
		go func(p p2p.Peer) {
			defer wg.Done()
			msg := &message.Message{
				Type: message.BroadcastMsg,
				BroadcastPayload: message.BroadcastPayload{
					From: p.LocalAddr().String(),
					Size: size,
					Key:  key,
				},
			}
			if err := gob.NewEncoder(p).Encode(msg); err != nil {
				log.Printf("Failed to send broadcast to %v: %v", p.RemoteAddr(), err)
				return
			}
			log.Printf("Broadcasted message to %v: %v", p.RemoteAddr(), msg)
		}(peer)
	}
	return nil
}

func (s *Server) SaveData(key string, r io.Reader) error {

	fileBuffer := new(bytes.Buffer)
	teeReader := io.TeeReader(r, fileBuffer)
	size, err := s.store.Write(key, teeReader)
	if err != nil {
		return err
	}

	err = s.Broadcast(size, key)
	if err != nil {
		return err
	}
	time.Sleep(2 * time.Second) // TODO: Replace with proper sync mechanism
	err = s.StreamToPeers(fileBuffer.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) StreamToPeers(data []byte) error {
	var wg sync.WaitGroup

	for _, p := range s.cfg.transport.Peers {
		wg.Add(1)
		go func(peer p2p.Peer, dataToSend []byte) {
			defer wg.Done()
			reader := bytes.NewReader(dataToSend)
			n, err := io.Copy(peer, reader)
			if err != nil {
				log.Printf("Failed to stream to %v: %v", peer.RemoteAddr(), err)
				return
			}
			log.Printf("Successfully streamed %d bytes to %v", n, peer.RemoteAddr())
		}(p, data)
	}

	wg.Wait()
	log.Printf("Successfully streamed data to all peers")
	return nil
}
