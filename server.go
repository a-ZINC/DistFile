package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

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
}

func NewServer(cfg ServerCfg) *Server {
	store := store.NewStore(store.StoreOpts{
		DefaultRoot:       cfg.root,
		PathTransformFunc: cfg.pathTransformFunc,
	})
	return &Server{
		cfg:    cfg,
		store:  store,
		quitCh: make(chan os.Signal, 1),
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
	signal.Notify(s.quitCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	log.Printf("Server listening on %s", s.cfg.addr)
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
