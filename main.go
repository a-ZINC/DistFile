package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/a-ZINC/DistFile/p2p"
	"github.com/a-ZINC/DistFile/p2p/handshake"
	"github.com/a-ZINC/DistFile/p2p/message"
	"github.com/a-ZINC/DistFile/p2p/tcp"
	"github.com/a-ZINC/DistFile/store"
)

func onPeer(peer p2p.Peer) error {
	fmt.Printf("New peer connected: %v\n", peer)
	return nil
}

func pathTransform(key string, root string) *store.Path {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	pathLen := 5
	pathFolder := make([]string, pathLen)
	for i := range pathFolder {
		pathFolder[i] = hashStr[i*pathLen : (i+1)*pathLen]
	}
	return &store.Path{FileName: hashStr, DirPath: strings.Join(pathFolder, "/"), DefaultRoot: root}
}

func main() {
	fmt.Println("Hello, World!")
	cfg := tcp.Config{
		ListenerAddr: ":3000",
		HandShake:    handshake.NOPHandshake,
		MSGChan:      make(chan *message.Message, 100),
		OnPeer:       onPeer,
	}

	transport := tcp.NewTCPTransport(cfg)

	servercfg := ServerCfg{
		addr:              ":3000",
		root:              "/tmp/store",
		pathTransformFunc: pathTransform,
		transport:        transport,
	}

	server := NewServer(servercfg)

	if err := server.Start(); err != nil {
		log.Printf("Failed to start server: %v", err)
	}
}
