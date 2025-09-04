package main

import (
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"log"
	"os"
	"strings"
	"time"

	"github.com/a-ZINC/DistFile/p2p/handshake"
	"github.com/a-ZINC/DistFile/p2p/message"
	"github.com/a-ZINC/DistFile/p2p/tcp"
	"github.com/a-ZINC/DistFile/store"
)

func init() {
	gob.Register(&Payload{})
	gob.Register(&message.Message{})
	gob.Register(&message.BroadcastPayload{})
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

func makeServer(addr string, nodes ...string) *Server {
	cfg := tcp.Config{
		ListenerAddr: addr,
		HandShake:    handshake.NOPHandshake,
		MSGChan:      make(chan *message.Message, 100),
	}
	transport := tcp.NewTCPTransport(cfg)

	return NewServer(ServerCfg{
		root:              "net_" + addr,
		pathTransformFunc: pathTransform,
		addr:              addr,
		transport:         transport,
	}, nodes...)
}

func main() {
	server1 := makeServer(":3000")
	server2 := makeServer(":4000", ":3000")
	server1.cfg.transport.Config.OnPeer = server1.OnPeer
	server2.cfg.transport.Config.OnPeer = server2.OnPeer

	go func() {
		if err := server1.Start(); err != nil {
			log.Printf("Failed to start server: %v", err)
		}
	}()
	time.Sleep(2 * time.Second)
	go func() {
		if err := server2.Start(); err != nil {
			log.Printf("Failed to start server: %v", err)
		}
	}()
	time.Sleep(3 * time.Second)
	log.Printf("Both servers are ready")
	log.Printf("server1 peers: %v", server1.cfg.transport.Peers)
	log.Printf("server2 peers: %v", server2.cfg.transport.Peers)

	if _, err := os.Stat("ex.txt"); err != nil {
		f, err := os.Create("ex.txt")
		if err != nil {
			log.Printf("Failed to create file: %v", err)
			return
		}
		for i := 0; i < 100; i++ {
			f.WriteString("Hello, this is a sample file to test distributed file system. \n")
		}
	}
	f, err := os.Open("ex.txt")
	if err != nil {
		log.Printf("Failed to open file: %v", err)
		return
	}
	server2.SaveData("myPrivateMessage", f)
	select {}
}
