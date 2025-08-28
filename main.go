package main

import (
	"fmt"
	"log"

	"github.com/a-ZINC/DistFile/p2p"
	"github.com/a-ZINC/DistFile/p2p/handshake"
	"github.com/a-ZINC/DistFile/p2p/message"
	"github.com/a-ZINC/DistFile/p2p/tcp"
)

func onPeer(peer p2p.Peer) error {
	fmt.Printf("New peer connected: %v\n", peer)
	return nil
}

func main() {
	// Your code here
	fmt.Println("Hello, World!")
	cfg := tcp.Config{
		ListenerAddr: ":3000",
		HandShake:    handshake.NOPHandshake,
		MSGChan:      make(chan *message.Message, 100),
		OnPeer:      onPeer,
	}

	server := tcp.NewTCPTransport(cfg)

	go func() {
		for msg := range server.MSGChan {
			fmt.Printf("Received message: %v\n", msg.Payload)
		}
	}()

	if err := server.ListenAndAccept(); err != nil {
		log.Printf("Hii, bro server creation fucked up!")
	}
	select {}
	// Start the TCP transport
}
