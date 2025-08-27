package main

import (
	"fmt"
	"log"

	"github.com/a-ZINC/DistFile/p2p/tcp"
)

func main() {
	// Your code here
	fmt.Println("Hello, World!")
	server := tcp.NewTCPTransport(":4000")
	if err := server.ListenAndAccept(); err != nil {
		log.Printf("Hii, bro server creation fucked up!")
	}
	select {}
	// Start the TCP transport
}
