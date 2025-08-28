package p2p

import "net"

type Peer interface {
	Close() error
}

type Transport interface {
	ListenAndAccept() error
	Close(conn net.Conn)
}