package p2p

import "net"

type Peer interface {
	net.Conn
	IsInbound() bool
}

type Transport interface {
	ListenAndAccept() error
	Dial(addr string) error
	Close()
}