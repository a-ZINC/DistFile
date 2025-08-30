package p2p

import "net"

type Peer interface {
	Close() error
	RemoteAddr() net.Addr
	IsInbound() bool
}

type Transport interface {
	ListenAndAccept() error
	Dial(addr string) error
	Close()
}