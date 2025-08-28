package decoder

import (
	"net"

	"github.com/a-ZINC/DistFile/p2p/message"
)

type decoder interface {
	Decode(conn net.Conn, msg *message.Message) error
}
