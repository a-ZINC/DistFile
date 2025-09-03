package decoder

import (
	"net"

	"github.com/a-ZINC/DistFile/p2p/message"
)

func Decode(conn net.Conn, msg *message.Message) error {
	// buff := make([]byte, 1024)
	// n, err := conn.Read(buff)
	// if err != nil {
	// 	return err
	// }
	// *msg = message.Message{
	// 	Type:    message.BroadcastMsg,
	// 	BroadcastPayload: buff[:n],
	// }
	return nil
}
