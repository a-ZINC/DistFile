package decoder

import (
	"encoding/gob"
	"net"

	"github.com/a-ZINC/DistFile/p2p/message"
)

func Decode(conn net.Conn, msg *message.Message) error {
	incomingMessageByte := make([]byte, 3)
	_, err := conn.Read(incomingMessageByte)
	if err != nil {
		return err
	}
	isStream := string(incomingMessageByte) == message.StreamTypeBits
	if isStream {
		*msg = message.Message{IsStream: true}
        return nil
	}

	decoder := gob.NewDecoder(conn)
	if err := decoder.Decode(msg); err != nil {
		return err
	}
	msg.IsStream = false
	return nil
}
