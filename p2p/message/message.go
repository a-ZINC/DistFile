package message

const (
	MessageTypeBits      =  "0x1"
	StreamTypeBits       =  "0x2"
)

type StoreMessagePayload struct {
	From string
	Size int64
	Key  string
}
type GetMessagePayload struct {
	From string
	Key  string
}
type Message struct {
	Payload  interface{}
	IsStream bool
}
