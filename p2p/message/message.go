package message

var (
	BroadcastMsg = "broadcast"
	StreamMsg    = "stream"
)

type BroadcastPayload struct {
	From string
	Size int64
	Key  string
}
type Message struct {
	Type string
	BroadcastPayload
}
