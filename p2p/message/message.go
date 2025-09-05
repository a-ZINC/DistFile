package message



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
	Payload interface{}
}
