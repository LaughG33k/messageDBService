package model

type MessageForSave struct {
	SenderUuid   string
	ReceiverUuid string
	Text         string
	MessageId    string
	Time         int64
}
