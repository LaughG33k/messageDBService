package model

type MessageForEdit struct {
	Sender    string
	Recipient string
	MessageId string
	NewText   string
}
