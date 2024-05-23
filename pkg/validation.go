package pkg

import (
	"errors"

	"github.com/LaughG33k/messageDBService/iternal/model"

	"github.com/google/uuid"
)

func ValidateMessage(msg model.MessageForSave) error {

	if msg.MessageId == "" {
		return errors.New("message id is empty")
	}

	if msg.ReceiverUuid == "" || uuid.Validate(msg.ReceiverUuid) != nil {
		return errors.New("receiver uuid is invalid")
	}

	if msg.SenderUuid == "" || uuid.Validate(msg.SenderUuid) != nil {
		return errors.New("sender uuid is invalid")
	}

	if msg.Text == "" {
		return errors.New("cannot save empty text")
	}

	return nil
}

func ValidMsgForDel(msg model.MessageForDelete) error {

	if uuid.Validate(msg.Sender) != nil {
		return errors.New("invalid sender uuid")
	}

	if uuid.Validate(msg.Receiver) != nil {
		return errors.New("invalid receiver uuid")
	}

	if msg.MessageId == "" {
		return errors.New("message id is empty")
	}

	return nil

}

func ValidMsgForEdit() {

}
