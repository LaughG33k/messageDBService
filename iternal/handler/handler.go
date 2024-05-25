package handler

import (
	"context"
	"errors"
	"time"

	"github.com/LaughG33k/messageDBService/iternal/codegen"
	"github.com/LaughG33k/messageDBService/iternal/model"
	chatrepository "github.com/LaughG33k/messageDBService/iternal/repository/mongo/chatRepository"
	"github.com/LaughG33k/messageDBService/pkg"
)

type Handler struct {
	codegen.MessageDBworkerServiceServer
	OperationTimeout     time.Duration
	ctx                  context.Context
	repository           *chatrepository.ChatRepository
	reqBufferSizePerConn int
}

func InitSaveMessagesHandler(repository *chatrepository.ChatRepository, reqBufferSizePerConn int) codegen.MessageDBworkerServiceServer {
	return &Handler{
		ctx:                  context.Background(),
		reqBufferSizePerConn: reqBufferSizePerConn,
		repository:           repository,
	}
}

func (h *Handler) Operation(c codegen.MessageDBworkerService_OperationServer) error {

	err := make(chan error, 1)

	return <-err

}

func (h *Handler) receive(c codegen.MessageDBworkerService_OperationServer, errCh chan error) {

	wp := pkg.InitWp(h.reqBufferSizePerConn)

	for {

		req, err := c.Recv()

		if err != nil {
			errCh <- err
			return
		}

		wp.AddWorker(func() {
			h.proccesRequest(req)
		})

	}

}

func (h *Handler) proccesRequest(req *codegen.Request) error {

	switch req.OperationId {

	case 1:

		if req.Message == nil {
			return errors.New("empty body for save the message")
		}

		return h.saveMessage(req)

	case 2:

		if req.DelMessage == nil {
			return errors.New("empty body for del the message")
		}

		return h.deleteMessage(req)

	}

	return nil
}

func (h *Handler) saveMessage(req *codegen.Request) error {

	msg := model.MessageForSave{
		SenderUuid:   req.Message.Sender,
		ReceiverUuid: req.Message.Recipient,
		Text:         req.Message.Text,
		MessageId:    req.Message.Id,
		Time:         req.Message.Time,
	}

	if err := pkg.ValidateMessage(msg); err != nil {
		return err
	}

	tm, canc := context.WithTimeout(h.ctx, h.OperationTimeout)
	defer canc()

	if err := h.repository.SaveMessage(tm, msg); err != nil {
		return err
	}

	return nil
}

func (h *Handler) deleteMessage(req *codegen.Request) error {

	msg := model.MessageForDelete{
		Sender:    req.DelMessage.Sender,
		Receiver:  req.DelMessage.Recipient,
		MessageId: req.DelMessage.Id,
	}

	if err := pkg.ValidMsgForDel(msg); err != nil {
		return err
	}

	tm, canc := context.WithTimeout(h.ctx, h.OperationTimeout)
	defer canc()

	if req.DelMessage.ForEveryone {

		if err := h.repository.DelSentMsgForEvryone(tm, msg); err != nil {
			return err
		}
		return nil
	}

	if err := h.repository.DelSentMsg(tm, msg); err != nil {
		return err
	}

	return nil
}

func (h *Handler) editMessage(req *codegen.Request) error {

	return nil
}

func (s *Handler) mustEmbedUnimplementedMessageDBworkerServiceServer() {
}
