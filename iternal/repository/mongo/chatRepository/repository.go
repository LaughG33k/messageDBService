package chatrepository

import (
	"context"
	"fmt"

	mcl "github.com/LaughG33k/messageDBService/iternal/client/mongo"
	"github.com/LaughG33k/messageDBService/iternal/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type ChatRepository struct {
	mongoClient *mcl.MongoClient
	collection  string
}

func NewRepository(mongoClient *mcl.MongoClient, collection string) *ChatRepository {
	return &ChatRepository{

		mongoClient: mongoClient,
		collection:  collection,
	}

}

func (c *ChatRepository) SaveMessage(ctx context.Context, data model.MessageForSave) error {

	updateAtRecipient := mongo.NewUpdateOneModel()
	updateAtRecipient.
		SetFilter(bson.M{"uuid": data.ReceiverUuid}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("received.%s.%s", data.SenderUuid, data.MessageId): bson.M{"text": data.Text, "time": data.Time, "editFlag": false}}})

	updateAtSender := mongo.NewUpdateOneModel()

	updateAtSender.
		SetFilter(bson.M{"uuid": data.SenderUuid}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("sent.%s.%s", data.ReceiverUuid, data.MessageId): bson.M{"text": data.Text, "time": data.Time, "editFlag": false}}})

	if _, err := c.mongoClient.Collection(c.collection).BulkWrite(ctx, []mongo.WriteModel{updateAtRecipient, updateAtSender}); err != nil {
		return err
	}

	return nil

}

func (c *ChatRepository) DelSentMsg(ctx context.Context, msg model.MessageForDelete) error {

	if _, err := c.mongoClient.Collection(c.collection).UpdateOne(ctx, bson.M{"uuid": msg.Sender}, bson.M{"$unset": bson.M{fmt.Sprintf("sent.%s.%s", msg.Receiver, msg.MessageId): 1}}); err != nil {
		return err
	}

	return nil

}

func (c *ChatRepository) DelSentMsgForEvryone(ctx context.Context, msg model.MessageForDelete) error {

	deleteAtReceiver := mongo.NewUpdateOneModel()

	deleteAtReceiver.SetFilter(bson.M{"uuid": msg.Receiver})
	deleteAtReceiver.SetUpdate(bson.M{"$unset": bson.M{fmt.Sprintf("received.%s.%s", msg.Sender, msg.MessageId): 1}})

	deleteAtSender := mongo.NewUpdateOneModel()

	deleteAtSender.SetFilter(bson.M{"uuid": msg.Sender})
	deleteAtSender.SetUpdate(bson.M{"$unset": bson.M{fmt.Sprintf("sent.%s.%s", msg.Receiver, msg.MessageId): 1}})

	if _, err := c.mongoClient.Collection(c.collection).BulkWrite(ctx, []mongo.WriteModel{deleteAtReceiver, deleteAtSender}); err != nil {
		return err
	}

	return nil
}

func (c *ChatRepository) EditMessage(ctx context.Context, msg model.MessageForEdit) error {

	updateAtRecipient := mongo.NewUpdateOneModel()
	updateAtRecipient.
		SetFilter(bson.M{"uuid": msg.Recipient}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("received.%s.%s.text", msg.Sender, msg.MessageId): msg.NewText, fmt.Sprintf("received.%s.%s.editFlag", msg.Sender, msg.MessageId): true}})
	updateAtSender := mongo.NewUpdateOneModel()

	updateAtSender.
		SetFilter(bson.M{"uuid": msg.Sender}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("sent.%s.%s.text", msg.Recipient, msg.MessageId): msg.NewText, fmt.Sprintf("sent.%s.%s.editFlag", msg.Recipient, msg.MessageId): true}})

	if _, err := c.mongoClient.Collection(c.collection).BulkWrite(ctx, []mongo.WriteModel{updateAtRecipient, updateAtSender}); err != nil {
		return err
	}

	return nil

}

func (c *ChatRepository) InitUser(ctx context.Context, uuid string) error {

	if _, err := c.mongoClient.Collection(c.collection).InsertOne(ctx, bson.M{"uuid": uuid, "sent": bson.M{}, "received": bson.M{}}); err != nil {
		return err
	}

	return nil

}
