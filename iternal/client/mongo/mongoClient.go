package mongo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LaughG33k/messageDBService/iternal/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type MongoClient struct {
	ctx context.Context

	conn       *mongo.Client
	db         *mongo.Database
	collection *mongo.Collection

	muBulkData *sync.Mutex

	stopClient  bool
	closeClient bool

	cfg MongoClientConfig

	onceInitDistrib *sync.Once
}

type MongoClientConfig struct {
	Host       string
	Port       string
	Db         string
	Collection string

	BulkWriteTimeSleep     time.Duration
	HealthCheakWaitingTime time.Duration
	ReconectWaitingTime    time.Duration
	OperationTimeout       time.Duration

	SizeWriteBulkBuffer int
	RecconectAttempts   int
	MaxPoolSize         int
	MaxCollNums         int

	RetryWrites bool
	RetryReads  bool
}

func NewMongoClient(ctx context.Context, config MongoClientConfig) (*MongoClient, error) {

	conn, err := mongo.Connect(
		ctx,
		options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s/", config.Host, config.Port)),
		options.Client().SetMaxPoolSize(uint64(config.MaxPoolSize)),
		options.Client().SetRetryWrites(config.RetryWrites),
		options.Client().SetRetryReads(config.RetryReads),
		options.Client().SetBSONOptions(options.Collection().BSONOptions),
		options.Client().SetWriteConcern(writeconcern.Journaled()),
	)

	if err != nil {
		return nil, err
	}

	if err = conn.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	db := conn.Database(
		config.Db,
		options.Database().SetWriteConcern(writeconcern.Majority()),
	)

	mc := &MongoClient{
		ctx:             ctx,
		conn:            conn,
		db:              db,
		collection:      db.Collection(config.Collection),
		muBulkData:      &sync.Mutex{},
		onceInitDistrib: &sync.Once{},
		stopClient:      false,
		closeClient:     false,
		cfg:             config,
	}

	return mc, err

}

func (c *MongoClient) healthCheck() {

	for {

		if c.closeClient {
			return
		}

		if c.stopClient {
			continue
		}

		time.Sleep(c.cfg.HealthCheakWaitingTime)

		if err := c.conn.Ping(c.ctx, readpref.Primary()); err != nil {
			c.stopClient = true
			fmt.Println(err)
			go c.recconect()
		}

	}

}

func (c *MongoClient) recconect() {

	attempts := c.cfg.RecconectAttempts

	for attempts > 0 {

		conn, err := mongo.Connect(
			c.ctx,
			options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s/", c.cfg.Host, c.cfg.Port)),
			options.Client().SetMaxPoolSize(uint64(c.cfg.MaxPoolSize)),
			options.Client().SetRetryWrites(c.cfg.RetryWrites),
			options.Client().SetRetryReads(c.cfg.RetryReads),
			options.Client().SetBSONOptions(options.Collection().BSONOptions),
			options.Client().SetWriteConcern(writeconcern.Journaled()),
		)

		if err != nil {
			attempts--
			time.Sleep(c.cfg.ReconectWaitingTime)
			continue
		}

		c.conn = conn

		c.db = conn.Database(
			c.cfg.Db,
			options.Database().SetWriteConcern(writeconcern.Majority()),
		)

		c.stopClient = false

		return

	}

	if err := c.conn.Disconnect(c.ctx); err != nil {
		fmt.Println(err)
	}

	c.closeClient = true

}

func (c *MongoClient) SaveMessage(ctx context.Context, data model.MessageForSave) error {

	if c.closeClient {
		return fmt.Errorf("conn closed")
	}

	if c.stopClient {
		return fmt.Errorf("stopped")
	}

	updateAtRecipient := mongo.NewUpdateOneModel()
	updateAtRecipient.
		SetFilter(bson.M{"uuid": data.ReceiverUuid}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("received.%s.%s", data.SenderUuid, data.MessageId): bson.M{"text": data.Text, "time": data.Time, "editFlag": false}}})

	updateAtSender := mongo.NewUpdateOneModel()

	updateAtSender.
		SetFilter(bson.M{"uuid": data.SenderUuid}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("sent.%s.%s", data.ReceiverUuid, data.MessageId): bson.M{"text": data.Text, "time": data.Time, "editFlag": false}}})

	if _, err := c.collection.BulkWrite(ctx, []mongo.WriteModel{updateAtRecipient, updateAtSender}); err != nil {
		return err
	}

	return nil

}

func (c *MongoClient) DelSentMsg(ctx context.Context, msg model.MessageForDelete) error {

	if c.closeClient {
		return fmt.Errorf("conn closed")
	}

	if c.stopClient {
		return fmt.Errorf("stopped")
	}

	if _, err := c.collection.UpdateOne(ctx, bson.M{"uuid": msg.Sender}, bson.M{"$unset": bson.M{fmt.Sprintf("sent.%s.%s", msg.Receiver, msg.MessageId): 1}}); err != nil {
		return err
	}

	return nil

}

func (c *MongoClient) DelSentMsgForEvryone(ctx context.Context, msg model.MessageForDelete) error {

	if c.closeClient {
		return fmt.Errorf("conn closed")
	}

	if c.stopClient {
		return fmt.Errorf("stopped")
	}

	deleteAtReceiver := mongo.NewUpdateOneModel()

	deleteAtReceiver.SetFilter(bson.M{"uuid": msg.Receiver})
	deleteAtReceiver.SetUpdate(bson.M{"$unset": bson.M{fmt.Sprintf("received.%s.%s", msg.Sender, msg.MessageId): 1}})

	deleteAtSender := mongo.NewUpdateOneModel()

	deleteAtSender.SetFilter(bson.M{"uuid": msg.Sender})
	deleteAtSender.SetUpdate(bson.M{"$unset": bson.M{fmt.Sprintf("sent.%s.%s", msg.Receiver, msg.MessageId): 1}})

	if _, err := c.collection.BulkWrite(ctx, []mongo.WriteModel{deleteAtReceiver, deleteAtSender}); err != nil {
		return err
	}

	return nil
}

func (c *MongoClient) EditMessage(ctx context.Context, msg model.MessageForEdit) error {

	if c.closeClient {
		return fmt.Errorf("conn closed")
	}

	if c.stopClient {
		return fmt.Errorf("stopped")
	}

	updateAtRecipient := mongo.NewUpdateOneModel()
	updateAtRecipient.
		SetFilter(bson.M{"uuid": msg.Recipient}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("received.%s.%s.text", msg.Sender, msg.MessageId): msg.NewText}}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("received.%s.%s.editFlag", msg.Sender, msg.MessageId): true}})

	updateAtSender := mongo.NewUpdateOneModel()

	updateAtSender.
		SetFilter(bson.M{"uuid": msg.Sender}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("sent.%s.%s.text", msg.Recipient, msg.MessageId): msg.NewText}}).
		SetUpdate(bson.M{"$set": bson.M{fmt.Sprintf("sent.%s.%s.editFlag", msg.Recipient, msg.MessageId): true}})

	if _, err := c.collection.BulkWrite(ctx, []mongo.WriteModel{updateAtRecipient, updateAtSender}); err != nil {
		return err
	}

	return nil

}

func (c *MongoClient) InitUser(ctx context.Context, uuid string) error {

	if _, err := c.collection.InsertOne(ctx, bson.M{"uuid": uuid, "sent": bson.M{}, "received": bson.M{}}); err != nil {
		return err
	}

	return nil

}
