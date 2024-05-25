package mongo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LaughG33k/messageDBService/pkg"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type MongoClient struct {
	ctx context.Context

	conn *mongo.Client
	db   *mongo.Database

	collections    map[string]*mongo.Collection
	lockCollection *sync.RWMutex

	closeClient bool

	cfg MongoClientConfig

	onceInitDistrib *sync.Once
}

type MongoClientConfig struct {
	Host        string
	Port        string
	Db          string
	Collections []string

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

	colls := make(map[string]*mongo.Collection, len(config.Collections))

	for _, v := range config.Collections {
		colls[v] = db.Collection(v)
	}

	mc := &MongoClient{
		ctx:             ctx,
		conn:            conn,
		db:              db,
		collections:     colls,
		lockCollection:  &sync.RWMutex{},
		onceInitDistrib: &sync.Once{},
		closeClient:     false,
		cfg:             config,
	}

	return mc, err

}

func (c *MongoClient) HealthCheck() {

	for {

		time.Sleep(c.cfg.HealthCheakWaitingTime)

		if err := c.conn.Ping(c.ctx, readpref.Primary()); err != nil {
			fmt.Println(err)
			if err := c.recconect(); err != nil {
				fmt.Println(err)
				return
			}
		}

	}

}

func (c *MongoClient) recconect() error {

	err := pkg.RetrySmth(func() error {
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
			return err
		}

		c.conn = conn

		c.db = conn.Database(
			c.cfg.Db,
			options.Database().SetWriteConcern(writeconcern.Majority()),
		)

		c.reInitCollections()

		return nil
	}, c.cfg.RecconectAttempts, c.cfg.ReconectWaitingTime)

	if err != nil {
		if err := c.conn.Disconnect(c.ctx); err != nil {
			return err
		}

		c.closeClient = true
		return err
	}

	return nil
}

func (c *MongoClient) reInitCollections() {

	c.lockCollection.Lock()
	defer c.lockCollection.Unlock()

	for _, v := range c.cfg.Collections {

		c.collections[v] = c.db.Collection(v)

	}

}

func (c *MongoClient) Collection(name string) *mongo.Collection {

	if c.closeClient {
		return nil
	}

	c.lockCollection.RLock()
	defer c.lockCollection.RUnlock()

	return c.collections[name]

}
