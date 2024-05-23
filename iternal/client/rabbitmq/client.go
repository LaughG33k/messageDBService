package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LaughG33k/messageDBService/pkg"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitMqClient struct {
	log  pkg.Logger
	conn *amqp091.Connection
	ch   *amqp091.Channel
	*consumer
	close chan struct{}
	cfg   RabbitMqClientConfig

	catchNotif *sync.Once
	onceClose  *sync.Once
}

type RabbitMqClientConfig struct {
	Login    string
	Password string
	Host     string
	Port     string
	ConnCfg  amqp091.Config
}

func InitRbbitMqCLient(log pkg.Logger, rqc RabbitMqClientConfig) (*RabbitMqClient, error) {

	conn, err := amqp091.DialConfig(
		fmt.Sprintf("amqp://%s:%s@%s:%s/", rqc.Login, rqc.Password, rqc.Host, rqc.Port),
		rqc.ConnCfg,
	)

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	consumer, err := initConsumer(conn)

	if err != nil {
		return nil, err
	}

	rqClient := &RabbitMqClient{
		conn:       conn,
		ch:         ch,
		consumer:   consumer,
		close:      make(chan struct{}),
		cfg:        rqc,
		catchNotif: &sync.Once{},
		onceClose:  &sync.Once{},
		log:        log,
	}

	return rqClient, nil

}

func (c *RabbitMqClient) CreateQueue(name, rkey, excahnge string, durable, autoDelete, exclusive, noWait bool) error {

	q, err := c.ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, nil)

	if err != nil {
		return err
	}

	if err := c.ch.QueueBind(q.Name, rkey, excahnge, false, nil); err != nil {
		return err
	}

	return nil
}

func (c *RabbitMqClient) CatchNotify() {

	c.catchNotif.Do(func() {

		connBlocked := c.conn.NotifyBlocked(make(chan amqp091.Blocking))
		connClosed := c.conn.NotifyClose(make(chan *amqp091.Error))
		connChanClose := c.ch.NotifyClose(make(chan *amqp091.Error))
		consumerChanClose := c.consumer.GetCahnNotifyClose()

		go func() {

			for {

				select {

				case v, ok := <-connBlocked:

					if !ok {
						c.log.Info("connBlocked chan closed")
					} else {
						c.log.Info(v)
					}

				case v, ok := <-connClosed:

					if !ok {
						c.log.Info("connClosed chan closed")
					} else {
						c.log.Info(v)
					}

				case v, ok := <-connChanClose:

					if !ok {
						c.log.Info("connClosed chan closed")
					} else {
						c.log.Info(v)
					}

					err := pkg.RetrySmth(func() error {
						ch, err := c.conn.Channel()
						if err != nil {
							return err
						}
						c.ch = ch
						return nil
					}, 3, 500*time.Millisecond)

					if err != nil {
						c.log.Info("chan reinit failed", err)
					} else {
						c.log.Info("chan reinit successeful", err)
						continue
					}

				case v, ok := <-consumerChanClose:
					if !ok {
						c.log.Info("consumer chan closed")
					} else {
						c.log.Info(v)
					}

					err := pkg.RetrySmth(func() error {
						if err := c.consumer.ReInitChannel(c.conn); err != nil {
							return err
						}
						return nil
					}, 3, 500*time.Millisecond)

					if err != nil {
						c.log.Info("consumer chan reinit failed", err)
					} else {
						c.log.Info("consumer chan reinit successeful", err)
						continue
					}

				}

				if err := c.closeAndRecconect(); err != nil {
					c.log.Info("Rabbitmq client recconect failed", err)
					return
				}

				connBlocked = c.conn.NotifyBlocked(make(chan amqp091.Blocking))
				connClosed = c.conn.NotifyClose(make(chan *amqp091.Error))
				connChanClose = c.ch.NotifyClose(make(chan *amqp091.Error))
				consumerChanClose = c.consumer.GetCahnNotifyClose()

				c.log.Info("reconnected")
			}

		}()

	})
}

func (c *RabbitMqClient) recconnect(attempts int, recconnectInteraval time.Duration) (err error) {

	c.log.Info("start recconnect")

	connEstabilished := false

	i := attempts

	return pkg.RetrySmth(func() error {

		i--
		c.log.Debug(fmt.Sprintf("RabbitMqClient: recconect attempt number %d", i))

		if !connEstabilished {

			c.log.Info("attempt to establish connection")

			conn, err := amqp091.DialConfig(
				fmt.Sprintf("amqp://%s:%s@%s:%s/", c.cfg.Login, c.cfg.Password, c.cfg.Host, c.cfg.Port),
				c.cfg.ConnCfg,
			)

			if err != nil {
				c.log.Info("attempt to establish connection failed")
				return err
			}

			c.conn = conn
			connEstabilished = true

			c.log.Info("attempt to establish connection is successful")

		}

		c.log.Info("attempt to init consumer")

		err := c.consumer.ReInit(c.conn)

		if err != nil {
			c.log.Info("attempt to init consumer failed")
			return err
		}

		c.log.Info("attempt to init consumer successeful")

		c.log.Info("attempt to init the client channel")

		ch, err := c.conn.Channel()

		if err != nil {
			c.log.Info("attempt to init the client channel failed")
			return err
		}

		c.ch = ch
		c.log.Info("attempt to init the client channel successeful")

		c.close = make(chan struct{})
		c.onceClose = &sync.Once{}

		return nil

	}, attempts, recconnectInteraval)

}

func (c *RabbitMqClient) checkIsClose() bool {

	select {

	case _, ok := <-c.close:
		if !ok {
			return true
		}

	default:
	}

	return false

}

func (c *RabbitMqClient) Close() {

	if !c.checkIsClose() {

		c.onceClose.Do(func() {

			c.log.Info("to close the connection")

			if err := c.conn.CloseDeadline(time.Now().Add(3 * time.Second)); err != nil {
				c.log.Info("close the connection failed", err)
			}

			c.log.Info("to close the consumer")

			if err := c.consumer.Close(); err != nil {
				c.log.Info("to close the consumer failed", err)
			}

			close(c.close)

			c.log.Info("Rabbitmq client closed")
		})

		return

	}

	c.log.Info("Client already closed")

}

func (c *RabbitMqClient) closeAndRecconect() error {
	c.Close()
	return c.recconnect(7, 5*time.Second)
}

func (c *RabbitMqClient) Consume(queue string, bufferSize int) <-chan amqp091.Delivery {
	ctx := context.Background()
	return c.ConsumeWithContext(ctx, queue, bufferSize)
}

func (c *RabbitMqClient) ConsumeWithContext(ctx context.Context, queue string, bufferSize int) <-chan amqp091.Delivery {
	ch := make(chan amqp091.Delivery, bufferSize)
	c.consumer.AddReceivingToQueueWithContext(ctx, ch, queue)
	return ch

}
