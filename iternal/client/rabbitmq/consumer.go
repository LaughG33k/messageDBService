package rabbitmq

import (
	"context"
	"errors"
	"sync"

	"github.com/rabbitmq/amqp091-go"
	"github.com/savsgio/gotils/uuid"
)

type consumer struct {
	rChan           *amqp091.Channel
	chClose         chan *amqp091.Error
	close           chan struct{}
	listeningQueues map[string][]chan amqp091.Delivery
	muLQ            *sync.RWMutex
	onceClose       *sync.Once
	reinitLock      chan struct{}
}

func initConsumer(conn *amqp091.Connection) (*consumer, error) {

	rChan, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	c := &consumer{
		rChan:           rChan,
		close:           make(chan struct{}),
		listeningQueues: make(map[string][]chan amqp091.Delivery),
		muLQ:            &sync.RWMutex{},
		onceClose:       &sync.Once{},
		reinitLock:      make(chan struct{}),
	}

	return c, nil
}

func (c *consumer) ReInit(conn *amqp091.Connection) error {

	if !c.CheckIsClose() {
		return errors.New("Can not to reinit not closed consumer")
	}

	if err := c.ReInitChannel(conn); err != nil {
		return err
	}

	c.close = make(chan struct{})
	c.onceClose = &sync.Once{}

	return nil
}

func (c *consumer) ReInitChannel(conn *amqp091.Connection) error {

	rChan, err := conn.Channel()

	if err != nil {
		return err
	}

	c.rChan = rChan
	c.chClose = c.rChan.NotifyClose(make(chan *amqp091.Error))

	return nil

}

func (c *consumer) GetCahnNotifyClose() <-chan *amqp091.Error {

	if c.chClose == nil {
		c.chClose = c.rChan.NotifyClose(make(chan *amqp091.Error))
	}

	return c.chClose

}

func (c *consumer) CheckIsClose() bool {

	select {

	case _, ok := <-c.close:
		if !ok {
			return true
		}

	default:

	}

	return false
}

func (c *consumer) Close() (err error) {

	if !c.CheckIsClose() {

		c.onceClose.Do(func() {

			close(c.close)

			go func() { c.rChan.Close() }()

			if _, ok := <-c.chClose; !ok {
				err = errors.New("consumer notify chan closed")
				return
			}

		})

		if err != nil {
			return err
		}
	}

	return nil

}

func (c *consumer) receive(ctx context.Context, queue string) error {

	m, err := c.rChan.ConsumeWithContext(ctx, queue, uuid.V4(), false, true, false, false, nil)

	if err != nil {
		return err
	}

	for {

		select {

		case _, ok := <-c.close:
			if !ok {
				return errors.New("Close")
			}

		case data, ok := <-m:

			c.muLQ.RLock()
			channels := c.listeningQueues[queue]
			c.muLQ.RUnlock()

			if !ok {
				for _, v := range channels {
					close(v)
				}

				return nil
			}

			for _, v := range channels {
				v <- data
			}
		}

	}

}

func (c *consumer) AddReceivingToQueueWithContext(ctx context.Context, out chan amqp091.Delivery, queue string) {

	c.muLQ.Lock()
	defer c.muLQ.Unlock()

	if _, ok := c.listeningQueues[queue]; !ok {
		c.listeningQueues[queue] = append(c.listeningQueues[queue], out)
		go c.receive(ctx, queue)
		return
	}

	c.listeningQueues[queue] = append(c.listeningQueues[queue], out)

}

func (c *consumer) AddReceivingToQueue(out chan amqp091.Delivery, queue string) {
	ctx := context.Background()
	c.AddReceivingToQueueWithContext(ctx, out, queue)
}
