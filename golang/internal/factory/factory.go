package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MyQueueMiddleware struct {
	myConnection *amqp.Connection
	myChannel    *amqp.Channel
	myQueue      amqp.Queue
}

func (m MyQueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	//TODO implement me
	panic("implement me")
}

func (m MyQueueMiddleware) StopConsuming() error {
	//TODO implement me
	panic("implement me")
}

func (m MyQueueMiddleware) Send(msg m.Message) error {
	//TODO implement me
	panic("implement me")
}

func (m MyQueueMiddleware) Close() error {
	//TODO implement me
	panic("implement me")
}

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "guest", "guest", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	queue, err := channel.QueueDeclare(queueName, true, false, false, false, amqp.Table{
		amqp.QueueTypeArg: amqp.QueueTypeQuorum,
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	aMiddleWare := &MyQueueMiddleware{
		myConnection: conn,
		myQueue:      queue,
	}
	return aMiddleWare, nil
}

type MyExchangeMiddleware struct {
	myConnection *amqp.Connection
	myChannel    *amqp.Channel
	myKeys       []string
}

func (m MyExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	//TODO implement me
	panic("implement me")
}

func (m MyExchangeMiddleware) StopConsuming() error {
	//TODO implement me
	panic("implement me")
}

func (m MyExchangeMiddleware) Send(msg m.Message) error {
	//TODO implement me
	panic("implement me")
}

func (m MyExchangeMiddleware) Close() error {
	//TODO implement me
	panic("implement me")
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "guest", "guest", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	aMiddleware := &MyExchangeMiddleware{
		myConnection: conn,
		myChannel:    ch,
		myKeys:       keys,
	}
	return aMiddleware, nil
}
