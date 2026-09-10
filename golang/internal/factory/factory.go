package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MyQueueMiddleware struct {
	myDial    *amqp.Connection
	myChannel *amqp.Channel
	myQueue   amqp.Queue
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
		myDial:  conn,
		myQueue: queue,
	}
	return aMiddleWare, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}
