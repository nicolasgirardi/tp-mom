package factory

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SecureString struct {
	text string
	mu   sync.Mutex
}

func NewSecureString(text string) *SecureString {
	return &SecureString{text: text}
}

func (s *SecureString) Compare(text string) bool {
	return s.text == text
}

func (s *SecureString) Text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.text
}

func (s *SecureString) Store(newText string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.text = newText
}

type MyQueueMiddleware struct {
	myConnection  *amqp.Connection
	myChannel     *amqp.Channel
	myQueue       amqp.Queue
	myConsumerTag *SecureString
	consuming     bool
	mutex         sync.Mutex
}

func (mQ *MyQueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	mQ.mutex.Lock()
	mQ.consuming = true
	mQ.mutex.Unlock()
	msgs, err := mQ.myChannel.Consume(
		mQ.myQueue.Name, // queue
		mQ.myConsumerTag.Text(),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	firstMessage := true
	for msg := range msgs {
		if firstMessage {
			mQ.myConsumerTag.Store(msg.ConsumerTag)
		}
		firstMessage = false
		message := m.Message{Body: string(msg.Body)}
		ack := func() { _ = msg.Ack(false) }
		nack := func() { _ = msg.Nack(false, true) }
		callbackFunc(message, ack, nack)
	}
	mQ.mutex.Lock()
	defer mQ.mutex.Unlock()
	if mQ.consuming {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (mQ *MyQueueMiddleware) StopConsuming() error {
	if !mQ.consuming {
		return nil
	}
	consumerTag := mQ.myConsumerTag.Text()
	mQ.mutex.Lock()
	defer mQ.mutex.Unlock()
	mQ.consuming = false
	err := mQ.myChannel.Cancel(consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (mQ *MyQueueMiddleware) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := mQ.myChannel.PublishWithContext(ctx, "", mQ.myQueue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg.Body)})
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (mQ *MyQueueMiddleware) Close() error {
	errC := mQ.myConnection.Close()
	errQ := mQ.myChannel.Close()
	if errC != nil || errQ != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
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
	var consuming atomic.Bool
	consuming.Store(false)
	aMiddleWare := &MyQueueMiddleware{
		myConnection:  conn,
		myChannel:     channel,
		myQueue:       queue,
		myConsumerTag: NewSecureString(""),
		consuming:     false,
	}
	return aMiddleWare, nil
}

type MyExchangeMiddleware struct {
	myConnection *amqp.Connection
	myChannel    *amqp.Channel
	myKeys       []string
}

func (mE *MyExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	//TODO implement me
	panic("implement me")
}

func (mE *MyExchangeMiddleware) StopConsuming() error {
	//TODO implement me
	panic("implement me")
}

func (mE *MyExchangeMiddleware) Send(msg m.Message) error {
	//TODO implement me
	panic("implement me")
}

func (mE *MyExchangeMiddleware) Close() error {
	err := mE.myConnection.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
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
