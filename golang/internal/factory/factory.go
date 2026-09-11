package factory

import (
	"fmt"
	"io"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ConsumeFromQueue(msgs <-chan amqp.Delivery, callbackFunc func(msg m.Message, ack func(), nack func()), tag *SecureString) {
	firstMessage := true
	for msg := range msgs {
		if firstMessage {
			tag.Store(msg.ConsumerTag)
		}
		firstMessage = false
		message := m.Message{Body: string(msg.Body)}
		ack := func() { _ = msg.Ack(false) }
		nack := func() { _ = msg.Nack(false, true) }
		callbackFunc(message, ack, nack)
	}
}

func CloseResources(resources ...io.Closer) error {
	var errors []error
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		if err := resource.Close(); err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

type SecureString struct {
	text string
	mu   sync.Mutex
}

func NewSecureString(text string) *SecureString {
	return &SecureString{text: text}
}

func (s *SecureString) Compare(text string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if mQ.consuming {
		mQ.mutex.Unlock()
		return nil
	}
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
	ConsumeFromQueue(msgs, callbackFunc, mQ.myConsumerTag)
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
	err := mQ.myChannel.Publish("", mQ.myQueue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg.Body)})
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (mQ *MyQueueMiddleware) Close() error {
	return CloseResources(mQ.myChannel, mQ.myConnection)
}

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "guest", "guest", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		er := CloseResources(conn)
		if er != nil {
			return nil, er
		}
		return nil, err
	}
	queue, err := channel.QueueDeclare(queueName, true, false, false, false, amqp.Table{
		amqp.QueueTypeArg: amqp.QueueTypeQuorum,
	})
	if err != nil {
		er := CloseResources(channel, conn)
		if er != nil {
			return nil, er
		}
		return nil, err
	}
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
	myConnection   *amqp.Connection
	myChannel      *amqp.Channel
	myQueue        amqp.Queue
	myExchangeName string
	myKeys         []string
	myConsumerTag  *SecureString
	consuming      bool
	mutex          sync.Mutex
}

func (mE *MyExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {

	mE.mutex.Lock()
	if mE.consuming {
		mE.mutex.Unlock()
		return nil
	}
	msgs, er := mE.myChannel.Consume(mE.myQueue.Name, "", false, false, false, false, nil)
	if er != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	ConsumeFromQueue(msgs, callbackFunc, mE.myConsumerTag)
	mE.mutex.Lock()
	defer mE.mutex.Unlock()
	if mE.consuming {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (mE *MyExchangeMiddleware) StopConsuming() error {
	if !mE.consuming {
		return nil
	}
	consumerTag := mE.myConsumerTag.Text()
	mE.mutex.Lock()
	defer mE.mutex.Unlock()
	mE.consuming = false
	err := mE.myChannel.Cancel(consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	panic("implement me")
}

func (mE *MyExchangeMiddleware) Send(msg m.Message) error {
	for _, key := range mE.myKeys {
		err := mE.myChannel.Publish(mE.myExchangeName, key, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg.Body)})
		if err != nil {
			return m.ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

func (mE *MyExchangeMiddleware) Close() error {
	return CloseResources(mE.myChannel, mE.myConnection)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "guest", "guest", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		er := CloseResources(conn)
		if er != nil {
			return nil, er
		}
		return nil, err
	}
	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		er := CloseResources(ch, conn)
		if er != nil {
			return nil, er
		}
		return nil, err
	}
	queue, err := ch.QueueDeclare(
		"",    // name
		false, // durability
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		er := CloseResources(ch, conn)
		if er != nil {
			return nil, er
		}
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	for _, key := range keys {
		err = ch.QueueBind(queue.Name, key, exchange, false, nil)
		if err != nil {
			er := CloseResources(conn)
			if er != nil {
				return nil, er
			}
			return nil, m.ErrMessageMiddlewareMessage
		}
	}
	aMiddleware := &MyExchangeMiddleware{
		myConnection:   conn,
		myChannel:      ch,
		myQueue:        queue,
		myExchangeName: exchange,
		myKeys:         keys,
		myConsumerTag:  NewSecureString(""),
		consuming:      false,
	}
	return aMiddleware, nil
}
