package factory

import (
	"fmt"
	"io"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ConsumeFromQueue(msgs <-chan amqp.Delivery, callbackFunc func(msg m.Message, ack func(), nack func()), tag *SyncString) {
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

type SyncString struct {
	text string
	mu   sync.Mutex
}

func NewSecureString(text string) *SyncString {
	return &SyncString{text: text}
}

func (s *SyncString) Compare(text string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.text == text
}

func (s *SyncString) Text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.text
}

func (s *SyncString) Store(newText string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.text = newText
}

type MyQueueMiddleware struct {
	myConnection  *amqp.Connection
	myChannel     *amqp.Channel
	myQueue       amqp.Queue
	myConsumerTag *SyncString
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
	msgs, err := mQ.myChannel.Consume(
		mQ.myQueue.Name,
		mQ.myConsumerTag.Text(),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		mQ.mutex.Unlock()
		return m.ErrMessageMiddlewareDisconnected
	}
	mQ.mutex.Unlock()
	ConsumeFromQueue(msgs, callbackFunc, mQ.myConsumerTag)
	mQ.mutex.Lock()
	defer mQ.mutex.Unlock()
	if mQ.consuming {
		mQ.consuming = false
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (mQ *MyQueueMiddleware) StopConsuming() error {
	mQ.mutex.Lock()
	consumerTag := mQ.myConsumerTag.Text()
	if !mQ.consuming || consumerTag == "" {
		mQ.mutex.Unlock()
		return nil
	}
	mQ.consuming = false
	mQ.mutex.Unlock()
	err := mQ.myChannel.Cancel(consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	mQ.myConsumerTag.Store("")
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
	err := mQ.StopConsuming()
	if err != nil {
		return err
	}
	return CloseResources(mQ.myChannel, mQ.myConnection)
}

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "guest", "guest", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	channel, err := conn.Channel()
	if err != nil {
		er := CloseResources(conn)
		if er != nil {
			return nil, er
		}
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	queue, err := channel.QueueDeclare(queueName, true, false, false, false, amqp.Table{
		amqp.QueueTypeArg: amqp.QueueTypeClassic,
	})
	if err != nil {
		er := CloseResources(channel, conn)
		if er != nil {
			return nil, er
		}
		return nil, m.ErrMessageMiddlewareDisconnected
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
	myConsumerTag  *SyncString
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
	mE.consuming = true
	mE.mutex.Unlock()
	ConsumeFromQueue(msgs, callbackFunc, mE.myConsumerTag)
	mE.mutex.Lock()
	defer mE.mutex.Unlock()
	if mE.consuming {
		mE.consuming = false
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (mE *MyExchangeMiddleware) StopConsuming() error {
	mE.mutex.Lock()
	if !mE.consuming {
		mE.mutex.Unlock()
		return nil
	}
	consumerTag := mE.myConsumerTag.Text()
	mE.consuming = false
	mE.mutex.Unlock()
	err := mE.myChannel.Cancel(consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
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
	err := mE.StopConsuming()
	if err != nil {
		return err
	}
	return CloseResources(mE.myChannel, mE.myConnection)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "guest", "guest", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	ch, err := conn.Channel()
	if err != nil {
		er := CloseResources(conn)
		if er != nil {
			return nil, er
		}
		return nil, m.ErrMessageMiddlewareDisconnected
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
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	queue, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
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
