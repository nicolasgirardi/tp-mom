package factory

import (
	"fmt"
	"sync"

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
	err := mQ.myChannel.Publish("", mQ.myQueue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg.Body)})
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
		channel.Close()
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
	firstMessage := true
	for msg := range msgs {
		if firstMessage {
			mE.myConsumerTag.Store(msg.ConsumerTag)
		}
		firstMessage = false
		message := m.Message{Body: string(msg.Body)}
		ack := func() { _ = msg.Ack(false) }
		nack := func() { _ = msg.Nack(false, true) }
		callbackFunc(message, ack, nack)
	}
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
	errC := mE.myConnection.Close()
	errQ := mE.myChannel.Close()
	if errC != nil || errQ != nil {
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
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		ch.Close()
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
		ch.Close()
		conn.Close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	for _, key := range keys {
		err = ch.QueueBind(queue.Name, key, exchange, false, nil)
		if err != nil {
			conn.Close()
			ch.Close()
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
