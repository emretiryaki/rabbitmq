package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

var (
	oncebus            sync.Once
	messageBusInstance *messageBus
	ErrorPrefix        = ".Error"
	retryCount         = 0
)

type (
	HandleFunc    func(*messageBus) error
	OnConsumeFunc func(Message) error

	MessageBus interface {
		Publish(payload interface{}, builders ...BuilderPublishFunc) error
		Listen(queueName string, consumeMessage interface{}, fn OnConsumeFunc)
	}

	messageBus struct {
		queues          map[string]string
		exchanges       map[string]string
		concurrentCount int
		retryCount      int
		connection      *amqp.Connection
		channel         *amqp.Channel
		uri             string
	}

	Message struct {
		Payload       []byte
		CorrelationId string
		MessageId     string
		Timestamp     time.Time
	}
)

func RetryCount(retryCount int) HandleFunc {
	return func(m *messageBus) error {
		m.retryCount = retryCount
		return nil
	}
}

func (m messageBus) Publish(payload interface{}, builders ...BuilderPublishFunc) error {

	m.createChannel()
	defer m.channel.Close()

	var exchange = getExchangeName(payload)
	_, isExist := m.exchanges[exchange]

	var message = publishMessage{Payload: payload}

	for _, handler := range builders {
		handler(&message)
	}
	var publishingMessage = convertPublishMessage(message)

	if isExist {
		return m.channel.Publish(exchange, "", false, false, publishingMessage)
	} else {
		m.createExchange(exchange, "")
		return m.channel.Publish(exchange, "", false, false, publishingMessage)
	}
}

func (mb *messageBus) Listen(queueName string, consumeMessage interface{}, fn OnConsumeFunc) {

	mb.createChannel()
	var exchangeName = getExchangeName(consumeMessage)
	destinationExchange := queueName

	var errorQueue = destinationExchange + ErrorPrefix
	var errorExchange = queueName + ErrorPrefix

	_, isExistExchange := mb.exchanges[exchangeName]

	if !isExistExchange {
		mb.createExchange(exchangeName, "")
	}

	mb.createQueue(destinationExchange, queueName, "")
	mb.channel.ExchangeBind(destinationExchange, "", exchangeName, true, nil)
	mb.createQueue(errorQueue, errorExchange, "")

	defer mb.channel.Close()
	defer mb.connection.Close()

	for {
		var conn, err = createConn(mb.uri)
		if err != nil {
			time.Sleep(time.Second*5)
			mb.connection.Close()
			mb.channel.Close()
			continue
		}
		mb.connection = conn
		mb.createChannel()

		delivery, _ := mb.channel.Consume(queueName,
			"",
			false,
			false,
			false,
			false,
			nil)

		for m := range delivery {

			Do(func(attempt int) (retry bool, err error) {
				retry = attempt < mb.retryCount

				defer func() {

					if r := recover(); r != nil {
						fmt.Println("recovered:", r)
						if !retry {
							mb.channel.Publish(errorExchange, "", false, false, convertErrorPublishMessage(m.CorrelationId, m.Body))
							m.Ack(false)
						}
						if err == nil{
							m.Nack(false,true) //Requeue When An Panic Occured In Consumer
						}
						return
					}
				}()
				err = fn(Message{CorrelationId: m.CorrelationId, Payload: m.Body, MessageId: m.MessageId, Timestamp: m.Timestamp})
				if err != nil {
					panic(err)
				}
				err = m.Ack(false)
				if err != nil {
					panic(err)
				}
				return
			})
		}
	}
}

func (mb *messageBus) createQueue(destinationExchange string, queueName string, routingKey string) {
	mb.channel.ExchangeDeclare(destinationExchange, "fanout", true, false, false, false, nil)
	q, _ := mb.channel.QueueDeclare(queueName, true, false, false, false, nil)
	mb.channel.QueueBind(q.Name, routingKey, destinationExchange, false, nil)
}

func (mb *messageBus) createExchange(exchange string, routingKey string) error {
	var err = mb.channel.ExchangeDeclare(exchange, exchangeType(routingKey), true, false, false, false, nil)
	mb.exchanges[exchange] = exchange
	if err != nil {
		return err
	}
	return nil
}

func exchangeType(routingKey string) string {
	var exchangeType = "fanout"
	if routingKey != "" {
		exchangeType = "direct"
	}
	return exchangeType
}

func createConn(dsn string) (*amqp.Connection, error) {
	conn, error := amqp.Dial(dsn)
	return conn, error
}

func (m *messageBus) createChannel() error {
	var channel, err = m.connection.Channel()
	m.channel = channel
	return err
}

func CreateUsingRabbitMq(uri string, handlefunceList ...HandleFunc) MessageBus {

	oncebus.Do(func() {
		var conn, _ = createConn(uri)

		messageBusInstance = &messageBus{
			retryCount: retryCount,
			queues:     make(map[string]string),
			exchanges:  make(map[string]string),
			connection: conn,
			uri:        uri,
		}
		for _, handler := range handlefunceList {
			if err := handler(messageBusInstance); err != nil {
				panic(err)
			}
		}

	})
	return messageBusInstance
}
