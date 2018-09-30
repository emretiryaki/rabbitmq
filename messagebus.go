package rabbitmq

import (
	"time"
	"github.com/streadway/amqp"
	"sync"
	"fmt"
)

var (
	ERROR_PREFIX        = ".Error"
	CONCURRENT_LIMIT= 1
	RETRY_COUNT=    0
	PREFECT_COUNT= 1
	RETRY_INTERVAL= time.Second
)

type (
	HandleFunc    func(*messageBus) error
	OnConsumeFunc func(Message) error

	MessageBus interface {
		Publish (payload interface{}, builders ...BuilderPublishFunc) error
		Consume (queueName string, exchangeName string, fn OnConsumeFunc)
	}

	messageBus struct {
		queues          map[string]string
		exchanges       map[string]string
		parameters      MessageBrokerParameter
		publisherClientIsFail bool
		messageBroker    MessageBroker
		brokerClientConsumer 	*BrokerChannel
		brokerClientPublisher   *BrokerChannel

	}

	Message struct {
		Payload       []byte
		CorrelationId string
		MessageId     string
		Timestamp     time.Time
	}
)

func PrefetchCount(prefetchCount int) HandleFunc {
	return func(m *messageBus) error {
		m.parameters.PrefetchCount = prefetchCount
		return nil
	}
}



func RetryCount(retryCount int,retryInterval time.Duration) HandleFunc {
	return func(m *messageBus) error {
		m.parameters.RetryCount = retryCount
		m.parameters.RetryInterval=retryInterval
		return nil
	}
}


func (mb* messageBus) Publish(payload interface{}, builders ...BuilderPublishFunc) error {

	var err error
	var exchange= getExchangeName(payload)
	var m sync.Mutex
	m.Lock()
	_, isExist := mb.exchanges[exchange]
	m.Unlock()
	if !mb.publisherClientIsFail{
		mb.messageBroker.CreateConnection(mb.parameters)
		mb.publisherClientIsFail =<- mb.messageBroker.SignalConnection()
	}

	mb.brokerClientPublisher,err = mb.messageBroker.CreateChannel()

	if err!=nil{
		return err
	}

	defer mb.brokerClientPublisher.channel.Close()

	if !isExist {
		mb.createExchangeForPublisher(exchange, "")
		mb.exchanges[exchange] = exchange
	}
	return mb.brokerClientPublisher.channel.Publish(exchange, "", false, false,  convertToPublishMessage(payload))

}

func (mb *messageBus) Consume(queueName string, exchangeName string, fn OnConsumeFunc) {

	var err error

	var errorQueue = queueName + ERROR_PREFIX
	var errorExchange = queueName + ERROR_PREFIX

	_, isExistExchange := mb.exchanges[exchangeName]

	if !isExistExchange {
		mb.exchanges[exchangeName] = exchangeName
	}

	for {

		select {

		case isConnected := <-mb.messageBroker.SignalConnection():

			if !isConnected {

				mb.messageBroker.CreateConnection(mb.parameters)

			}
			if isConnected {

				if mb.brokerClientConsumer, err = mb.messageBroker.CreateChannel(); err != nil {
					panic(err)
				}
				mb.createExchange(exchangeName, "")
				mb.createQueue(exchangeName, queueName, "")
				mb.createQueue( errorQueue, errorExchange, "")

				mb.brokerClientConsumer.channel.Qos(mb.parameters.PrefetchCount, 0, false)

				delivery, _ := mb.listenToQueue(queueName)

				for i := 0; i < mb.parameters.PrefetchCount; i++ {

					go func() {

						for m := range delivery {

							Do(func(attempt int) (retry bool, err error) {

								retry = attempt < mb.parameters.RetryCount

								defer func() {

									if r := recover(); r != nil {
										if !retry {
											mb.brokerClientConsumer.channel.Publish(errorExchange, "", false, false, convertErrorPublishMessage(m.CorrelationId, m.Body, mb.parameters.RetryCount, err))
											m.Ack(false)
										}
										if  err == nil {
											err = fmt.Errorf("panic occured In Consumer  %q (message %d)",m.CorrelationId, m.Body )
											mb.brokerClientConsumer.channel.Publish(errorExchange, "", false, false, convertErrorPublishMessage(m.CorrelationId, m.Body, mb.parameters.RetryCount, err))
											m.Ack(false)
											retry =false
										}
										return
									}
								}()
								err = fn(Message{CorrelationId: m.CorrelationId, Payload: m.Body, MessageId: m.MessageId, Timestamp: m.Timestamp})
								if err != nil {
									panic(err)
								}
								m.Ack(false)
								return
							})

						}

					}()
				}
			}
		}
	}
}



func (mb* messageBus) listenToQueue( queueName string)(<-chan amqp.Delivery,error){

	msg,err := mb.brokerClientConsumer.channel.Consume(queueName,
		"",
		false,
		false,
		false,
		false,
		nil)

	if err != nil {
		return nil, err
	}

	return msg, nil
}

func(mb *messageBus)  createQueue(destinationExchange string, queueName string, routingKey string) {

	mb.brokerClientConsumer.channel.ExchangeDeclare(destinationExchange, "fanout", true, false, false, false, nil)
	q, _ :=  mb.brokerClientConsumer.channel.QueueDeclare(queueName, true, false, false, false, nil)
	mb.brokerClientConsumer.channel.QueueBind(q.Name, routingKey, destinationExchange, false, nil)
}

func(mb *messageBus) createExchange( exchange string, routingKey string) error {
	var err = mb.brokerClientConsumer.channel.ExchangeDeclare(exchange, exchangeType(routingKey), true, false, false, false, nil)
	if err != nil {
		return err
	}
	return nil
}
func(mb *messageBus) createExchangeForPublisher( exchange string, routingKey string) error {
	var err = mb.brokerClientPublisher.channel.ExchangeDeclare(exchange, exchangeType(routingKey), true, false, false, false, nil)
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

func CreateUsingRabbitMq(uri string, handlefuncList ...HandleFunc) MessageBus {


	messageBusInstance := &messageBus{
		queues:    make(map[string]string),
		exchanges: make(map[string]string),
		messageBroker:NewMessageBroker(),
		parameters: MessageBrokerParameter{
			Uri:             uri,
			ConcurrentLimit: CONCURRENT_LIMIT,
			RetryCount:      RETRY_COUNT,
			PrefetchCount:   PREFECT_COUNT,
			RetryInterval:   RETRY_INTERVAL,
		},
	}

	for _, handler := range handlefuncList {
		if err := handler(messageBusInstance); err != nil {
			panic(err)
		}
	}

	return messageBusInstance
}


