package rabbitmq

import (
	"time"
	"fmt"
	"strings"
	"github.com/streadway/amqp"
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
		Consume (queueName string, consumeMessage interface{}, fn OnConsumeFunc)
	}

	messageBus struct {
		queues          map[string]string
		exchanges       map[string]string
		parameters      MessageBrokerParameter
		consumeIsFailOnce bool
		messageBroker    MessageBroker
		brokerClientConsumer 	*BrokerChannel
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
	var brokerClient *BrokerChannel
	var exchange= getExchangeName(payload)
	_, isExist := mb.exchanges[exchange]

	brokerClient,err = mb.messageBroker.CreateChannel()

	if err != nil {

		if err = mb.messageBroker.CreateConnection(mb.parameters) ; err != nil {
			return err
		}

	}

	defer brokerClient.channel.Close()

	if isExist {
		return brokerClient.channel.Publish(exchange, "", false, false,  convertToPublishMessage(payload))

		} else {

		createExchange(brokerClient.channel,exchange, "")
		mb.exchanges[exchange] = exchange
		return brokerClient.channel.Publish(exchange, "", false, false,  convertToPublishMessage(payload))
	}

}

func (mb *messageBus) Consume(queueName string, consumeMessage interface{}, fn OnConsumeFunc) {

	var broker *BrokerChannel
	var err error
	var exchangeName= getExchangeName(consumeMessage)

	var errorQueue= queueName + ERROR_PREFIX
	var errorExchange= queueName + ERROR_PREFIX

	_, isExistExchange := mb.exchanges[exchangeName]

	if broker, err = mb.messageBroker.CreateChannel(); err != nil {
		panic(err)

	} else {
		mb.brokerClientConsumer = broker

	}

	if !isExistExchange {
		createExchange(broker.channel, exchangeName, "")
		mb.exchanges[exchangeName] = exchangeName
	}

	createQueue(broker.channel,exchangeName, queueName, "")
	createQueue(broker.channel,errorQueue, errorExchange, "")

	defer mb.brokerClientConsumer.channel.Close()

	mb.brokerClientConsumer.channel.Qos(mb.parameters.PrefetchCount, 0, false)

	for {

		select {

		case isConnected := <-mb.messageBroker.SignalConnection():

			if !isConnected {

				if err = mb.messageBroker.CreateConnection(mb.parameters); err != nil {
					continue
				}
				mb.brokerClientConsumer, _ = mb.messageBroker.CreateChannel()

				continue

			} else if isConnected {

				delivery, _ := mb.listenToQueue(queueName)

				for i := 0; i < mb.parameters.PrefetchCount; i++ {

					go func() {

						for m := range delivery {

							Do(func(attempt int) (retry bool, err error) {

								retry = attempt < mb.parameters.RetryCount

								defer func() {

									if r := recover(); r != nil {
										fmt.Println("recovered:", r)
										if err!=nil && strings.Index(err.Error(), "504") > -1 {
											mb.messageBroker.SignalConnectionStatus(false)
											return
										}
										if !retry {

											brokerError := broker.channel.Publish(errorExchange, "", false, false, convertErrorPublishMessage(m.CorrelationId, m.Body, mb.parameters.RetryCount, err))

											if brokerError == nil {
												m.Ack(false)
											}else {
												m.Nack(false, true) //Requeue When An Panic Occured In Consumer
											}
										}
										if err == nil {
											m.Nack(false, true) //Requeue When An Panic Occured In Consumer
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

func  createQueue(channel *amqp.Channel,destinationExchange string, queueName string, routingKey string) {

	channel.ExchangeDeclare(destinationExchange, "fanout", true, false, false, false, nil)
	q, _ :=  channel.QueueDeclare(queueName, true, false, false, false, nil)
	channel.QueueBind(q.Name, routingKey, destinationExchange, false, nil)
}

func createExchange(channel *amqp.Channel, exchange string, routingKey string) error {
	var err = channel.ExchangeDeclare(exchange, exchangeType(routingKey), true, false, false, false, nil)
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

	messageBusInstance.messageBroker.CreateConnection(messageBusInstance.parameters)

	return messageBusInstance
}


