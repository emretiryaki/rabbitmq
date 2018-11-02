package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"time"
)

var (
	ERROR_PREFIX     = ".Error"
	CONCURRENT_LIMIT = 1
	RETRY_COUNT      = 0
	PREFECT_COUNT    = 1
	RETRY_INTERVAL   = time.Second
)

type (
	Message struct {
		Payload       []byte
		CorrelationId string
		MessageId     string
		Timestamp     time.Time
	}

	MessageBrokerServer struct {
		context            context.Context
		shutdownFn         context.CancelFunc
		childRoutines      *errgroup.Group
		parameters         MessageBrokerParameter
		shutdownReason     string
		shutdownInProgress bool
		Consumers          []Consumer
		messageBroker      MessageBroker
	}

	Consumer struct {
		queueName         string
		exchangeName      string
		routingKey        string
		handleConsumer    handleConsumer
		errorQueueName    string
		errorExchangeName string
		brokerChannel     *BrokerChannel
		startConsumerCn   chan bool
	}

	WithBrokerServerFunc func(*MessageBrokerServer) error
	handleConsumer       func(message Message) error
)

func PrefetchCount(prefetchCount int) WithBrokerServerFunc {
	return func(m *MessageBrokerServer) error {
		m.parameters.PrefetchCount = prefetchCount
		return nil
	}
}

func RetryCount(retryCount int, retryInterval time.Duration) WithBrokerServerFunc {
	return func(m *MessageBrokerServer) error {
		m.parameters.RetryCount = retryCount
		m.parameters.RetryInterval = retryInterval
		return nil
	}
}

func (m *MessageBrokerServer) RunConsumers() error {

	go func() {

		for {
			select {

			case isConnected := <-m.messageBroker.SignalConnection():
				if !isConnected {
					m.messageBroker.CreateConnection(m.parameters)
					for _, consumer := range m.Consumers {
						consumer.startConsumerCn <- true
					}
				}
			}
		}

	}()

	for _, consumer := range m.Consumers {

		consumer := consumer

		m.childRoutines.Go(func() error {

			for {
				select {

				case isConnected := <-consumer.startConsumerCn:

					if isConnected {

						logConsole(consumer.queueName + " started to listen rabbitmq")

						var err error
						if consumer.brokerChannel, err = m.messageBroker.CreateChannel(); err != nil {
							panic(err)
						}

						consumer.createExchange(consumer.exchangeName, consumer.routingKey)
						consumer.createQueue(consumer.exchangeName, consumer.queueName, consumer.routingKey)
						consumer.createQueue(consumer.errorQueueName, consumer.errorExchangeName, consumer.routingKey)

						consumer.brokerChannel.channel.Qos(m.parameters.PrefetchCount, 0, false)

						delivery, _ := consumer.listenToQueue(consumer.queueName)

						for i := 0; i < m.parameters.PrefetchCount; i++ {

							go func() {

								for d := range delivery {

									Do(func(attempt int) (retry bool, err error) {

										retry = attempt < m.parameters.RetryCount

										defer func() {

											if r := recover(); r != nil {
												if !retry {
													consumer.brokerChannel.channel.Publish(consumer.errorExchangeName, "", false, false, convertErrorPublishMessage(d.CorrelationId, d.Body, m.parameters.RetryCount, err))
													d.Ack(false)
												}
												if err == nil {
													err = fmt.Errorf("panic occured In Consumer  %q (message %d)", d.CorrelationId, d.Body)
													consumer.brokerChannel.channel.Publish(consumer.errorExchangeName, "", false, false, convertErrorPublishMessage(d.CorrelationId, d.Body, m.parameters.RetryCount, err))
													d.Ack(false)
													retry = false
												}
												return
											}
										}()
										err = consumer.handleConsumer(Message{CorrelationId: d.CorrelationId, Payload: d.Body, MessageId: d.MessageId, Timestamp: d.Timestamp})
										if err != nil {
											panic(err)
										}
										d.Ack(false)
										return
									})
								}
							}()
						}
					}
				}
			}

			return nil
		})
	}

	return m.childRoutines.Wait()
}

func (m *MessageBrokerServer) AddConsumer(queueName string, exchangeName string, routingKey string, handleConsumer handleConsumer) {

	var consumer = Consumer{
		queueName:         queueName,
		routingKey:        routingKey,
		exchangeName:      exchangeName,
		handleConsumer:    handleConsumer,
		errorQueueName:    queueName + ERROR_PREFIX,
		errorExchangeName: queueName + ERROR_PREFIX,
		startConsumerCn:   make(chan bool),
	}

	var isAlreadyDeclareQueue bool
	for _, item := range m.Consumers {
		if item.queueName == queueName {
			isAlreadyDeclareQueue = true
		}
	}

	if !isAlreadyDeclareQueue {
		m.Consumers = append(m.Consumers, consumer)
	}

}

func (c *Consumer) createExchange(exchange string, routingKey string) error {
	var err = c.brokerChannel.channel.ExchangeDeclare(exchange, exchangeType(routingKey), true, false, false, false, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) createQueue(destinationExchange string, queueName string, routingKey string) {
	c.brokerChannel.channel.ExchangeDeclare(destinationExchange, "fanout", true, false, false, false, nil)
	q, _ := c.brokerChannel.channel.QueueDeclare(queueName, true, false, false, false, nil)
	c.brokerChannel.channel.QueueBind(q.Name, routingKey, destinationExchange, false, nil)
}

func (c Consumer) listenToQueue(queueName string) (<-chan amqp.Delivery, error) {

	msg, err := c.brokerChannel.channel.Consume(queueName,
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

func NewRabbitmqServer(uri string, withFunc ...WithBrokerServerFunc) *MessageBrokerServer {

	rootCtx, shutdownFn := context.WithCancel(context.Background())
	childRoutines, childCtx := errgroup.WithContext(rootCtx)

	messageBrokerServer := &MessageBrokerServer{
		context:       childCtx,
		shutdownFn:    shutdownFn,
		childRoutines: childRoutines,
		parameters: MessageBrokerParameter{
			Uri:             uri,
			ConcurrentLimit: CONCURRENT_LIMIT,
			RetryCount:      RETRY_COUNT,
			PrefetchCount:   PREFECT_COUNT,
			RetryInterval:   RETRY_INTERVAL,
		},
		messageBroker: NewMessageBroker(),
	}

	for _, handler := range withFunc {
		if err := handler(messageBrokerServer); err != nil {
			panic(err)
		}
	}
	return messageBrokerServer
}
