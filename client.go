package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"time"
)

var (
	ERRORPREFIX     = ".Error"
	CONCURRENTLIMIT = 1
	RETRYCOUNT      =    0
	PREFECTCOUNT    = 1
	RETRY_INTERVAL  = time.Second
)

func NewRabbitMqClient(uri string, withFunc ...withFunc) *MessageBrokerServer {

	rootCtx, shutdownFn := context.WithCancel(context.Background())
	childRoutines, childCtx := errgroup.WithContext(rootCtx)

	messageBrokerServer := &MessageBrokerServer{
		context:       childCtx,
		shutdownFn:    shutdownFn,
		childRoutines: childRoutines,
		parameters: MessageBrokerParameter{
			Uri:             uri,
			ConcurrentLimit: CONCURRENTLIMIT,
			RetryCount:      RETRYCOUNT,
			PrefetchCount:   PREFECTCOUNT,
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
type Message struct {
	Payload       []byte
	CorrelationId string
	MessageId     string
	Timestamp     time.Time
}
type MessageBrokerServer struct {
	context            context.Context
	shutdownFn         context.CancelFunc
	childRoutines      *errgroup.Group
	parameters         MessageBrokerParameter
	shutdownReason     string
	shutdownInProgress bool
	consumers          []Consumer
	messageBroker      MessageBroker
}
type withFunc func(*MessageBrokerServer) error

type handleConsumer func(message Message) error

type Consumer struct {
	queueName         string
	exchangeName      string
	routingKey        string
	handleConsumer    handleConsumer
	errorQueueName    string
	errorExchangeName string
	brokerChannel     *BrokerChannel
	startConsumerCn   chan bool
}


func PrefetchCount(prefetchCount int) withFunc {
	return func(m *MessageBrokerServer) error {
		m.parameters.PrefetchCount = prefetchCount
		return nil
	}
}

func RetryCount(retryCount int,retryInterval time.Duration) withFunc {
	return func(m *MessageBrokerServer) error {
		m.parameters.RetryCount = retryCount
		m.parameters.RetryInterval=retryInterval
		return nil
	}
}

func (m *MessageBrokerServer) Shutdown(reason string) {
	m.shutdownReason = reason
	m.shutdownInProgress = true

	m.shutdownFn()

	m.childRoutines.Wait()

}


func (m *MessageBrokerServer) Exit(reason error) int {

	code := 1
	if reason == context.Canceled && m.shutdownReason != "" {
		reason = fmt.Errorf(m.shutdownReason)
		code = 0
	}
	return code
}

func (m *MessageBrokerServer) RunConsumers() error {

	sendSystemNotification("READY=1")

	go func() {

		for {
			select {

			case isConnected := <-m.messageBroker.SignalConnection():
				if !isConnected {
					m.messageBroker.CreateConnection(m.parameters)
					for _, consumer := range m.consumers {
						consumer.startConsumerCn <- true
					}
				}
			}
		}

	}()
	for _, consumer := range m.consumers {

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
													consumer.brokerChannel.channel.Publish(consumer.errorExchangeName, "", false, false, errorPublishMessage(d.CorrelationId, d.Body, m.parameters.RetryCount, err))
													d.Ack(false)
												}
												if  err == nil {
													err = fmt.Errorf("panic occured In Consumer  %q (message %d)",d.CorrelationId, d.Body )
													consumer.brokerChannel.channel.Publish(consumer.errorExchangeName, "", false, false, errorPublishMessage(d.CorrelationId, d.Body, m.parameters.RetryCount, err))
													d.Ack(false)
													retry =false
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


func (consumer Consumer) listenToQueue(queueName string) (<-chan amqp.Delivery, error) {

	msg, err := consumer.brokerChannel.channel.Consume(queueName,
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

func (m *MessageBrokerServer) AddConsumer(queueName string, exchangeName string, routingKey string, handleConsumer handleConsumer) {

	var consumer = Consumer{
		queueName:         queueName,
		routingKey:        routingKey,
		exchangeName:      exchangeName,
		handleConsumer:    handleConsumer,
		errorQueueName:    queueName + ERRORPREFIX,
		errorExchangeName: queueName + ERRORPREFIX,
		startConsumerCn:       make(chan bool),
	}

	var isAlreadyDeclareQueue bool
	for _, item := range m.consumers {
		if item.queueName == queueName {
			isAlreadyDeclareQueue = true
		}
	}

	if !isAlreadyDeclareQueue {
		m.consumers = append(m.consumers, consumer)
	}

}

func sendSystemNotification(state string) error {

	notifySocket := os.Getenv("NOTIFY_SOCKET")

	if notifySocket == "" {
		return fmt.Errorf("NOTIFY_SOCKET environment variable empty or unset.")
	}
	socketAddr := &net.UnixAddr{
		Name: notifySocket,
		Net:  "unixgram",
	}

	conn, err := net.DialUnix(socketAddr.Net, nil, socketAddr)

	if err != nil {
		return err
	}

	_, err = conn.Write([]byte(state))

	conn.Close()

	return err

}
func (consumer *Consumer) createExchange(exchange string, routingKey string) error {
	var err = consumer.brokerChannel.channel.ExchangeDeclare(exchange, exchangeType(routingKey), true, false, false, false, nil)
	if err != nil {
		return err
	}
	return nil
}

func (consumer *Consumer) createQueue(destinationExchange string, queueName string, routingKey string) {
	consumer.brokerChannel.channel.ExchangeDeclare(destinationExchange, "fanout", true, false, false, false, nil)
	q, _ := consumer.brokerChannel.channel.QueueDeclare(queueName, true, false, false, false, nil)
	consumer.brokerChannel.channel.QueueBind(q.Name, routingKey, destinationExchange, false, nil)
}
func exchangeType(routingKey string) string {
	var exchangeType = "fanout"
	if routingKey != "" {
		exchangeType = "direct"
	}
	return exchangeType
}