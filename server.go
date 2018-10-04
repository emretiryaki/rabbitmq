package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"os/signal"
	"syscall"

)

func NewRabbitmqServer(uri string, withFunc ...WithFunc) *MessageBrokerServer {

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

type MessageBrokerServer struct {
	context            context.Context
	shutdownFn         context.CancelFunc
	childRoutines      *errgroup.Group
	parameters         MessageBrokerParameter
	shutdownReason     string
	shutdownInProgress bool
	Consumers          []Consumer
	messageBroker      MessageBroker
}
type WithFunc func(*MessageBrokerServer) error

type handleConsumer func(message Message) error

type Consumer struct {
	queueName         string
	exchangeName      string
	routingKey        string
	handleConsumer    handleConsumer
	errorQueueName    string
	errorExchangeName string
	channel           *BrokerChannel
	startConsumerCn       chan bool
}

func (mBrokerServer *MessageBrokerServer) Shutdown(reason string) {
	mBrokerServer.shutdownReason = reason
	mBrokerServer.shutdownInProgress = true

	mBrokerServer.shutdownFn()

	mBrokerServer.childRoutines.Wait()

}

func ListenToSystemSignals(server *MessageBrokerServer) {
	signalChan := make(chan os.Signal, 1)
	ignoreChan := make(chan os.Signal, 1)

	signal.Notify(ignoreChan, syscall.SIGHUP)
	signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGTERM)

	select {
	case sig := <-signalChan:
		server.Shutdown(fmt.Sprintf("System signal: %s", sig))
	}
}

func (mBrokerServer *MessageBrokerServer) Exit(reason error) int {

	code := 1
	if reason == context.Canceled && mBrokerServer.shutdownReason != "" {
		reason = fmt.Errorf(mBrokerServer.shutdownReason)
		code = 0
	}
	return code
}

func (mBrokerServer *MessageBrokerServer) RunConsumers() error {
	sendSystemNotification("READY=1")

	go func() {

		for {
			select {

			case isConnected := <-mBrokerServer.messageBroker.SignalConnection():
				if !isConnected {
					mBrokerServer.messageBroker.CreateConnection(mBrokerServer.parameters)
					for _, consumer := range mBrokerServer.Consumers {
						consumer.startConsumerCn <- true
					}
				}
			}
		}

	}()
	for _, consumer := range mBrokerServer.Consumers {

		consumer := consumer

		mBrokerServer.childRoutines.Go(func() error {

			for {
				select {

				case isConnected := <-consumer.startConsumerCn:

					if isConnected {

						logConsole(consumer.queueName + " started to listen rabbitmq")

						var err error

						if consumer.channel, err = mBrokerServer.messageBroker.CreateChannel(); err != nil {
							panic(err)
						}

						consumer.createExchange(consumer.exchangeName, consumer.routingKey)
						consumer.createQueue(consumer.exchangeName, consumer.queueName, consumer.routingKey)
						consumer.createQueue(consumer.errorQueueName, consumer.errorExchangeName, consumer.routingKey)

						consumer.channel.channel.Qos(mBrokerServer.parameters.PrefetchCount, 0, false)

						delivery, _ := consumer.listenToQueue(consumer.queueName)

						for i := 0; i < mBrokerServer.parameters.PrefetchCount; i++ {

							go func() {

								for m := range delivery {

									Do(func(attempt int) (retry bool, err error) {

										retry = attempt < mBrokerServer.parameters.RetryCount

										defer func() {

											if r := recover(); r != nil {
												if !retry {
													consumer.channel.channel.Publish(consumer.errorExchangeName, "", false, false, convertErrorPublishMessage(m.CorrelationId, m.Body, mBrokerServer.parameters.RetryCount, err))
													m.Ack(false)
												}
												if  err == nil {
													err = fmt.Errorf("panic occured In Consumer  %q (message %d)",m.CorrelationId, m.Body )
													consumer.channel.channel.Publish(consumer.errorExchangeName, "", false, false, convertErrorPublishMessage(m.CorrelationId, m.Body, mBrokerServer.parameters.RetryCount, err))
													m.Ack(false)
													retry =false
												}
												return
											}
										}()
										err = consumer.handleConsumer(Message{CorrelationId: m.CorrelationId, Payload: m.Body, MessageId: m.MessageId, Timestamp: m.Timestamp})
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

			return nil
		})
	}

	return mBrokerServer.childRoutines.Wait()
}


func (consumer Consumer) listenToQueue(queueName string) (<-chan amqp.Delivery, error) {

	msg, err := consumer.channel.channel.Consume(queueName,
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

func (mBrokerServer *MessageBrokerServer) AddConsumer(queueName string, exchangeName string, routingKey string, handleConsumer handleConsumer) {

	var consumer = Consumer{
		queueName:         queueName,
		routingKey:        routingKey,
		exchangeName:      exchangeName,
		handleConsumer:    handleConsumer,
		errorQueueName:    queueName + ERROR_PREFIX,
		errorExchangeName: queueName + ERROR_PREFIX,
		startConsumerCn:       make(chan bool),
	}

	var isAlreadyDeclareQueue bool
	for _, item := range mBrokerServer.Consumers {
		if item.queueName == queueName {
			isAlreadyDeclareQueue = true
		}
	}

	if !isAlreadyDeclareQueue {
		mBrokerServer.Consumers = append(mBrokerServer.Consumers, consumer)
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
	var err = consumer.channel.channel.ExchangeDeclare(exchange, exchangeType(routingKey), true, false, false, false, nil)
	if err != nil {
		return err
	}
	return nil
}

func (consumer *Consumer) createQueue(destinationExchange string, queueName string, routingKey string) {
	consumer.channel.channel.ExchangeDeclare(destinationExchange, "fanout", true, false, false, false, nil)
	q, _ := consumer.channel.channel.QueueDeclare(queueName, true, false, false, false, nil)
	consumer.channel.channel.QueueBind(q.Name, routingKey, destinationExchange, false, nil)
}
