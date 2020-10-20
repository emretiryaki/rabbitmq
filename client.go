package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"reflect"
	"runtime"
	"time"
)

var (
	ERRORPREFIX     = "_error"
	CONCURRENTLIMIT = 1
	RETRYCOUNT      = 0
	PREFECTCOUNT    = 1
	reconnectDelay  = 2 * time.Second
)

const (
	Direct            ExchangeType = 1
	Fanout            ExchangeType = 2
	Topic             ExchangeType = 3
	ConsistentHashing ExchangeType = 4
	XDelayedMessage   ExchangeType = 5
)

type (
	ExchangeType int

	Client struct {
		context            context.Context
		shutdownFn         context.CancelFunc
		childRoutines      *errgroup.Group
		parameters         MessageBrokerParameter
		shutdownReason     string
		shutdownInProgress bool
		consumerBuilder    ConsumerBuilder
		publisherBuilder   *PublisherBuilder
	}

	withFunc func(*Client) error
)

func (c *Client) checkConsumerConnection() {

	go func() {

		for {
			select {

			case isConnected := <-c.consumerBuilder.messageBroker.SignalConnection():
				if !isConnected {
					c.consumerBuilder.messageBroker.CreateConnection(c.parameters)
					for _, consumer := range c.consumerBuilder.consumers {
						consumer.startConsumerCn <- true
					}
				}
			}
		}

	}()
}

func (c *Client) checkPublisherConnection() {

	if c.publisherBuilder.isAlreadyStartConnection {
		return
	}

	c.publisherBuilder.isAlreadyStartConnection = true

	go func() {

		for {

			select {

			case isConnected := <-c.publisherBuilder.messageBroker.SignalConnection():

				if !isConnected {
					c.publisherBuilder.messageBroker.CreateConnection(c.parameters)
					c.publisherBuilder.startPublisherCh <- true

				}
			}
		}

	}()
}

func NewRabbitMqClient(nodes []string, userName string, password string, virtualHost string, withFunc ...withFunc) *Client {

	rootCtx, shutdownFn := context.WithCancel(context.Background())
	childRoutines, childCtx := errgroup.WithContext(rootCtx)

	client := &Client{
		context:       childCtx,
		shutdownFn:    shutdownFn,
		childRoutines: childRoutines,
		consumerBuilder: ConsumerBuilder{
			messageBroker: NewMessageBroker(),
		},
		publisherBuilder: &PublisherBuilder{
			messageBroker:    NewMessageBroker(),
			startPublisherCh: make(chan bool),
		},
		parameters: MessageBrokerParameter{
			Nodes:           nodes,
			ConcurrentLimit: CONCURRENTLIMIT,
			RetryCount:      RETRYCOUNT,
			PrefetchCount:   PREFECTCOUNT,
			Password:        password,
			UserName:        userName,
			VirtualHost:     virtualHost,
		},
	}

	for _, handler := range withFunc {
		if err := handler(client); err != nil {
			panic(err)
		}
	}
	return client
}

func PrefetchCount(prefetchCount int) withFunc {
	return func(m *Client) error {
		m.parameters.PrefetchCount = prefetchCount
		return nil
	}
}

func RetryCount(retryCount int) withFunc {
	return func(m *Client) error {
		m.parameters.RetryCount = retryCount
		//m.parameters.RetryInterval = retryInterval
		return nil
	}
}

func (c *Client) Shutdown(reason string) {
	c.shutdownReason = reason
	c.shutdownInProgress = true
	c.shutdownFn()
	c.childRoutines.Wait()
}

func (c *Client) Exit(reason error) int {
	code := 1
	if reason == context.Canceled && c.shutdownReason != "" {
		reason = fmt.Errorf(c.shutdownReason)
		code = 0
	}
	return code
}

func (c *Client) RunConsumers() error {

	sendSystemNotification("READY=1")

	c.checkConsumerConnection()

	for _, consumer := range c.consumerBuilder.consumers {

		consumer := consumer
		c.childRoutines.Go(func() error {

			for {
				select {
				case isConnected := <-consumer.startConsumerCn:

					if isConnected {

						var (
							err           error
							brokerChannel *Channel
						)

						if brokerChannel, err = c.consumerBuilder.messageBroker.CreateChannel(); err != nil {
							panic(err)
						}

						brokerChannel.createQueue(consumer.queueName).createErrorQueueAndBind(consumer.errorExchangeName, consumer.errorQueueName)

						for _, item := range consumer.exchanges {

							brokerChannel.
								createExchange(item.exchangeName, item.exchangeType, item.args).
								exchangeBind(item.exchangeName, consumer.queueName, item.routingKey, item.exchangeType)

						}

						brokerChannel.rabbitChannel.Qos(c.parameters.PrefetchCount, 0, false)

						c.runV2(brokerChannel, consumer)

					}
				}
			}

			return nil
		})
	}

	return c.childRoutines.Wait()
}

func (c *Client) AddConsumer(queueName string) *Consumer {

	var consumerDefination = &Consumer{
		queueName:         queueName,
		errorQueueName:    queueName + ERRORPREFIX,
		errorExchangeName: queueName + ERRORPREFIX,
		startConsumerCn:   make(chan bool),
		singleGoroutine:   false,
	}

	var isAlreadyDeclareQueue bool
	for _, item := range c.consumerBuilder.consumers {
		if item.queueName == queueName {
			isAlreadyDeclareQueue = true
		}
	}

	if !isAlreadyDeclareQueue {
		c.consumerBuilder.consumers = append(c.consumerBuilder.consumers, consumerDefination)
	}
	return consumerDefination
}

func (c *Client) runV2(brokerChannel *Channel, defination *Consumer) {

	delivery, _ := brokerChannel.listenToQueue(defination.queueName)

	if defination.singleGoroutine {

		c.deliver(brokerChannel, defination, delivery)

	} else {
		for i := 0; i < c.parameters.PrefetchCount; i++ {
			go func() {
				c.deliver(brokerChannel, defination, delivery)
			}()
		}
	}

}

func (c *Client) deliver(brokerChannel *Channel, consumer *Consumer, delivery <-chan amqp.Delivery) {
	for d := range delivery {

		Do(func(attempt int) (retry bool, err error) {

			retry = attempt < c.parameters.RetryCount

			defer func() {

				if r := recover(); r != nil {

					if !retry || err == nil {

						err, ok := r.(error)

						if !ok {
							retry = false //Because of panic exception
							err = fmt.Errorf("%v", r)
						}

						stack := make([]byte, 4<<10)
						length := runtime.Stack(stack, false)

						brokerChannel.rabbitChannel.Publish(consumer.errorExchangeName, "", false, false, errorPublishMessage(d.CorrelationId, d.Body, c.parameters.RetryCount, err, fmt.Sprintf("[Exception Recover] %v %s\n", err, stack[:length])))

						select {
						case confirm := <-brokerChannel.notifyConfirm:
							if confirm.Ack {
								break
							}
						case <-time.After(resendDelay):
						}

						d.Ack(false)
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
}

func (c *Client) AddPublisher(exchangeName string, exchangeType ExchangeType, payloads ...interface{}) {

	var payloadTypes []reflect.Type
	for _, item := range payloads {
		if reflect.TypeOf(item).Kind() != reflect.Struct {
			panic(fmt.Sprintf("%s  is not struct", reflect.TypeOf(item).Name()))
		} else {
			payloadTypes = append(payloadTypes, reflect.TypeOf(item))
		}

	}
	var publisher = Publisher{
		exchangeName: exchangeName,
		exchangeType: exchangeType,
		payloadTypes: payloadTypes,
	}

	if len(payloads) == 0 || payloads == nil {
		panic("payloads are not empty")
	}

	for _, item := range c.publisherBuilder.publishers {
		if item.exchangeName == exchangeName {
			panic(fmt.Sprintf("%s exchangename is already declared ", exchangeName))
		}
	}

	c.publisherBuilder.publishers = append(c.publisherBuilder.publishers, publisher)

	c.checkPublisherConnection()

	c.publisherBuilder.CreateChannel()

}

func (c *Client) Publish(ctx context.Context, routingKey string, payload interface{}) error {

	var (
		publisher *Publisher
	)

	for _, item := range c.publisherBuilder.publishers {
		for _, payloadType := range item.payloadTypes {
			if payloadType == reflect.TypeOf(payload) {
				publisher = &item
			}
		}
	}
	if publisher == nil {
		return fmt.Errorf("%v is not declared before ", payload)
	}

	return c.publisherBuilder.Publish(ctx, routingKey, publisher.exchangeName, payload)

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
