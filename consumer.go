package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"runtime"
	"time"
)

type (
	Consumer struct {
		queueName         string
		handleConsumer    handleConsumer
		errorQueueName    string
		errorExchangeName string
		brokerChannel     *BrokerChannel
		startConsumerCn   chan bool
		singleGoroutine   bool
		exchanges		  []exchange
	}
	Message struct {
		Payload       []byte
		CorrelationId string
		MessageId     string
		Timestamp     time.Time
	}

	exchange struct {
		exchangeName      string
		routingKey        string
		exchangeType      ExchangeType
		args         	  amqp.Table
	}

	handleConsumer func(message Message) error

)

func (m *MessageBrokerServer) AddConsumer(queueName string) *Consumer {

	var consumer = &Consumer{
		queueName:         queueName,
		errorQueueName:    queueName + ERRORPREFIX,
		errorExchangeName: queueName + ERRORPREFIX,
		startConsumerCn:   make(chan bool),
		singleGoroutine : false,
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
	return consumer
}

func (c *Consumer) HandleConsumer (consumer handleConsumer)  *Consumer {
	c.handleConsumer = consumer
	return c
}

func (c *Consumer) WithSingleGoroutine (value bool)  *Consumer {
	c.singleGoroutine = value
	return c
}

func (c *Consumer) SubscriberExchange (routingKey string,exchangeType ExchangeType, exchangeName string)  *Consumer {

	var isAlreadyDeclareExchange bool

	for _, item := range c.exchanges{
		if item.exchangeName == exchangeName {
			isAlreadyDeclareExchange = true
		}
	}

	if isAlreadyDeclareExchange {
		return c
	}

	c.exchanges = append(c.exchanges,exchange{exchangeName:exchangeName,exchangeType:exchangeType,routingKey:routingKey})
	return c
}

func (c *Consumer) SubscriberExchangeWithArguments(routingKey string, exchangeType ExchangeType, exchangeName string, args amqp.Table) *Consumer {

	var isAlreadyDeclareExchange bool

	for _, item := range c.exchanges {
	   if item.exchangeName == exchangeName {
		   isAlreadyDeclareExchange = true
	   }
   }

	if isAlreadyDeclareExchange {
	   return c
   }

	c.exchanges = append(c.exchanges, exchange{
	   exchangeName: exchangeName,
	   exchangeType: exchangeType,
	   routingKey:   routingKey,
	   args:         args,
   })
   return c
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

						logConsole(consumer.queueName + " started to listen rabbitMq")
						var err error
						if consumer.brokerChannel, err = m.messageBroker.CreateChannel(); err != nil {
							panic(err)
						}

						consumer.createQueue().createErrorQueueAndBind()

						for _, item := range consumer.exchanges {

							consumer.
								createExchange(item.exchangeName, item.exchangeType, item.args).
								exchangeBind(item.exchangeName, consumer.queueName, item.routingKey, item.exchangeType)

						}

						consumer.brokerChannel.channel.Qos(m.parameters.PrefetchCount, 0, false)

						delivery, _ := consumer.listenToQueue(consumer.queueName)

						if consumer.singleGoroutine {
							m.run(delivery, consumer)

						} else {
							for i := 0; i < m.parameters.PrefetchCount; i++ {

								go func() {
									m.run(delivery, consumer)
								}()
							}
						}

					}
				}
			}

			return nil
		})
	}

	return m.childRoutines.Wait()
}

func (m *MessageBrokerServer) run(delivery <-chan amqp.Delivery, consumer *Consumer) {
	for d := range delivery {

		Do(func(attempt int) (retry bool, err error) {

			retry = attempt < m.parameters.RetryCount

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

						consumer.brokerChannel.channel.Publish(consumer.errorExchangeName, "", false, false, errorPublishMessage(d.CorrelationId, d.Body, m.parameters.RetryCount, err, fmt.Sprintf("[Exception Recover] %v %s\n", err, stack[:length])))
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
