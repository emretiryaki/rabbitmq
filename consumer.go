package rabbitmq

import (
	"fmt"
	"time"
	"github.com/streadway/amqp"
)

type (
	Consumer struct {
		queueName         string
		exchangeName      string
		routingKey        string
		handleConsumer    handleConsumer
		errorQueueName    string
		errorExchangeName string
		brokerChannel     *BrokerChannel
		startConsumerCn   chan bool
		exchangeType      ExchangeType
	}
	Message struct {
		Payload       []byte
		CorrelationId string
		MessageId     string
		Timestamp     time.Time
	}

	handleConsumer func(message Message) error
)

func (m *MessageBrokerServer) AddConsumer(queueName string, exchangeName string, routingKey string, exchangeType ExchangeType, handleConsumer handleConsumer) {

	var consumer = Consumer{
		queueName:         queueName,
		routingKey:        routingKey,
		exchangeName:      exchangeName,
		handleConsumer:    handleConsumer,
		errorQueueName:    queueName + ERRORPREFIX,
		errorExchangeName: queueName + ERRORPREFIX,
		startConsumerCn:   make(chan bool),
		exchangeType:      exchangeType,
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

						consumer.createExchange(consumer.exchangeName, consumer.exchangeType)
						consumer.createQueue(consumer.exchangeName, consumer.queueName, consumer.routingKey, consumer.exchangeType)
						consumer.createQueue(consumer.errorQueueName, consumer.errorExchangeName, "", Fanout)

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
												if err == nil {
													err = fmt.Errorf("panic occured In Consumer  %q (message %d)", d.CorrelationId, d.Body)
													consumer.brokerChannel.channel.Publish(consumer.errorExchangeName, "", false, false, errorPublishMessage(d.CorrelationId, d.Body, m.parameters.RetryCount, err))
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
