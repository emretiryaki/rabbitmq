package rabbitmq

import (
	"github.com/streadway/amqp"
	"time"
)

type (
	ConsumerBuilder struct {
		messageBroker MessageBroker
		consumers     []*Consumer
	}

	Consumer struct {
		queueName         string
		handleConsumer    handleConsumer
		errorQueueName    string
		errorExchangeName string
		startConsumerCn   chan bool
		singleGoroutine   bool
		exchanges         []exchange
	}
	Message struct {
		Payload       []byte
		CorrelationId string
		MessageId     string
		Timestamp     time.Time
	}

	exchange struct {
		exchangeName string
		routingKey   string
		exchangeType ExchangeType
		args         amqp.Table
	}

	handleConsumer func(message Message) error
)

func (c *Consumer) HandleConsumer(consumer handleConsumer) *Consumer {
	c.handleConsumer = consumer
	return c
}

func (c *Consumer) WithSingleGoroutine(value bool) *Consumer {
	c.singleGoroutine = value
	return c
}

func (c *Consumer) SubscriberExchange(routingKey string, exchangeType ExchangeType, exchangeName string) *Consumer {

	var isAlreadyDeclareExchange bool

	for _, item := range c.exchanges {
		if item.exchangeName == exchangeName {
			isAlreadyDeclareExchange = true
		}
	}

	if isAlreadyDeclareExchange {
		return c
	}

	c.exchanges = append(c.exchanges, exchange{exchangeName: exchangeName, exchangeType: exchangeType, routingKey: routingKey})
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

