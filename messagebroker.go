package rabbitmq

import (
	"github.com/streadway/amqp"
	"time"
)

type MessageBroker interface {
	CreateChannel ()  (*BrokerChannel,error)
	CreateConnection (parameters MessageBrokerParameter) error
	SignalConnectionStatus (status bool)
	SignalConnection()  (chan bool)
}

type broker struct {
	parameters MessageBrokerParameter
	connection *amqp.Connection
	connNotifyChannel chan bool
}

type MessageBrokerParameter struct {
	Uri             string
	PrefetchCount   int
	RetryCount      int
	ConcurrentLimit int
	RetryInterval   time.Duration
}

type BrokerChannel struct {
	channel         *amqp.Channel
	prefetchCount   int
	retryCount      int
	concurrentLimit int
	retryInterval   time.Duration
}

func (b *broker) CreateConnection(parameters MessageBrokerParameter) (error) {

	var err error

	b.parameters = parameters

	for {

		if b.connection, err = amqp.Dial(b.parameters.Uri); err != nil {
			time.Sleep(b.parameters.RetryInterval)
			continue
		}
		b.onClose()
		b.SignalConnectionStatus(true)

		break
	}

	return err

}
func (b *broker) onClose() {
	go func() {
		err := <-b.connection.NotifyClose(make(chan *amqp.Error))
		if err != nil {
			b.SignalConnectionStatus(false)
			return
		}
	}()
}

func (b *broker)  CreateChannel()  (*BrokerChannel,error) {

	brokerChannel, err := b.connection.Channel()
	if err != nil {
		return nil, err
	}

	return &BrokerChannel{
		channel:         brokerChannel,
		prefetchCount:   b.parameters.PrefetchCount,
		retryCount:      b.parameters.RetryCount,
		retryInterval:   b.parameters.RetryInterval,
		concurrentLimit: b.parameters.ConcurrentLimit,
	}, nil

}

func (b *broker) SignalConnectionStatus(status bool) {
	go func() {
		b.connNotifyChannel <- status
	}()
}

func (b *broker) SignalConnection() (chan bool) {
	 return b.connNotifyChannel
}


func NewMessageBroker () MessageBroker {
	brokerClient := &broker{connNotifyChannel:make(chan bool)}
	brokerClient.SignalConnectionStatus(false)
	return brokerClient
}

