package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type (
	MessageBroker interface {
		CreateChannel() (*BrokerChannel, error)
		CreateConnection(parameters MessageBrokerParameter) error
		SignalConnectionStatus(status bool)
		SignalConnection() chan bool
	}

	MessageBrokerParameter struct {
		Nodes             []string
		PrefetchCount     int
		RetryCount        int
		ConcurrentLimit   int
		RetryInterval     time.Duration
		UserName          string
		Password          string
		selectedNodeIndex int
		VirtualHost       string
	}

	BrokerChannel struct {
		channel         *amqp.Channel
		prefetchCount   int
		retryCount      int
		concurrentLimit int
		retryInterval   time.Duration
	}
	broker struct {
		parameters        MessageBrokerParameter
		connection        *amqp.Connection
		connNotifyChannel chan bool
	}
)

func (b *broker) CreateConnection(parameters MessageBrokerParameter) (error) {

	var err error

	b.parameters = parameters

	for {

		if b.connection, err = amqp.Dial(b.chooseNode()); err != nil {
			time.Sleep(b.parameters.RetryInterval)

			logConsole("Application Retried To Connect RabbitMq")
			continue
		}
		b.onClose()
		logConsole("Application  Connected RabbitMq")
		b.SignalConnectionStatus(true)

		break
	}

	return err

}

// TODO: This crashes if we define no servers in our config
func (b *broker) chooseNode() string {
	if b.parameters.selectedNodeIndex== len(b.parameters.Nodes){
		b.parameters.selectedNodeIndex=0
	}
	var selectedNode = b.parameters.Nodes[b.parameters.selectedNodeIndex]
	b.parameters.selectedNodeIndex++
  	logConsole(fmt.Sprintf("Started To Listen On Node %s",selectedNode))
	return   fmt.Sprintf("amqp://%s:%s@%s/%s",b.parameters.UserName,b.parameters.Password, selectedNode,b.parameters.VirtualHost)

}

func (b *broker) onClose() {
	go func() {
		err := <-b.connection.NotifyClose(make(chan *amqp.Error))
		if err != nil {
			logConsole("RabbitMq Connection Is Down")
			b.SignalConnectionStatus(false)
			return
		}
	}()
}

func (b *broker) CreateChannel() (*BrokerChannel, error) {

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

func (b *broker) SignalConnection() chan bool {
	return b.connNotifyChannel
}

func NewMessageBroker() MessageBroker {
	brokerClient := &broker{connNotifyChannel: make(chan bool)}
	brokerClient.SignalConnectionStatus(false)
	return brokerClient
}
