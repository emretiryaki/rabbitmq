package rabbitmq

import (
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"time"
)

var (
	ERRORPREFIX     = ".Error"
	CONCURRENTLIMIT = 1
	RETRYCOUNT      = 0
	PREFECTCOUNT    = 1
	RETRY_INTERVAL  = time.Second
)

const (
	Direct ExchangeType = 1
	Fanout ExchangeType = 2
	Topic  ExchangeType = 3
)

type (
	ExchangeType int

	MessageBrokerServer struct {
		context            context.Context
		shutdownFn         context.CancelFunc
		childRoutines      *errgroup.Group
		parameters         MessageBrokerParameter
		shutdownReason     string
		shutdownInProgress bool
		consumers          []Consumer
		messageBroker      MessageBroker
		publishers          []Publisher
	}

	withFunc func(*MessageBrokerServer) error
)

func NewRabbitMqClient(nodes []string, userName string, password string, virtaulHost string, withFunc ...withFunc) *MessageBrokerServer {

	rootCtx, shutdownFn := context.WithCancel(context.Background())
	childRoutines, childCtx := errgroup.WithContext(rootCtx)

	messageBrokerServer := &MessageBrokerServer{
		context:       childCtx,
		shutdownFn:    shutdownFn,
		childRoutines: childRoutines,
		parameters: MessageBrokerParameter{
			Nodes:           nodes,
			ConcurrentLimit: CONCURRENTLIMIT,
			RetryCount:      RETRYCOUNT,
			PrefetchCount:   PREFECTCOUNT,
			RetryInterval:   RETRY_INTERVAL,
			Password:        password,
			UserName:        userName,
			VirtualHost: virtaulHost,
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

func PrefetchCount(prefetchCount int) withFunc {
	return func(m *MessageBrokerServer) error {
		m.parameters.PrefetchCount = prefetchCount
		return nil
	}
}

func RetryCount(retryCount int, retryInterval time.Duration) withFunc {
	return func(m *MessageBrokerServer) error {
		m.parameters.RetryCount = retryCount
		m.parameters.RetryInterval = retryInterval
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

func (consumer *Consumer) createExchange(exchange string, exchangeType ExchangeType) error {

	var err = consumer.brokerChannel.channel.ExchangeDeclare(exchange, convertRabbitmqExchangeType(exchangeType), true, false, false, false, nil)
	if err != nil {
		return err
	}
	return nil
}

func (consumer *Consumer) createQueue(destinationExchange string, queueName string, routingKey string, exchangeType ExchangeType) {

	consumer.brokerChannel.channel.ExchangeDeclare(destinationExchange, convertRabbitmqExchangeType(exchangeType), true, false, false, false, nil)
	q, _ := consumer.brokerChannel.channel.QueueDeclare(queueName, true, false, false, false, nil)
	consumer.brokerChannel.channel.QueueBind(q.Name, routingKey, destinationExchange, false, nil)
}
