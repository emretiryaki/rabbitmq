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
	ConsistentHashing ExchangeType = 4
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
		consumers          []*Consumer
		messageBroker      MessageBroker
		publishers         []Publisher
	}

	withFunc func(*MessageBrokerServer) error
)

func NewRabbitMqClient(nodes []string, userName string, password string, virtualHost string, withFunc ...withFunc) *MessageBrokerServer {

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
			VirtualHost: virtualHost,
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

func (c *Consumer) createExchange(exchange string, exchangeType ExchangeType) *Consumer {
	 c.brokerChannel.channel.ExchangeDeclare(exchange, convertExchangeType(exchangeType), true, false, false, false, nil)

	 return c

}

func (c *Consumer) createQueue()  *Consumer {
	 c.brokerChannel.channel.QueueDeclare(c.queueName, true, false, false, false, nil)
	return c

}

func (c *Consumer) exchangeBind(destinationExchange string, queueName string, routingKey string, exchangeType ExchangeType) *Consumer {
	c.brokerChannel.channel.ExchangeDeclare(destinationExchange, convertExchangeType(exchangeType), true, false, false, false, nil)
	c.brokerChannel.channel.QueueBind(queueName, routingKey, destinationExchange, false, nil)
	return c
}

func (c *Consumer) createErrorQueueAndBind()  *Consumer {
	c.brokerChannel.channel.ExchangeDeclare(c.errorExchangeName, "fanout", true, false, false, false, nil)
	q, _ := c.brokerChannel.channel.QueueDeclare(c.errorQueueName, true, false, false, false, nil)
	c.brokerChannel.channel.QueueBind(q.Name, "",c.errorExchangeName, false, nil)
	return c

}
