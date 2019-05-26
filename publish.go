package rabbitmq

import (
	"encoding/json"
	"errors"
	"github.com/streadway/amqp"
	"time"

	"strconv"
)

var (
	deliveryMode     uint8 = 2
	headerError            = "Error"
	headerRetryCount       = "RetryCount"
	headerStackTrace       = "StackTrace"
	headerTime       = "Time"
)

type (
	builderPublishFunc func(*publishMessage) error

	publishMessage struct {
		CorrelationId string
		Exchange      string
		Payload       interface{}
	}

	Publisher struct {
		exchangeName  string
		routingKey    string
		brokerChannel *BrokerChannel
		exchangeType  ExchangeType
	}
)

func convertToPublishMessage(payload interface{}, builders ...builderPublishFunc) amqp.Publishing {

	var message = publishMessage{Payload: payload}
	for _, handler := range builders {
		handler(&message)
	}
	if message.CorrelationId == "" {
		message.CorrelationId = getGuid()
	}
	var body, _ = getBytes(message.Payload)
	return amqp.Publishing{
		MessageId:       getGuid(),
		Body:            body,
		Headers:         amqp.Table{},
		CorrelationId:   message.CorrelationId,
		Timestamp:       time.Now(),
		DeliveryMode:    deliveryMode,
		ContentEncoding: "UTF-8",
		ContentType:     "application/json",
	}
}


func errorPublishMessage(correlationId string, payload []byte, retryCount int, err error, stackTracing string) amqp.Publishing {

	headers := make(map[string]interface{})
	headers[headerRetryCount] = strconv.Itoa(retryCount)
	headers[headerError] = err.Error()
	headers[headerStackTrace]=stackTracing
	headers[headerTime]=time.Now().String()

	return amqp.Publishing{
		MessageId:       getGuid(),
		Body:            payload,
		Headers:         headers,
		CorrelationId:   correlationId,
		Timestamp:       time.Now(),
		DeliveryMode:    deliveryMode,
		ContentEncoding: "UTF-8",
		ContentType:     "application/json",
	}
}

func getBytes(key interface{}) ([]byte, error) {
	return json.Marshal(key)
}

func WithCorrelationId(correlationId string) builderPublishFunc {

	return func(m *publishMessage) error {
		if isGuid(correlationId) {
			m.CorrelationId = correlationId
			return nil
		} else {
			return errors.New("invalid UUID format")
		}
	}
}

func (m *MessageBrokerServer) Publish(exchangeName string, routing string, ) {

}

func (m *MessageBrokerServer) AddPublisher(exchangeName string, routingKey string, exchangeType ExchangeType) {

	var publisher = Publisher{
		routingKey:   routingKey,
		exchangeName: exchangeName,
		exchangeType: exchangeType,
	}

	var isAlreadyDeclareExchange bool
	for _, item := range m.publishers {
		if item.exchangeName == exchangeName {
			isAlreadyDeclareExchange = true
		}
	}

	if !isAlreadyDeclareExchange {
		m.publishers = append(m.publishers, publisher)
	}

}
