package rabbitmq

import (
	"encoding/json"
	"errors"
	"github.com/streadway/amqp"
	"time"

	"strconv"
)

var (
	deliveryMode uint8 = 2
	HEADER_ERROR ="Error"
	HEADER_RETRY_COUNT="RetryCount"
)

type (
	BuilderPublishFunc func(*publishMessage) error

	publishMessage struct {
		CorrelationId string
		Exchange      string
		Payload       interface{}
	}
)

func convertToPublishMessage(payload interface{} , builders ...BuilderPublishFunc) amqp.Publishing {

	var message = publishMessage {Payload: payload }

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



func convertErrorPublishMessage(correlationId string, payload []byte, retryCount int, err error) amqp.Publishing {

	headers := make(map[string]interface{})
	headers[HEADER_RETRY_COUNT] = strconv.Itoa(retryCount)
	headers[HEADER_ERROR] = err.Error()

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

func WithCorrelationId(correlationId string) BuilderPublishFunc {

	return func(m *publishMessage) error {
		if isGuid(correlationId) {
			m.CorrelationId = correlationId
			return nil
		} else {
			return errors.New("invalid UUID format")
		}
	}
}