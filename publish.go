package rabbitmq

import (
	 "github.com/streadway/amqp"
	"time"
	"errors"
	"encoding/json"
)

var (
	deliveryMode uint8=2
)

type(

	BuilderPublishFunc func(*publishMessage) error

	publishMessage struct {
		CorrelationId string
		Exchange string
		Payload interface{}

	}

)

func convertPublishMessage(message publishMessage)(amqp.Publishing){

	if message.CorrelationId== ""{
		message.CorrelationId = getGuid()
	}
	var body,_= getBytes(message.Payload)
	return amqp.Publishing{
		MessageId:getGuid(),
		Body:body,
		Headers: amqp.Table{},
		CorrelationId:message.CorrelationId,
		Timestamp:time.Now(),
		DeliveryMode:deliveryMode,
		ContentEncoding:"UTF-8",
		ContentType:"application/json",
		}
}

func convertErrorPublishMessage(correlationId string, payload []byte)(amqp.Publishing){

	return amqp.Publishing{
		MessageId:getGuid(),
		Body:payload,
		Headers: amqp.Table{},
		CorrelationId:correlationId,
		Timestamp:time.Now(),
		DeliveryMode:deliveryMode,
		ContentEncoding:"UTF-8",
		ContentType:"application/json",
	}
}


func getBytes(key interface{}) ([]byte, error) {
	return  json.Marshal(key)
}


func WithCorrelationId(correlationId string)  BuilderPublishFunc{
	return func(m *publishMessage)  error {
		if isGuid(correlationId){
			m.CorrelationId = correlationId
			return nil
		}else {
		 	return 	errors.New("invalid UUID format")
		}
	}
}

