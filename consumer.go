package rabbitmq

import (
	"github.com/streadway/amqp"
	"time"
	"sync"
)

var (
	consumeOnce sync.Once
	consumeInstance  *consume
)

type (
		Consume interface{
			Consume(ConsumeSetting)  (chan contextMessage, error)
		}

		consume struct{
			ch  *amqp.Channel
		}

	   ConsumeSetting struct {
			queueName string
			autoAck bool
			isExclusive bool
			isNoLocal bool
			isNoWait bool
			arg  map[string]interface{}
		}

		contextMessage struct {
			Body          []byte
			CorrelationId string
			Timestamp     time.Time
			MessageId     string
			delivery      amqp.Delivery
		}
)

func (c consume) Consume(message ConsumeSetting) (chan contextMessage, error){

	deliverMessage :=make(chan contextMessage)
	delivery, err:=  c.ch.Consume(message.queueName,
								"",
								message.autoAck,
								message.isExclusive,
								message.isNoLocal,
								message.isNoWait,
								message.arg)

	if err!=nil{
		panic(err)
	 }

	go wrapMessage(deliverMessage, delivery)
	return  deliverMessage, err
}

func (cm contextMessage) Ack(multiple bool) error {
	return cm.delivery.Ack(multiple)
}

func wrapMessage(message chan contextMessage, sourceChan <-chan amqp.Delivery) {
	for m := range sourceChan {
		message <- newConsumerMessage(m)
	}
}

func newConsumerMessage(d amqp.Delivery) contextMessage {
	return contextMessage{
		Body:            d.Body,
		CorrelationId:	 d.CorrelationId,
		Timestamp : d.Timestamp,
		MessageId : d.MessageId,
		delivery:d,

	}
}

func NewInstanceConsume(ch *amqp.Channel) (Consume) {
	consumeOnce.Do(func() {
		consumeInstance = &consume{ch:ch}
	})
	return  consumeInstance
}


