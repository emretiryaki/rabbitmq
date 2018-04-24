package rabbitmq

import (
	"sync"
	"github.com/streadway/amqp"


	"fmt"
	"errors"
	"time"
)

var (
	oncebus sync.Once
	messageBusInstance *messageBus
	ErrorPrefix=".Error"
	retryCount=0
	concurrentCount=1

)

type (

	HandleFunc 	func(*messageBus) error
	OnConsumeFunc   func(Message) error

	MessageBus interface{
		Publish(payload interface{},builders ...BuilderPublishFunc) error
		Consume(queueName string,  consumeMessage interface{},fn OnConsumeFunc)
	}

	messageBus struct {
	queues          map[string]string
	exchanges       map[string]string
	consumer        Consume
	concurrentCount int
	retryCount      int
	sync.Mutex
	uri             string
	channel         *amqp.Channel
}

	Message struct {
		Payload     []byte
		CorrelationId string
		MessageId	string
		Timestamp time.Time
	}
)


func  RetryCount(retryCount int)  HandleFunc{
	return func(m *messageBus) error{
		m.retryCount=retryCount
		return  nil
	}
}

func  ConcurrentCount(concurrentCount int)  HandleFunc{
	return func(m *messageBus) error{
		m.concurrentCount=concurrentCount
		return  nil
	}
}


func (m messageBus) Publish(payload interface{},builders ...BuilderPublishFunc) error {

	var conn,channel,err= createConnAndChan(m.uri)

	defer  conn.Close()
	defer  channel.Close()

	if err!=nil{
		panic(err)
	}

	var exchange = getExchangeName(payload)
	_,isExist:=m.exchanges[exchange]

	var message =publishMessage{Payload:payload}

	for _, handler:= range builders {
		handler(&message)
	}
	var publishingMessage= convertPublishMessage(message)

	if isExist {
			return channel.Publish(exchange,"",false,false,publishingMessage)
		}else{
			m.createExchange(exchange,"")
			return channel.Publish(exchange,"",false,false,publishingMessage)
	}
}

func (mb messageBus) Consume(queueName string,consumeMessage interface{},fn OnConsumeFunc )  {

	var exchangeName = getExchangeName(consumeMessage)
	destinationExchange:=queueName

	var errorQueue= destinationExchange + ErrorPrefix
	var errorExchange= queueName + ErrorPrefix

	_,isExistExchange:=mb.exchanges[exchangeName]

	if !isExistExchange {
		mb.createExchange(exchangeName,"")
	}


	mb.createQueue(destinationExchange,queueName,"")
	mb.channel.ExchangeBind(destinationExchange,"",exchangeName,true,nil)
	mb.createQueue(errorQueue,errorExchange,"")


	consumerMessage,_:=mb.consumer.Consume(ConsumeSetting{arg:nil,isNoWait:false,isNoLocal:false,isExclusive:false,queueName:queueName,autoAck:false})


	forever := make(chan bool)
	for x := 0; x < mb.concurrentCount; x++ {
		go func() {
			defer close(consumerMessage)
			for m := range consumerMessage {
				Do(func(attempt int) (retry bool, err error) {

				 	retry = attempt< mb.retryCount

					 defer func() {
						if r := recover(); r != nil {
							err = errors.New(fmt.Sprintf("panic: %v", r))
							if  attempt== mb.retryCount {
								var  messageError= publishMessage{Exchange:errorExchange,Payload:m.Body}
								mb.channel.Publish(errorExchange,"",false,false,convertPublishMessage(messageError))
								m.Ack(false)
							}
						}
					}()
					err=fn(Message{CorrelationId:m.CorrelationId,Payload:m.Body ,MessageId:m.MessageId,Timestamp:m.Timestamp})
					if err!=nil{
						panic(err)
					}
					m.Ack(false)
					return
				})

			}

		}()
	}

	<-forever

}


func (mb* messageBus) createQueue(destinationExchange string, queueName string, routingKey string) {
	mb.channel.ExchangeDeclare(destinationExchange,"fanout",true,false,false,false,nil)
	q, _ := mb.channel.QueueDeclare(queueName, true, false, false, false, nil)
	mb.channel.QueueBind(q.Name, routingKey, destinationExchange, false, nil)
}

func  (mb *messageBus) createExchange(exchange string,routingKey  string) error{
	var err =mb.channel.ExchangeDeclare(exchange,exchangeType(routingKey),true,false,false,false,nil)
	mb.exchanges[exchange]=exchange
	if err!=nil{
		return err
	}
	 return nil
}




func exchangeType (routingKey string) string{
	var exchangeType="fanout"
	if routingKey!=""{
		exchangeType="direct"
	}
	return exchangeType
}

func createConnAndChan(dsn string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(dsn)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}

func CreateUsingRabbitMq(uri string,handlefunceList ...HandleFunc) (MessageBus) {

	oncebus.Do(func() {

		var _,channel,_= createConnAndChan(uri)
		messageBusInstance = &messageBus{
			uri:uri,
			concurrentCount:concurrentCount,
			retryCount:retryCount,
			consumer:NewInstanceConsume(channel),
			queues:make(map[string]string),
			exchanges:make(map[string]string),
			channel:channel,


		}

		for _, handler:= range handlefunceList {
			if  err:= handler(messageBusInstance);err!=nil{
				panic(err)
			}
		}

	})
	return  messageBusInstance
}



