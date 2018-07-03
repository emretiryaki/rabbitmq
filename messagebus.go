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
	connection      *amqp.Connection
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

	m.createChannel()
	defer  m.channel.Close()

	var exchange = getExchangeName(payload)
	_,isExist:=m.exchanges[exchange]

	var message =publishMessage{Payload:payload}

	for _, handler:= range builders {
		handler(&message)
	}
	var publishingMessage= convertPublishMessage(message)

	if isExist {
			return m.channel.Publish(exchange,"",false,false,publishingMessage)
		}else{
			m.createExchange(exchange,"")
			return m.channel.Publish(exchange,"",false,false,publishingMessage)
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
	mb.createChannel()
	mb.channel.ExchangeBind(destinationExchange,"",exchangeName,true,nil)
	mb.createQueue(errorQueue,errorExchange,"")
	mb.channel.Close()

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
	var err= mb.channel.ExchangeDeclare(exchange,exchangeType(routingKey),true,false,false,false,nil)
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


func createConn(dsn string) (*amqp.Connection) {
	conn, _ := amqp.Dial(dsn)
	return conn
}

func (m* messageBus	) createChannel() (error) {
	var channel,err=m.connection.Channel()
	m.channel=channel
	return  err
}

func CreateUsingRabbitMq(uri string,handlefunceList ...HandleFunc) (MessageBus) {

	oncebus.Do(func() {
		var conn =createConn(uri)

		messageBusInstance = &messageBus{
			concurrentCount:concurrentCount,
			retryCount:retryCount,
			queues:make(map[string]string),
			exchanges:make(map[string]string),
			connection:conn,
		}
		for _, handler:= range handlefunceList {
			if  err:= handler(messageBusInstance);err!=nil{
				panic(err)
			}
		}

	})
	return  messageBusInstance
}



