package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
	"github.com/google/uuid"
)


type (


	CustomerV1 struct{
		Name string
		Surname string
		Count int
	}
	CustomerV2 struct{
		Name string
		Surname string
		Count int
	}

)

func main(){

	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/",rabbit.ConcurrentCount(4))

	go func(){
		onConsumed := func(message rabbit.Message) error {
			var consumeMessage string
			var err =json.Unmarshal(message.Payload, &consumeMessage)
			if err!=nil{
				fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " UnSupported Message Type : " )
				return  err
			}
			return nil
		}

		messageBus.Consume("In.CustomerV1", CustomerV1{}, onConsumed)
	}()

	for i := 0; i < 100; i++ {
		messageBus.Publish(CustomerV1{Name:"Emre",Surname:"Tiryaki",Count:i},
			rabbit.WithCorrelationId(uuid.New().String()))
		fmt.Println("Message was sent successfully : Count => ", i)
	}


	go func(){
		onConsumed := func(message rabbit.Message) error {
			var consumeMessage CustomerV2
			var err =json.Unmarshal(message.Payload, &consumeMessage)
			if err!=nil{
				fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " UnSupported Message Type : " )
				return  err
			}
			if consumeMessage.Count==1{
				panic("runtime error")
			}
			return nil
		}

		messageBus.Consume("In.CustomerV2", CustomerV2{}, onConsumed)
	}()

	for i := 0; i < 100; i++ {
		messageBus.Publish(CustomerV1{Name:"Emre",Surname:"Tiryaki",Count:i},
			rabbit.WithCorrelationId(uuid.New().String()))
		fmt.Println("Message was sent successfully : Count => ", i)
	}


	for i := 0; i < 100; i++ {
		messageBus.Publish(CustomerV2{Name:"Emre",Surname:"Tiryaki",Count:i},
			rabbit.WithCorrelationId(uuid.New().String()))
		fmt.Println("Message was sent successfully : Count => ", i)
	}

	var userInput string
	fmt.Scanln(&userInput)
}

