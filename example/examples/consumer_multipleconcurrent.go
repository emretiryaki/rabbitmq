package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
	//"github.com/google/uuid"
	"github.com/google/uuid"
)


type (


	Customer struct{
		Name string
		Surname string
		Count int
	}


)

func main(){

	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/",rabbit.ConcurrentCount(4))

	go func(){
		onConsumed := func(message rabbit.Message) error {
			var consumeMessage Customer
			var err =json.Unmarshal(message.Payload, &consumeMessage)
			if err!=nil{
				return  err
			}
			time.Sleep(1 * time.Second)
			fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message : " ,consumeMessage)
			return nil
		}

		messageBus.Consume("In.Customer", Customer{}, onConsumed)
	}()

	for i := 0; i < 100; i++ {
		messageBus.Publish(Customer{Name:"Emre",Surname:"Tiryaki",Count:i},
			rabbit.WithCorrelationId(uuid.New().String()))
		fmt.Println("Message was sent successfully : Count => ", i)
	}


	var userInput string
	fmt.Scanln(&userInput)
}

