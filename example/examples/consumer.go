package main

import (

	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"

	"encoding/json"
	"time"

)



type (

	PersonV1 struct{
		Name string
		Surname string
		City City
		Count int
	}

	City struct{
		Name string
	}

)

func main(){

	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/",rabbit.ConcurrentCount(1))

	go func(){

		onConsumed := func(message rabbit.Message) error {
			var consumeMessage PersonV1
			var err =json.Unmarshal(message.Payload, &consumeMessage)
			if err!=nil{
				return  err
			}
			time.Sleep(1 * time.Second)
			fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:",consumeMessage)
			return nil
		}
		messageBus.Consume("In.Person",  PersonV1{}, onConsumed)
	}()

	go func(){
		for i := 0; i < 100; i++ {
			messageBus.Publish(PersonV1{Name: "Adam", Surname: "Smith", City: City{Name: "London"}, Count: i},
			rabbit.WithCorrelationId("e1a14bd2-c1dd-49a7-aa80-1bd5f4d0c47e"))
			fmt.Println("Message was sent successfully : Count => ", i)
		}
	}()

	var userInput string
	fmt.Scanln(&userInput)
}

