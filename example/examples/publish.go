package main

import (

	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"

	"github.com/google/uuid"
)



type (

	PersonV2 struct{
		Name string
		Surname string


	}



)

func main(){

	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/",rabbit.ConcurrentCount(1))

	go func(){
		for i := 0; i < 100; i++ {
			messageBus.Publish(PersonV2{Name: "Adam", Surname: "Smith"}, rabbit.WithCorrelationId(uuid.New().String()))
			fmt.Println(" Message was sent successfully by FirstFunc")
		}
	}()

	for i := 0; i < 100; i++ {
		go func(){
			messageBus.Publish(PersonV2{Name: "Adam", Surname: "Smith"}, rabbit.WithCorrelationId(uuid.New().String()))
			fmt.Println("Message was sent successfully by SecondFunc")
		}()
	}

	var userInput string
	fmt.Scanln(&userInput)
}

