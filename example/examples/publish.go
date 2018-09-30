package main

import (

	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"github.com/google/uuid"
	"sync"

)

type (
	PersonV2 struct {
		Name    string
		Surname string
	}
)

func main() {

	var wg sync.WaitGroup
	wg.Add(101)
	var messageBus = rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/")

	go func() {
		for i := 0; i < 100; i++ {
			var publishMessage = PersonV2{Name: "Adam", Surname: "Smith"}
			messageBus.Publish(publishMessage, rabbit.WithCorrelationId(uuid.New().String()))
			fmt.Println(" Message was sent successfully :",publishMessage )
		}
		wg.Done()
	}()

	for i := 0; i < 100; i++ {
		go func() {
			var publishMessage = PersonV2{Name: "Adam", Surname: "Smith"}
			messageBus.Publish(publishMessage, rabbit.WithCorrelationId(uuid.New().String()))
			fmt.Println(" Message was sent successfully :",publishMessage )
			wg.Done()
		}()
	}
	wg.Wait()


}
