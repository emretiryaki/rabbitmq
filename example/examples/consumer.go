package main

import (

	"fmt"
	"time"
	"encoding/json"
	rabbit "github.com/emretiryaki/rabbitmq"
	"github.com/google/uuid"
)

type (
	PersonV1 struct {
		Name    string
		Surname string
		City    City
		Count   int
	}

	City struct {
		Name string
	}
)

func main() {

	var messageBus = rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/",
		rabbit.RetryCount(1,time.Duration(1)),
		rabbit.PrefetchCount(2))

	for i := 0; i < 1000; i++ {

		var publishMessage= PersonV1{Name: "Adam", Surname: "Smith", Count: i}
		messageBus.Publish(publishMessage, rabbit.WithCorrelationId(uuid.New().String()))
		fmt.Println(" Message was sent successfully :", publishMessage)

	}

	onConsumed := func(message rabbit.Message) error {

		var consumeMessage PersonV1
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {

			var publishMessage= PersonV1{Name: "Adam", Surname: "Smith", Count: i}
			messageBus.Publish(publishMessage, rabbit.WithCorrelationId(uuid.New().String()))
			fmt.Println(" Message was sent successfully :", publishMessage)

		}
		//time.Sleep(1 * time.Second)
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
		return nil
	}
	messageBus.Consume("In.Person", PersonV1{}, onConsumed)

}
