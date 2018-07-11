package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
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

	var messageBus = rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/", rabbit.RetryCount(2),rabbit.PrefetchCount(2))

	for i := 0; i < 100; i++ {
		messageBus.Publish(PersonV1{Name: "Adam", Surname: "Smith",Count:i}, rabbit.WithCorrelationId(uuid.New().String()))
		fmt.Println(" Message was sent successfully by FirstFunc")
	}

	onConsumed := func(message rabbit.Message) error {

		var consumeMessage PersonV1
		var err = json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
		return nil
	}
	messageBus.Listen("In.Person", PersonV1{}, onConsumed)

}
