package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
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
	var messageBus = rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/", rabbit.RetryCount(2))

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
