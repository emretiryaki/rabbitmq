package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
	"github.com/google/uuid"
)

type (
	PersonV3 struct {
		Name    string
		Surname string
		Count   int
	}


)

func main() {

	var messageBus = rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/", rabbit.RetryCount(2,time.Duration(0)))

	for i := 0; i < 1; i++ {
		messageBus.Publish(PersonV3{Name: "Adam", Surname: "Smith",Count:i}, rabbit.WithCorrelationId(uuid.New().String()))
		fmt.Println(" Message was sent successfully by ")
	}

	onConsumed := func(message rabbit.Message) error {

		var consumeMessage PersonV3
		var err = json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)

		panic("panic")
		return nil
	}
	messageBus.Consume("In.Person3", "PersonV3", onConsumed)

}
