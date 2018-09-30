package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
)


func main() {

	var messageBus= rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/")


	onConsumed := func(message rabbit.Message) error {
		var consumeMessage string
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " UnSupported Message Type : ")
			return err
		}
		return nil
	}

	messageBus.Consume("In.CustomerV1", "CustomerV1", onConsumed)

}
