package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
)


func main() {

	var  rabbitServer= rabbit.NewRabbitmqServer("amqp://guest:guest@localhost:5672/",rabbit.PrefetchCount(3))

	onConsumed := func(message rabbit.Message) error {
		var consumeMessage string
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " UnSupported Message Type : ")
			return err
		}
		return nil
	}

	rabbitServer.AddConsumer("In.CustomerV1", "CustomerV1","", onConsumed)
	rabbitServer.RunConsumers()


}
