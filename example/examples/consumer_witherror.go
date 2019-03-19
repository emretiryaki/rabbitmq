package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
)


func main() {
	var  rabbitClient= rabbit.NewRabbitMqClient([]string{"127.0.0.1"},"guest","guest","",rabbit.PrefetchCount(3))

	onConsumed := func(message rabbit.Message) error {
		var consumeMessage string
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " UnSupported Message Type : ")
			return err
		}
		return nil
	}

	rabbitClient.AddConsumer("In.CustomerV1", "CustomerV1","",rabbit.Fanout, onConsumed)
	rabbitClient.RunConsumers()


}
