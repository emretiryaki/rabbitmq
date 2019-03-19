package main

import (
	"encoding/json"
	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"
	"time"
)

type (
	PersonV3 struct {
		Name    string
		Surname string
		Count   int
	}


)

func main() {


	var  rabbitClient= rabbit.NewRabbitMqClient([]string{"127.0.0.1"},"guest","guest","",rabbit.RetryCount(2,time.Duration(0)))


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
	rabbitClient.AddConsumer("In.Person3", "PersonV3","",rabbit.Fanout, onConsumed)
	rabbitClient.RunConsumers()

}
