package main

import (

	"fmt"
	"time"
	"encoding/json"
	rabbit "github.com/emretiryaki/rabbitmq"
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

	PersonV4 struct {
		Name    string
		Surname string
		City    City
		Count   int
	}


)

func main() {

	var  rabbitClient= rabbit.NewRabbitMqClient([]string{"127.0.0.1","127.0.0.2"},"guest","guest","",rabbit.RetryCount(2, time.Duration(0)),rabbit.PrefetchCount(3))

	onConsumed := func(message rabbit.Message) error {

		var consumeMessage PersonV1
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			return err
		}
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
		return nil
	}

	onConsumed2 := func(message rabbit.Message) error {

		var consumeMessage PersonV4
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			return err
		}
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
		return nil
	}

	onConsumed3 := func(message rabbit.Message) error {

		var consumeMessage PersonV4
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			return err
		}
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
		return nil
	}

	rabbitClient.AddConsumer("In.Person_Fanout", "PersonV3_Fanout","", rabbit.Fanout,onConsumed2)
	rabbitClient.AddConsumer("In.Person_Direct", "PersonV1_Direct","person", rabbit.Direct,onConsumed)
	rabbitClient.AddConsumer("In.Person_Topic", "PersonV4_Topic","person", rabbit.Topic,onConsumed3)
	rabbitClient.RunConsumers()


}