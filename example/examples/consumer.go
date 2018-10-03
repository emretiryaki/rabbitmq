package main

import (

	"fmt"
	"time"
	"encoding/json"
	rabbit "github.com/emretiryaki/rabbitmq"
	server "github.com/emretiryaki/rabbitmq"
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



	var  mes= server.NewRabbitmqServer("amqp://guest:guest@localhost:5672/")



	onConsumed := func(message rabbit.Message) error {

		var consumeMessage PersonV1
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
		return nil
	}

	onConsumed2 := func(message rabbit.Message) error {

		var consumeMessage PersonV4
		var err= json.Unmarshal(message.Payload, &consumeMessage)
		if err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:", consumeMessage)
		return nil
	}
	mes.AddConsumer("In.Person3", "PersonV3","", onConsumed2)
	mes.AddConsumer("In.Person", "PersonV1","", onConsumed)

	mes.RunConsumer()


}