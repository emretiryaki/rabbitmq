package main

import (

	"fmt"
	rabbit "github.com/emretiryaki/rabbitmq"

	"github.com/google/uuid"
	"sync"
	"time"
	"encoding/json"
)



type (

	PersonV2 struct{
		Name string
		Surname string


	}



)

func main(){

	var wg sync.WaitGroup
	wg.Add(101)
	var messageBus=rabbit.CreateUsingRabbitMq("amqp://guest:guest@localhost:5672/")

	go func(){
		for i := 0; i < 100; i++ {
			messageBus.Publish(PersonV2{Name: "Adam", Surname: "Smith"}, rabbit.WithCorrelationId(uuid.New().String()))
			fmt.Println(" Message was sent successfully by FirstFunc")
		}
		wg.Done()
	}()

	for i := 0; i < 100; i++ {
		go func(){
			messageBus.Publish(PersonV2{Name: "Adam", Surname: "Smith"}, rabbit.WithCorrelationId(uuid.New().String()))
			fmt.Println("Message was sent successfully by SecondFunc")
			wg.Done()
		}()
	}
	wg.Wait()


	onConsumed := func(message rabbit.Message) error {
		var consumeMessage PersonV2
		var err =json.Unmarshal(message.Payload, &consumeMessage)
		if err!=nil{
			return  err
		}
		time.Sleep(1 * time.Second)
		fmt.Println(time.Now().Format("Mon, 02 Jan 2006 15:04:05 "), " Message:",consumeMessage)
		return nil
	}
	messageBus.Listen("In.Personv2",  PersonV2{}, onConsumed)
}

