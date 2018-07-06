package rabbitmq

import (
	"testing"

	"encoding/json"
	"fmt"
)

type (
	PersonV1 struct {
		Name    string
		Surname string
		City    City
	}

	City struct {
		Name string
	}
)

var (
	url = "amqp://guest:guest@localhost:5672/"
)

func TestWithBasicPubSubNoPanicOrError(t *testing.T) {

	var consumeMessage PersonV1

	onConsumed := func(message Message) error {
		json.Unmarshal(message.Payload, &consumeMessage)
		fmt.Println(" Correlation Id: ", message.CorrelationId, "Name: ", consumeMessage.Name, "City :", consumeMessage.City.Name)
		return nil
	}

	var messageBus = CreateUsingRabbitMq(url)

	messageBus.Publish(PersonV1{Name: "Emre", Surname: "Tiryaki", City: City{Name: "Istanbul"}})
	messageBus.Listen("BasicPubSubNoPanicOrError", consumeMessage, onConsumed)

}
