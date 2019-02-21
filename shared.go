package rabbitmq

import (
	"github.com/google/uuid"
	"reflect"
	"fmt"
	"encoding/json"
	"github.com/streadway/amqp"
)

func getGuid() string {
	return uuid.New().String()
}

func isGuid(p string) bool {
	_, err := uuid.Parse(p)
	if err != nil {
		return false
	}
	return true
}

func getExchangeName(message interface{}) string {
	if t := reflect.TypeOf(message); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

func  logConsole(message string){
	logMessage,_ :=json.Marshal(Log{Message:message})
	fmt.Println(string(logMessage))
}


func convertRabbitmqExchangeType(exchangeType ExchangeType) (string) {

	var rabbitmqExchangeType string
	switch exchangeType {
	case Direct:
		rabbitmqExchangeType = amqp.ExchangeDirect
		break
	case Fanout:
		rabbitmqExchangeType = amqp.ExchangeFanout
		break
	case Topic:
		rabbitmqExchangeType = amqp.ExchangeTopic
		break
	}
	return rabbitmqExchangeType
}
type Log struct {
	Message   string
}