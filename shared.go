package rabbitmq

import (
	"github.com/google/uuid"
	"reflect"
	"fmt"
	"encoding/json"
)

func getGuid() string {
	return uuid.New().String()
}

func isGuid(paramater string) bool {
	_, err := uuid.Parse(paramater)
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

type Log struct {
	Message   string
}