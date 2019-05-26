package rabbitmq

import (
	"testing"
)

type ExampleMessage struct {
	Id string
}

func TestGetBytes(t *testing.T) {
	var comlexType = ExampleMessage{Id: "1"}
	var _, err = getBytes(comlexType)
	if err != nil {
		t.Errorf("Test Fail : Actual [%d]\tExptected [%d]\n", nil, err.Error())
	}

}

