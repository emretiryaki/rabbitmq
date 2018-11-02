package rabbitmq

import (
	"encoding/json"
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

func TestConvertPublishMessage(t *testing.T) {
	var comlexType = ExampleMessage{Id: "1"}
	var guid = getGuid()
	var publishingMessage = convertToPublishMessage(publishMessage{Payload: comlexType, CorrelationId: guid, Exchange: getExchangeName(comlexType)})
	if publishingMessage.CorrelationId != guid {
		t.Errorf("Test Fail : Actual [%d]\tExptected [%d]\n", publishingMessage.CorrelationId, guid)
	}
	var expectedExampleMessage ExampleMessage
	var err = json.Unmarshal(publishingMessage.Body, &expectedExampleMessage)
	if err != nil {
		t.Errorf("Test Fail : Actual [%d]\tExptected [%d]\n", err, nil)
	}
	if expectedExampleMessage.Id != comlexType.Id {
		t.Errorf("Test Fail : Actual [%d]\tExptected [%d]\n", expectedExampleMessage.Id, comlexType.Id)
	}
}
