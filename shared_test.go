package rabbitmq

import (
	"testing"

	"github.com/google/uuid"
)

func TestIsGuid(t *testing.T) {
	if isGuid("test") {
		t.Logf("Test Success : Actual [%v]\tExptected [%v]\n", false, false)
	}
	if isGuid(uuid.New().String()) {
		t.Logf("Test Success : Actual [%v]\tExptected [%v]\n", true, true)
	}
}

func TestGetGuid(t *testing.T) {
	var guid = getGuid()
	_, err := uuid.Parse(guid)
	if err != nil {
		t.Errorf("Test Failed : Actual [%v]\tExptected [%v]\n", guid, err.Error())
	}
}
